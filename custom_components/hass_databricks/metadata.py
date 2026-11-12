"""Export floors, areas, entity metadata and Energy config to Databricks.

Row builders are pure functions over duck-typed registry objects so they can be
unit-tested without a running Home Assistant. Registry access (``collect_snapshots``)
runs on the event loop; only file I/O goes through the executor.
"""

from __future__ import annotations

from datetime import datetime
import json
import logging
import os
from typing import Any, Iterable

from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import floor_registry as fr
from homeassistant.helpers import label_registry as lr

from .pipeline import (
    DatabricksTarget,
    RuntimeSyncConfig,
    SyncRequest,
    _async_run_job,
    _makedirs,
    _remove_path,
)

_LOGGER = logging.getLogger(__name__)

# Table name -> explicit column schema (without the synced_at column).
# This is a contract with the downstream project: do not rename.
TABLE_SCHEMAS: dict[str, str] = {
    "ha_floors": "floor_id STRING, name STRING, level INT, "
    "aliases ARRAY<STRING>, icon STRING",
    "ha_areas": "area_id STRING, name STRING, floor_id STRING, "
    "aliases ARRAY<STRING>, icon STRING, labels ARRAY<STRING>",
    "ha_entities": "entity_id STRING, friendly_name STRING, area_id STRING, "
    "device_id STRING, device_name STRING, domain STRING, device_class STRING, "
    "unit STRING, state_class STRING, platform STRING, labels ARRAY<STRING>, "
    "disabled BOOLEAN",
    "ha_energy_config": "role STRING, stat_id STRING, money_stat_id STRING, "
    "price_entity_id STRING, fixed_price DOUBLE, source_index INT, name STRING",
}


# --------------------------------------------------------------------------- rows


def build_floor_rows(floors: Iterable[Any]) -> list[dict]:
    """One row per floor registry entry."""
    return [
        {
            "floor_id": f.floor_id,
            "name": f.name,
            "level": f.level,
            "aliases": sorted(f.aliases),
            "icon": f.icon,
        }
        for f in floors
    ]


def _label_names(label_ids: Iterable[str], names: dict[str, str]) -> list[str]:
    return sorted(names[i] for i in label_ids if i in names)


def build_area_rows(areas: Iterable[Any], label_names: dict[str, str]) -> list[dict]:
    """One row per area, whether or not it has entities."""
    return [
        {
            "area_id": a.id,
            "name": a.name,
            "floor_id": a.floor_id,
            "aliases": sorted(a.aliases),
            "icon": a.icon,
            "labels": _label_names(a.labels, label_names),
        }
        for a in areas
    ]


def build_entity_rows(
    entities: Iterable[Any],
    devices: dict[str, Any],
    state_names: dict[str, str],
    label_names: dict[str, str],
) -> list[dict]:
    """One row per entity registry entry (all domains)."""
    rows = []
    for e in entities:
        device = devices.get(e.device_id) if e.device_id else None
        rows.append(
            {
                "entity_id": e.entity_id,
                "friendly_name": state_names.get(e.entity_id)
                or e.name
                or e.original_name,
                "area_id": e.area_id or (device.area_id if device else None),
                "device_id": e.device_id,
                "device_name": (device.name_by_user or device.name) if device else None,
                "domain": e.entity_id.split(".", 1)[0],
                "device_class": e.device_class or e.original_device_class,
                "unit": e.unit_of_measurement,
                "state_class": (e.capabilities or {}).get("state_class"),
                "platform": e.platform,
                "labels": _label_names(e.labels, label_names),
                "disabled": e.disabled_by is not None,
            }
        )
    return rows


def _energy_row(
    role, stat_id, index, *, money=None, price_entity=None, price=None, name=None
):
    return {
        "role": role,
        "stat_id": stat_id,
        "money_stat_id": money,
        "price_entity_id": price_entity,
        "fixed_price": price,
        "source_index": index,
        "name": name,
    }


def build_energy_rows(prefs: dict | None) -> list[dict]:
    """Flatten Energy dashboard preferences into one row per statistic.

    Keys verified against homeassistant/components/energy/data.py (2024.9):
    grid has ``flow_from``/``flow_to`` lists; solar/gas/water use
    ``stat_energy_from``; battery has ``stat_energy_from`` (discharge) and
    ``stat_energy_to`` (charge). Unmappable entries are skipped at debug level.
    """
    rows: list[dict] = []
    if not prefs:
        return rows

    def price_kwargs(d: dict, money_key: str) -> dict:
        return {
            "money": d.get(money_key),
            "price_entity": d.get("entity_energy_price"),
            "price": d.get("number_energy_price"),
        }

    for i, src in enumerate(prefs.get("energy_sources") or []):
        kind = src.get("type")
        if kind == "grid":
            for flow in src.get("flow_from") or []:
                if flow.get("stat_energy_from"):
                    rows.append(
                        _energy_row(
                            "grid_import",
                            flow["stat_energy_from"],
                            i,
                            **price_kwargs(flow, "stat_cost"),
                        )
                    )
            for flow in src.get("flow_to") or []:
                if flow.get("stat_energy_to"):
                    rows.append(
                        _energy_row(
                            "grid_export",
                            flow["stat_energy_to"],
                            i,
                            **price_kwargs(flow, "stat_compensation"),
                        )
                    )
        elif kind == "solar" and src.get("stat_energy_from"):
            rows.append(_energy_row("solar", src["stat_energy_from"], i))
        elif (
            kind == "battery"
            and src.get("stat_energy_from")
            and src.get("stat_energy_to")
        ):
            rows.append(_energy_row("battery_out", src["stat_energy_from"], i))
            rows.append(_energy_row("battery_in", src["stat_energy_to"], i))
        elif kind in ("gas", "water") and src.get("stat_energy_from"):
            rows.append(
                _energy_row(
                    kind, src["stat_energy_from"], i, **price_kwargs(src, "stat_cost")
                )
            )
        else:
            _LOGGER.debug("Skipping unmappable energy source at index %s", i)

    for i, dev in enumerate(prefs.get("device_consumption") or []):
        if dev.get("stat_consumption"):
            rows.append(
                _energy_row(
                    "device_consumption",
                    dev["stat_consumption"],
                    i,
                    name=dev.get("name"),
                )
            )
    return rows


# ---------------------------------------------------------------------------- SQL


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _table(catalog: str, schema: str, table: str) -> str:
    return ".".join(_quote_ident(p) for p in (catalog, schema, table))


def _quote_path(path: str) -> str:
    if "'" in path or "\\" in path:
        raise ValueError("Volume path must not contain quotes or backslashes.")
    return f"'{path}'"


def build_load_sql(catalog: str, schema: str, table: str, path: str) -> str:
    """Replace the snapshot table from an uploaded JSON-lines file."""
    return (
        f"CREATE OR REPLACE TABLE {_table(catalog, schema, table)} AS "
        "SELECT *, current_timestamp() AS synced_at "
        f"FROM read_files({_quote_path(path)}, format => 'json', "
        f"schema => '{TABLE_SCHEMAS[table]}')"
    )


def build_empty_ddl(catalog: str, schema: str, table: str) -> str:
    """Replace the snapshot table with an empty one that has the right columns."""
    return (
        f"CREATE OR REPLACE TABLE {_table(catalog, schema, table)} "
        f"({TABLE_SCHEMAS[table]}, synced_at TIMESTAMP)"
    )


# -------------------------------------------------------------------------- sync


async def _energy_preferences(hass: Any) -> dict | None:
    """Return Energy preferences, or None when energy is absent/unconfigured."""
    if "energy" not in hass.config.components:
        return None
    try:
        from homeassistant.components.energy.data import async_get_manager

        manager = await async_get_manager(hass)
    except ImportError:
        return None
    return manager.data


async def collect_snapshots(hass: Any) -> dict[str, list[dict]]:
    """Read all registries. Must run on the event loop."""
    labels = {lab.label_id: lab.name for lab in lr.async_get(hass).labels.values()}
    devices = dict(dr.async_get(hass).devices.items())
    state_names = {
        s.entity_id: s.attributes["friendly_name"]
        for s in hass.states.async_all()
        if s.attributes.get("friendly_name")
    }
    energy = None
    try:
        energy = await _energy_preferences(hass)
    except Exception as err:  # pylint: disable=broad-except
        _LOGGER.debug("Could not read energy preferences: %s", err)

    return {
        "ha_floors": build_floor_rows(fr.async_get(hass).floors.values()),
        "ha_areas": build_area_rows(ar.async_get(hass).areas.values(), labels),
        "ha_entities": build_entity_rows(
            er.async_get(hass).entities.values(), devices, state_names, labels
        ),
        "ha_energy_config": build_energy_rows(energy),
    }


def _write_jsonl(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


async def run_metadata_sync(request: SyncRequest) -> dict:
    """Replace the four ha_* snapshot tables. Returns row counts and errors.

    A failure in one table does not stop the others; errors are reported in the
    result rather than raised.
    """
    snapshots = await collect_snapshots(request.hass)

    stamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    job_id = f"metadata_{stamp}"
    local_dir = os.path.join(request.local_path, job_id)
    dbx_dir = f"{request.dbx_volumes_path}/{job_id}"

    target = DatabricksTarget(
        RuntimeSyncConfig(
            catalog=request.catalog,
            schema=request.schema,
            table=request.table,
            local_path=request.local_path,
            dbx_path=request.dbx_volumes_path,
        ),
        server_hostname=request.server_hostname,
        http_path=request.http_path,
        access_token=request.access_token,
        session=request.session,
        hass=request.hass,
    )

    counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    uploaded = False
    await target.create_schema()
    await _async_run_job(request.hass, _makedirs, local_dir)
    try:
        for table, rows in snapshots.items():
            local_file = os.path.join(local_dir, f"{table}.jsonl")
            try:
                if not rows:
                    await target._execute_sql(  # pylint: disable=protected-access
                        build_empty_ddl(request.catalog, request.schema, table)
                    )
                else:
                    await _async_run_job(request.hass, _write_jsonl, local_file, rows)
                    remote = f"{dbx_dir}/{table}.jsonl"
                    await target._upload_file(  # pylint: disable=protected-access
                        local_file, remote
                    )
                    uploaded = True
                    await target._execute_sql(  # pylint: disable=protected-access
                        build_load_sql(request.catalog, request.schema, table, remote)
                    )
                counts[table] = len(rows)
            except Exception as err:  # pylint: disable=broad-except
                errors[table] = str(err)
            finally:
                await _async_run_job(request.hass, _remove_path, local_file)
    finally:
        await _async_run_job(request.hass, _remove_path, local_dir)

    if uploaded:
        try:
            await target.delete_volume_folder(dbx_dir)
        except Exception:  # pylint: disable=broad-except
            pass

    return {"tables": counts, "errors": errors}
