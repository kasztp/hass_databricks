"""hass_databricks standalone package.

.. deprecated::
    This standalone package is deprecated. All active development has moved
    to the Home Assistant custom integration located in
    ``custom_components/hass_databricks/``. The integration provides a
    superior streaming micro-batch pipeline optimized for low-resource
    environments. This package is retained for reference only and will be
    removed in a future release.
"""

import warnings

warnings.warn(
    "The standalone 'hass_databricks' package is deprecated. "
    "Use the Home Assistant custom integration in "
    "'custom_components/hass_databricks/' instead.",
    DeprecationWarning,
    stacklevel=2,
)
