"""Pure validation for persisted herdr-core (``hc``) terminal addresses."""

from __future__ import annotations

import re

# ``t7``, ``t1r``: a terminal on this core, which hc keeps "forever on this
# core". Remote terminals (``vps/t3``) are deliberately excluded: their hooks
# cannot reach this machine's session map, so they are never topic targets.
_HC_ADDRESS_RE = re.compile(r"t[0-9a-z]+\Z")


def is_hc_address(value: str) -> bool:
    """Return whether *value* is a local hc terminal address."""
    return bool(_HC_ADDRESS_RE.fullmatch(value))
