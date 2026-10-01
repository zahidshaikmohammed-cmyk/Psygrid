"""Reduce a JSON payload to its shape: keys and value types, recursively.

Values are dropped, so a shape is stable across runs while still catching any
added, removed or renamed field and any change of type. A dictionary keyed by
data (989 stocks, 38 health components) collapses to the merged shape of its
values plus its count, and keeps its key names when there are 64 or fewer.
List elements and collapsed values merge: a key missing from some is marked
optional with a trailing "?", and differing scalar types join as "float|null".
"""

from __future__ import annotations

from functools import reduce

COLLAPSE_AT = 12  # a dict with this many object values is a keyed collection, not a record
LIST_KEYS_UP_TO = 64  # keep the key names of collections up to this size (health components, indices)
_TYPE_NAMES = {bool: "bool", int: "int", float: "float", str: "str", type(None): "null"}


def shape(value):
    if isinstance(value, dict):
        shapes = {key: shape(item) for key, item in value.items()}
        if len(shapes) >= COLLAPSE_AT and all(isinstance(s, dict) for s in shapes.values()):
            collapsed = {"<count>": len(shapes), "<each value>": reduce(merge, shapes.values())}
            if len(shapes) <= LIST_KEYS_UP_TO:
                collapsed["<keys>"] = sorted(shapes)
            return collapsed
        return shapes
    if isinstance(value, list):
        if not value:
            return []
        merged = shape(value[0])
        for item in value[1:]:
            merged = merge(merged, shape(item))
        return [merged]
    return _TYPE_NAMES[type(value)]


def merge(a, b):
    if a == b:
        return a
    if isinstance(a, dict) and isinstance(b, dict):
        # A key that is absent from either side is optional, marked with a trailing "?".
        left = {k.rstrip("?"): (k.endswith("?"), v) for k, v in a.items()}
        right = {k.rstrip("?"): (k.endswith("?"), v) for k, v in b.items()}
        out = {}
        for key in sorted(set(left) | set(right)):
            if key in left and key in right:
                optional = left[key][0] or right[key][0]
                value = merge(left[key][1], right[key][1])
            else:
                optional, value = True, (left.get(key) or right.get(key))[1]
            out[key + "?" if optional else key] = value
        return out
    if isinstance(a, list) and isinstance(b, list):
        if not a or not b:
            return a or b
        return [merge(a[0], b[0])]
    if isinstance(a, str) and isinstance(b, str):
        return "|".join(sorted(set(a.split("|")) | set(b.split("|"))))
    return "|".join(sorted({_label(a), _label(b)}))


def _label(s) -> str:
    if isinstance(s, str):
        return s
    return "object" if isinstance(s, dict) else "array"
