from typing import Any, Dict, List, Optional, Tuple


def safe_get(d: Any, path: List[Any], default=None):
    """
    Safe nested getter.
    path items can be keys (str) or indexes (int).
    """
    cur = d
    for p in path:
        try:
            if isinstance(p, int):
                if not isinstance(cur, list) or p >= len(cur):
                    return default
                cur = cur[p]
            else:
                if not isinstance(cur, dict):
                    return default
                cur = cur.get(p)
        except Exception:
            return default
    return default if cur is None else cur
