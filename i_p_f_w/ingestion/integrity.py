import hashlib
import json
from datetime import datetime
from numbers import Integral

import pandas as pd

from i_p_f_w.metadata_manager.yaml_metadata_parser import YAML_SUFFIXES


def source_files(path):
    """The data file, or for a shapefile every file sharing its stem (YAML sidecars excluded)."""
    if path.suffix.lower() == ".shp":
        return sorted(f for f in path.parent.iterdir()
                      if f.is_file() and f.stem == path.stem and f.suffix.lower() not in YAML_SUFFIXES)
    return [path]


def source_sha256(path):
    h = hashlib.sha256()
    for f in source_files(path):
        h.update(f.name.encode())
        with open(f, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                h.update(block)
    return h.hexdigest()


def _cell(v):
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return f"b{int(v)}"
    if isinstance(v, Integral):
        return f"i{int(v)}"
    if isinstance(v, float):
        return f"f{v.hex()}"
    if isinstance(v, datetime):
        ts = pd.Timestamp(v)
        return f"t{(ts.tz_convert('UTC') if ts.tzinfo else ts).isoformat()}"
    return f"s{v}"


def rows_sum(rows, start=0):
    """Order-independent row digest: SHA-256 of each row (as a JSON list), summed mod 2**256."""
    total = sum(int.from_bytes(hashlib.sha256(json.dumps([_cell(v) for v in row]).encode()).digest(), "big")
                for row in rows)
    return (start + total) % 2 ** 256