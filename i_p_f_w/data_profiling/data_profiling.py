"""Profile datasets loaded into raw_data and persist the results in the data_profiling schema.

Run directly:  python -m i_p_f_w.data_profiling.data_profiling [dataset_name]
From code:     run_profiling(dataset=None, correlations=True) -> summary dict
"""
import itertools
import logging
import math
import re
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
from scipy import stats

from i_p_f_w.metadata_manager.metadata_registry import MetadataRegistry

log = logging.getLogger(__name__)

RAW_SCHEMA = "raw_data"
FUNCTIONS_SQL = Path(__file__).with_name("profiling_functions.sql")

# Defaults; edit here to change the rules.
SENTINELS = {
    "numeric": [-9999, -999, -99],
    "text": ["unknown", "unspecified", "not available", "none", "-", "--", "?"],
}
INFER_THRESHOLD = 0.95         # share of non-blank text values that must look numeric / datetime to infer that type
TOP_N = 20                     # category values kept per column (the rest go to one 'other' bucket)
CATEGORY_MAX_DISTINCT = 50     # categories are profiled only for columns with at most this many distinct values ...
CATEGORY_MAX_RATIO = 0.5       # ... and distinct / non-null <= this (keeps names, free text and identifiers out)
MISSING_WARNING_PCT = 20.0     # null + empty + whitespace share of rows: >= warning, >= error, 100 = critical
MISSING_ERROR_PCT = 50.0
DUPLICATE_ROW_LIMIT = 1_000_000  # whole-row duplicate check is skipped above this many rows
CORR_MAX_COLUMNS = 40
CORR_MIN_SAMPLE = 10           # minimum pairwise-complete observations per correlation
CORR_SAMPLE_ROWS = 50_000      # row cap for Spearman (Pearson always uses the full table)
ID_LIKE = re.compile(r"(^|[^a-z])(id|fid|uid|objectid|code|pcode|index|year|lat|lon|lng|latitude|longitude)([^a-z]|$)|pcode", re.I)

TARGETS_SQL = """
SELECT d.id, d.name, t.schema, t."table",
       to_regclass(format('%%I.%%I', t.schema, t."table")) IS NOT NULL AS table_exists
FROM metadata_manager.dataset d
JOIN metadata_manager.target t ON t.dataset_id = d.id
WHERE t.database = current_database() AND t.schema = %s AND (%s::text IS NULL OR d.name = %s)
ORDER BY d.id
"""


def _pct(part, whole):
    return 100.0 * part / whole if whole else None


def _num(value):
    return None if value is None else float(value)  # also parses 'NaN' / 'Infinity' strings from jsonb


def _pearson_p(r, n):
    if n <= 2:
        return None
    if abs(r) >= 1:
        return 0.0
    t = r * math.sqrt((n - 2) / (1 - r * r))
    return float(2 * stats.t.sf(abs(t), n - 2))


def _metric_row(run_id, pid, name, value, unit=None, method=None):
    if value is None:
        return None  # column_metric requires exactly one non-null value
    if isinstance(value, (list, dict)):
        vals = (None, None, Json(value))
    elif isinstance(value, str):
        vals = (None, value, None)
    else:
        vals = (float(value), None, None)
    return (run_id, pid, name, *vals, unit, method)


def run_profiling(dataset=None, correlations=True):
    """Profile every registered dataset whose table exists in raw_data (or only `dataset`, matched by name).

    Each dataset gets its own profile_run and its own transaction, so one failure cannot affect another.
    Returns {"completed": [...], "failed": [...], "missing_tables": [...]}.
    """
    summary = {"completed": [], "failed": [], "missing_tables": []}
    conn = psycopg2.connect(**MetadataRegistry().db)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(FUNCTIONS_SQL.read_text(encoding="utf-8"))
            cur.execute(TARGETS_SQL, (RAW_SCHEMA, dataset, dataset))
            targets = cur.fetchall()
        if dataset and not targets:
            log.warning("Dataset '%s' is not registered with a raw_data target", dataset)

        for dataset_id, name, schema, table, exists in targets:
            if not exists:
                log.warning("%s: table %s.%s does not exist, skipped", name, schema, table)
                summary["missing_tables"].append(name)
                continue

            # The run row is committed first so a failed run still leaves a trace.
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE data_profiling.profile_run SET status = 'failed', completed_at = clock_timestamp(), "
                    "error_message = 'Interrupted before completion' WHERE dataset_id = %s AND status = 'running'",
                    (dataset_id,),
                )
                cur.execute("INSERT INTO data_profiling.profile_run (dataset_id) VALUES (%s) RETURNING run_id", (dataset_id,))
                run_id = cur.fetchone()[0]

            try:
                with conn, conn.cursor() as cur:  # all results + completion status commit together or not at all
                    rows = _profile_table(cur, run_id, schema, table, correlations)
                    cur.execute(
                        "UPDATE data_profiling.profile_run SET status = 'completed', completed_at = clock_timestamp(), "
                        "row_count = %s WHERE run_id = %s",
                        (rows, run_id),
                    )
                summary["completed"].append({"dataset": name, "run_id": run_id, "rows": rows})
                log.info("%s: profiled %d rows (run_id=%d)", name, rows, run_id)
            except Exception as exc:
                log.exception("%s: profiling failed", name)
                with conn, conn.cursor() as cur:
                    cur.execute(
                        "UPDATE data_profiling.profile_run SET status = 'failed', completed_at = clock_timestamp(), "
                        "error_message = %s WHERE run_id = %s",
                        (f"{type(exc).__name__}: {exc}"[:2000], run_id),
                    )
                summary["failed"].append({"dataset": name, "run_id": run_id, "error": (str(exc).splitlines() or [""])[0][:300]})
    finally:
        conn.close()
    return summary


def _profile_table(cur, run_id, schema, table, correlations):
    cur.execute("SELECT data_profiling.profile_columns(%s, %s, %s)", (schema, table, Json(SENTINELS)))
    prof = cur.fetchone()[0]
    total, cols = prof["row_count"], prof["columns"]

    # ---- inferred types (text columns are inspected; everything else keeps its native kind) ----
    for c in cols:
        c["nulls"] = total - c["nn"]
        if c["kind"] == "text":
            content = c["nn"] - c["emp"] - c["ws"]
            c["content"] = content
            if content and c["nm"] / content >= INFER_THRESHOLD:
                c["inferred"] = "numeric"
            elif content and c["dm"] / content >= INFER_THRESHOLD:
                c["inferred"] = "datetime"
            else:
                c["inferred"] = "text"
        else:
            c["inferred"] = {"numeric": "numeric", "temporal": "datetime"}.get(c["kind"], c["kind"])

    # ---- column_profile ----
    rows = [
        (run_id, c["name"], table, c["data_type"], c["inferred"], total, c["nn"], c["nulls"], c.get("emp", 0), c.get("dist"),
        c.get("sent"), _pct(c["emp"], total) if "emp" in c else None, c.get("ws"), _pct(c["ws"], total) if "ws" in c else None)
        for c in cols
    ]
    ids = dict(execute_values(
        cur,
        "INSERT INTO data_profiling.column_profile "
        "(run_id, column_name, table_name, source_data_type, inferred_type, total_count, "
        "non_null_count, null_count, empty_string_count, distinct_count, sentinel_count, empty_string_pct, "
        "whitespace_only_count, whitespace_only_pct) VALUES %s RETURNING column_name, column_profile_id",
        rows, fetch=True,
    ))

    # ---- column_metric ----
    metrics = []
    for c in cols:
        pid = ids[c["name"]]
        m = [("null_pct", _pct(c["nulls"], total), "%", "null_count / total_count"),
             ("distinct_ratio", c["dist"] / c["nn"] if c.get("dist") is not None and c["nn"] else None, None, "distinct_count / non_null_count")]
        if c["kind"] == "text":
            m += [("min_length", c["lmin"], "chars", None), ("max_length", c["lmax"], "chars", None)]
            if c["inferred"] == "numeric":
                m.append(("numeric_value_pct", _pct(c["nm"], c["content"]), "%", "regex match / non-blank text values"))
            if c["inferred"] == "datetime":
                m.append(("datetime_value_pct", _pct(c["dm"], c["content"]), "%", "regex match / non-blank text values"))
        if c["inferred"] == "datetime":
            m += [("min", c.get("mn"), None, "ISO text order"), ("max", c.get("mx"), None, "ISO text order")]
        metrics += [_metric_row(run_id, pid, *x) for x in m]

    numeric_cols = [c["name"] for c in cols if c["inferred"] == "numeric"]
    if numeric_cols and total:
        cur.execute("SELECT data_profiling.profile_numeric(%s, %s, %s, %s)", (schema, table, numeric_cols, Json(SENTINELS)))
        method = "sentinels and non-numeric text excluded"
        for name, s in cur.fetchone()[0].items():
            if not s["n"]:
                continue
            pid = ids[name]
            q1, med, q3 = s["q"]
            for key, value, mm in [
                ("numeric_count", s["n"], method), ("min", s["min"], method), ("max", s["max"], method),
                ("mean", s["mean"], method), ("sum", s["sum"], method),
                ("variance", s["var"], f"sample (n-1); {method}"), ("std_dev", s["std"], f"sample (n-1); {method}"),
                ("q1", q1, "percentile_cont"), ("median", med, "percentile_cont"), ("q3", q3, "percentile_cont"),
                ("zero_count", s["zeros"], method), ("negative_count", s["neg"], method),
            ]:
                metrics.append(_metric_row(run_id, pid, key, _num(value) if key != "numeric_count" else value, None, mm))
    metrics = [x for x in metrics if x]
    if metrics:
        execute_values(
            cur,
            "INSERT INTO data_profiling.column_metric (run_id, column_profile_id, metric_name, metric_value_numeric, "
            "metric_value_text, metric_value_json, unit, method) VALUES %s",
            metrics,
        )

    # ---- column_distribution (low-cardinality text / boolean columns only) ----
    cat_cols = [c["name"] for c in cols
                if c["inferred"] in ("text", "boolean") and c.get("dist") and c["dist"] <= CATEGORY_MAX_DISTINCT
                and c["dist"] <= CATEGORY_MAX_RATIO * c["nn"]]
    if cat_cols:
        cur.execute("SELECT * FROM data_profiling.profile_categories(%s, %s, %s, %s)", (schema, table, cat_cols, TOP_N))
        dist = [(run_id, ids[col], "other" if other else "category", val, None, None, freq, pct, other)
                for col, val, freq, pct, other in cur.fetchall()]
        if dist:
            execute_values(
                cur,
                "INSERT INTO data_profiling.column_distribution (run_id, column_profile_id, distribution_type, "
                "category_value, bin_lower, bin_upper, frequency, percentage, is_other_bucket) VALUES %s",
                dist,
            )

    # ---- spatial_profile ----
    spatial = {}
    for c in cols:
        if c["kind"] != "geometry":
            continue
        cur.execute("SELECT data_profiling.profile_geometry(%s, %s, %s)", (schema, table, c["name"]))
        g = cur.fetchone()[0]
        spatial[c["name"]] = g
        types = g["types"] or []
        has_extent = g["xmin"] is not None
        execute_values(
            cur,
            "INSERT INTO data_profiling.spatial_profile (run_id, column_profile_id, source_schema, source_table, "
            "geometry_column, geometry_type, coordinate_dimension, srid, crs_authority, crs_code, total_count, "
            "missing_geometry_count, empty_geometry_count, valid_geometry_count, invalid_geometry_count, "
            "extent_min_x, extent_min_y, extent_max_x, extent_max_y, extent_srid, spatial_completeness_pct) VALUES %s",
            [(run_id, ids[c["name"]], schema, table, c["name"],
              types[0] if len(types) == 1 else (f"MIXED({', '.join(types)})" if types else None),
              g["coord_dim"], g["srid"], g["auth_name"], None if g["auth_srid"] is None else str(g["auth_srid"]),
              g["total"], g["total"] - g["non_null"], g["empty"], g["valid"], g["invalid"],
              g["xmin"], g["ymin"], g["xmax"], g["ymax"], g["srid"] if has_extent else None,
              _pct(g["non_null"] - g["empty"], g["total"]))],
        )

    # ---- column_correlation ----
    if correlations and total:
        _correlations(cur, run_id, schema, table, cols, ids, total)

    # ---- profile_issue ----
    issues = _issues(cur, schema, table, total, cols, spatial)
    if issues:
        execute_values(
            cur,
            "INSERT INTO data_profiling.profile_issue (run_id, column_profile_id, issue_type, severity, affected_rows, "
            "issue_description, sample_values) VALUES %s",
            [(run_id, ids.get(col), kind, sev, affected, desc, Json(samples) if samples else None)
             for col, kind, sev, affected, desc, samples in issues],
        )
    return total


def _correlations(cur, run_id, schema, table, cols, ids, total):
    """Pearson (full table, in SQL) and Spearman (Python, capped sample), both pairwise-complete after dropping
    NULLs, non-numeric text and sentinels. p-values are raw (uncorrected for the number of pairs tested)."""
    cand = [c for c in cols if c["inferred"] == "numeric" and (c.get("dist") or 0) > 1
            and c["nn"] >= CORR_MIN_SAMPLE and not ID_LIKE.search(c["name"])]
    if len(cand) > CORR_MAX_COLUMNS:
        log.warning("%s.%s: %d numeric columns, correlating the first %d", schema, table, len(cand), CORR_MAX_COLUMNS)
        cand = cand[:CORR_MAX_COLUMNS]
    if len(cand) < 2:
        return
    names = [c["name"] for c in cand]
    rows = []

    cur.execute("SELECT * FROM data_profiling.pearson_pairs(%s, %s, %s, %s)", (schema, table, names, Json(SENTINELS)))
    for a, b, r, n in cur.fetchall():
        if r is not None and not math.isnan(r) and n >= CORR_MIN_SAMPLE:
            r = max(-1.0, min(1.0, r))
            rows.append((a, b, "pearson", r, n, _pearson_p(r, n)))

    sample_pct = min(100.0, 100.0 * CORR_SAMPLE_ROWS / total)
    cur.execute("SELECT data_profiling.numeric_sample(%s, %s, %s, %s, %s)", (schema, table, names, Json(SENTINELS), sample_pct))
    s = cur.fetchone()[0]
    df = pd.DataFrame(dict(zip(s["names"], s["values"])), dtype="float64")
    for a, b in itertools.combinations(s["names"], 2):
        pair = df[[a, b]].dropna()
        if len(pair) < CORR_MIN_SAMPLE:
            continue
        rho, p = stats.spearmanr(pair[a], pair[b])
        if not math.isnan(rho):
            rows.append((a, b, "spearman", float(rho), len(pair), None if math.isnan(p) else float(p)))

    out = []
    for a, b, method, coef, n, p in rows:
        ia, ib = sorted((ids[a], ids[b]))  # one order only, so a pair can never be stored twice
        out.append((run_id, ia, ib, method, coef, n, p))
    if out:
        execute_values(
            cur,
            "INSERT INTO data_profiling.column_correlation (run_id, column_a_id, column_b_id, method, coefficient, "
            "sample_size, p_value) VALUES %s",
            out,
        )


def _issues(cur, schema, table, total, cols, spatial):
    """Return (column or None, issue_type, severity, affected_rows, description, sample_values) tuples."""
    out = []
    if total == 0:
        return [(None, "empty_table", "critical", 0, "Table has no rows", None)]

    for c in cols:
        name = c["name"]
        missing = c["nulls"] + c.get("emp", 0) + c.get("ws", 0)
        pct = _pct(missing, total)
        if c["kind"] == "geometry":
            pass  # missing / empty geometries are reported by the spatial checks below
        elif pct >= MISSING_WARNING_PCT:
            sev = "critical" if missing == total else "error" if pct >= MISSING_ERROR_PCT else "warning"
            out.append((name, "excessive_missingness", sev, missing,
                        f"{pct:.1f}% of rows are null, empty or whitespace-only", None))
        elif missing:
            out.append((name, "missing_values", "info", missing, f"{pct:.1f}% of rows are null, empty or whitespace-only", None))
        if c.get("sent"):
            out.append((name, "sentinel_values", "warning", c["sent"],
                        "Values matching the configured sentinel rules (excluded from numeric statistics)", None))
        if c.get("dist") == 1 and c["nn"] > 1:
            out.append((name, "constant_column", "info", c["nn"], "All non-null values are identical", None))
        if c["kind"] == "text" and c["inferred"] in ("numeric", "datetime"):
            out.append((name, "type_mismatch", "info", c["content"],
                        f"Stored as text but values look {c['inferred']}", None))
            bad = c["content"] - (c["nm"] if c["inferred"] == "numeric" else c["dm"])
            if bad:
                cur.execute(
                    sql.SQL("SELECT DISTINCT left(btrim({c}), 40) FROM {s}.{t} WHERE btrim({c}) <> '' "
                            "AND btrim({c}) !~ data_profiling.value_pattern(%s) LIMIT 5").format(
                        c=sql.Identifier(name), s=sql.Identifier(schema), t=sql.Identifier(table)),
                    (c["inferred"],),
                )
                out.append((name, "mixed_type_values", "warning", bad,
                            f"Text values that do not look {c['inferred']}", [r[0] for r in cur.fetchall()]))

    if total <= DUPLICATE_ROW_LIMIT:
        cur.execute("SELECT data_profiling.duplicate_rows(%s, %s)", (schema, table))
        dup = cur.fetchone()[0]
        if dup:
            out.append((None, "duplicate_rows", "warning", dup, "Rows identical to an earlier row in every column", None))

    for name, g in spatial.items():
        missing = total - g["non_null"]
        if missing:
            out.append((name, "missing_geometry", "warning", missing, "NULL geometries", None))
        if g["empty"]:
            out.append((name, "empty_geometry", "warning", g["empty"], "Empty geometries", None))
        if g["invalid"]:
            out.append((name, "invalid_geometry", "error", g["invalid"], "Geometries failing ST_IsValid", g["reasons"]))
        if not g["srid"]:
            out.append((name, "missing_crs", "warning", None, "Geometry column has SRID 0 (CRS unknown)", None))
        elif g["srid"] == 4326 and g["xmin"] is not None and not (
                -180 <= g["xmin"] and g["xmax"] <= 180 and -90 <= g["ymin"] and g["ymax"] <= 90):
            out.append((name, "coordinates_out_of_range", "error", None,
                        "SRID 4326 extent lies outside lon/lat bounds", None))
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(run_profiling(sys.argv[1] if len(sys.argv) > 1 else None))
