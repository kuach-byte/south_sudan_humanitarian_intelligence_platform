-- Profiling functions. Read-only on the profiled table; results are persisted by data_profiling.py.
-- Identifiers are built with format(%I) and values with %L. Applied (CREATE OR REPLACE) at the start of every run.
-- Sentinel rules arrive as jsonb: {"numeric": [-9999, ...], "text": ["unknown", ...]}.

CREATE SCHEMA IF NOT EXISTS data_profiling;

-- Single source of truth for the patterns used to infer / safely cast text values.
CREATE OR REPLACE FUNCTION data_profiling.value_pattern(p_kind text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE p_kind
        WHEN 'numeric'  THEN '^[-+]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][-+]?[0-9]{1,2})?$'
        WHEN 'datetime' THEN '^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2}([.][0-9]+)?)?)?$'
    END
$$;

-- Columns of a table with a coarse kind: numeric | text | temporal | boolean | geometry | other.
CREATE OR REPLACE FUNCTION data_profiling.table_columns(p_schema text, p_table text)
RETURNS TABLE (ordinal int, col_name text, data_type text, kind text)
LANGUAGE sql STABLE AS $$
    SELECT a.attnum::int, a.attname::text, format_type(a.atttypid, a.atttypmod),
           CASE
               WHEN t.typname IN ('int2', 'int4', 'int8', 'numeric', 'float4', 'float8') THEN 'numeric'
               WHEN t.typname IN ('text', 'varchar', 'bpchar') THEN 'text'
               WHEN t.typname IN ('date', 'timestamp', 'timestamptz') THEN 'temporal'
               WHEN t.typname = 'bool' THEN 'boolean'
               WHEN t.typname = 'geometry' THEN 'geometry'
               ELSE 'other'
           END
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = to_regclass(format('%I.%I', p_schema, p_table))
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
$$;

-- SQL expression (as text) giving a column as double precision, with non-numeric text and sentinels -> NULL.
CREATE OR REPLACE FUNCTION data_profiling.numeric_expr(p_ident text, p_kind text, p_sentinels jsonb)
RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_sent text := (SELECT array_agg(v::double precision)::text
                    FROM jsonb_array_elements_text(p_sentinels -> 'numeric') AS v);
    v_raw  text;
BEGIN
    IF p_kind = 'numeric' THEN
        v_raw := format('%s::double precision', p_ident);
    ELSE
        v_raw := format('(CASE WHEN btrim(%1$s) ~ %2$L THEN btrim(%1$s)::double precision END)',
                        p_ident, data_profiling.value_pattern('numeric'));
    END IF;
    IF v_sent IS NULL THEN
        RETURN v_raw;
    END IF;
    RETURN format('(CASE WHEN %1$s = ANY (%2$L::double precision[]) THEN NULL ELSE %1$s END)', v_raw, v_sent);
END
$$;

-- Pass 1: one table scan, all columns. Returns {row_count, columns: [{name, data_type, kind, nn, dist, ...}]}.
--   nn = non-null, dist = distinct, emp = empty string, ws = whitespace-only (non-empty), sent = sentinel,
--   nm / dm = text values matching the numeric / datetime pattern, lmin / lmax = text length, mn / mx = min / max.
CREATE OR REPLACE FUNCTION data_profiling.profile_columns(p_schema text, p_table text, p_sentinels jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_num_pat  text := data_profiling.value_pattern('numeric');
    v_date_pat text := data_profiling.value_pattern('datetime');
    v_num_sent text := (SELECT array_agg(v::double precision)::text
                        FROM jsonb_array_elements_text(p_sentinels -> 'numeric') AS v);
    v_txt_sent text := (SELECT array_agg(lower(v))::text
                        FROM jsonb_array_elements_text(p_sentinels -> 'text') AS v);
    v_rel   regclass := to_regclass(format('%I.%I', p_schema, p_table));
    rec     record;
    v_i     int := 0;
    v_id    text;
    v_parts text[];
    v_cond  text[];
    v_sel   text[] := '{}';
    v_meta  jsonb := '[]';
    v_res   jsonb;
BEGIN
    IF v_rel IS NULL THEN
        RAISE EXCEPTION 'Table %.% does not exist', p_schema, p_table;
    END IF;

    FOR rec IN SELECT * FROM data_profiling.table_columns(p_schema, p_table) LOOP
        v_i := v_i + 1;
        v_id := format('%I', rec.col_name);
        v_parts := ARRAY[format('%L, count(%s)', 'nn', v_id)];
        v_cond := '{}';

        IF rec.kind IN ('numeric', 'text', 'temporal', 'boolean') THEN
            v_parts := v_parts || format('%L, count(DISTINCT %s)', 'dist', v_id);
        END IF;

        IF rec.kind = 'numeric' AND v_num_sent IS NOT NULL THEN
            v_cond := v_cond || format('%s::double precision = ANY (%L::double precision[])', v_id, v_num_sent);
        ELSIF rec.kind = 'temporal' THEN
            v_parts := v_parts || format('%L, min(%s)::text, %L, max(%s)::text', 'mn', v_id, 'mx', v_id);
        ELSIF rec.kind = 'text' THEN
            v_parts := v_parts || ARRAY[
                format('%L, count(*) FILTER (WHERE %s = %L)', 'emp', v_id, ''),
                format('%L, count(*) FILTER (WHERE %s <> %L AND btrim(%s) = %L)', 'ws', v_id, '', v_id, ''),
                format('%L, count(*) FILTER (WHERE btrim(%s) ~ %L)', 'nm', v_id, v_num_pat),
                format('%L, count(*) FILTER (WHERE btrim(%s) ~ %L)', 'dm', v_id, v_date_pat),
                format('%L, min(length(%s)), %L, max(length(%s))', 'lmin', v_id, 'lmax', v_id),
                format('%L, min(btrim(%s) COLLATE "C") FILTER (WHERE btrim(%s) ~ %L), %L, max(btrim(%s) COLLATE "C") FILTER (WHERE btrim(%s) ~ %L)',
                       'mn', v_id, v_id, v_date_pat, 'mx', v_id, v_id, v_date_pat)];
            IF v_txt_sent IS NOT NULL THEN
                v_cond := v_cond || format('lower(btrim(%s)) = ANY (%L::text[])', v_id, v_txt_sent);
            END IF;
            IF v_num_sent IS NOT NULL THEN
                v_cond := v_cond || format('(CASE WHEN btrim(%s) ~ %L THEN btrim(%s)::double precision END) = ANY (%L::double precision[])',
                                           v_id, v_num_pat, v_id, v_num_sent);
            END IF;
        END IF;

        IF cardinality(v_cond) > 0 THEN
            v_parts := v_parts || format('%L, count(*) FILTER (WHERE %s)', 'sent', array_to_string(v_cond, ' OR '));
        END IF;

        v_sel  := v_sel || format('jsonb_build_object(%s) AS %I', array_to_string(v_parts, ', '), 'c' || v_i);
        v_meta := v_meta || jsonb_build_object('name', rec.col_name, 'data_type', rec.data_type, 'kind', rec.kind);
    END LOOP;

    EXECUTE format('SELECT to_jsonb(s) FROM (SELECT count(*) AS total, %s FROM %I.%I) s',
                   array_to_string(v_sel, ', '), p_schema, p_table)
    INTO v_res;

    RETURN jsonb_build_object(
        'row_count', (v_res ->> 'total')::bigint,
        'columns', (SELECT coalesce(jsonb_agg(m.val || coalesce(v_res -> ('c' || m.ord), '{}'::jsonb) ORDER BY m.ord), '[]'::jsonb)
                    FROM jsonb_array_elements(v_meta) WITH ORDINALITY AS m(val, ord)));
END
$$;

-- Pass 2: numeric statistics for native numeric columns and numeric-looking text columns.
-- Sentinels and non-numeric text are excluded. Variance / stddev are sample (n-1) statistics.
-- Returns {column: {n, min, max, mean, sum, var, std, zeros, neg, q: [q1, median, q3]}}.
CREATE OR REPLACE FUNCTION data_profiling.profile_numeric(p_schema text, p_table text, p_cols text[], p_sentinels jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    rec     record;
    v_i     int := 0;
    v_names text[] := '{}';
    v_x     text[] := '{}';
    v_aggs  text[] := '{}';
    v_res   jsonb;
BEGIN
    FOR rec IN SELECT * FROM data_profiling.table_columns(p_schema, p_table)
               WHERE col_name = ANY (p_cols) AND kind IN ('numeric', 'text') ORDER BY ordinal LOOP
        v_i := v_i + 1;
        v_names := v_names || rec.col_name;
        v_x := v_x || format('%s AS %I', data_profiling.numeric_expr(format('%I', rec.col_name), rec.kind, p_sentinels), 'x' || v_i);
        v_aggs := v_aggs || format($f$jsonb_build_object('n', count(%1$I), 'min', min(%1$I), 'max', max(%1$I),
            'mean', avg(%1$I), 'sum', sum(%1$I), 'var', var_samp(%1$I), 'std', stddev_samp(%1$I),
            'zeros', count(*) FILTER (WHERE %1$I = 0), 'neg', count(*) FILTER (WHERE %1$I < 0),
            'q', percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY %1$I)) AS %2$I$f$,
            'x' || v_i, 'c' || v_i);
    END LOOP;
    IF v_i = 0 THEN
        RETURN '{}'::jsonb;
    END IF;

    EXECUTE format('SELECT to_jsonb(s) FROM (SELECT %s FROM (SELECT %s FROM %I.%I OFFSET 0) q) s',
                   array_to_string(v_aggs, ', '), array_to_string(v_x, ', '), p_schema, p_table)
    INTO v_res;

    RETURN (SELECT jsonb_object_agg(n.val, v_res -> ('c' || n.ord)) FROM unnest(v_names) WITH ORDINALITY AS n(val, ord));
END
$$;

-- Top-N value frequencies for several columns in ONE scan (unpivot through a lateral VALUES list).
-- Percentages use the column's non-null values as denominator; the remainder goes into one 'other' bucket.
CREATE OR REPLACE FUNCTION data_profiling.profile_categories(p_schema text, p_table text, p_cols text[], p_top_n int DEFAULT 20)
RETURNS TABLE (column_name text, category_value text, frequency bigint, percentage double precision, is_other boolean)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_vals text;
BEGIN
    SELECT string_agg(format('(%L, t.%I::text)', c, c), ', ') INTO v_vals FROM unnest(p_cols) AS c;
    IF v_vals IS NULL THEN
        RETURN;
    END IF;
    RETURN QUERY EXECUTE format($f$
        WITH f AS (
            SELECT u.col, u.val, count(*) AS n
            FROM %1$I.%2$I t CROSS JOIN LATERAL (VALUES %3$s) AS u(col, val)
            WHERE u.val IS NOT NULL
            GROUP BY u.col, u.val),
        r AS (
            SELECT col, val, n,
                   row_number() OVER (PARTITION BY col ORDER BY n DESC, val) AS rn,
                   sum(n) OVER (PARTITION BY col) AS tot
            FROM f)
        SELECT col, val, n, (100.0 * n / tot)::double precision, false FROM r WHERE rn <= %4$s
        UNION ALL
        SELECT col, NULL::text, sum(n)::bigint, (100.0 * sum(n) / max(tot))::double precision, true
        FROM r WHERE rn > %4$s GROUP BY col$f$,
        p_schema, p_table, v_vals, p_top_n);
END
$$;

-- Pairwise-complete Pearson r and n for all column pairs in ONE scan. Sentinels / non-numeric -> NULL first.
CREATE OR REPLACE FUNCTION data_profiling.pearson_pairs(p_schema text, p_table text, p_cols text[], p_sentinels jsonb DEFAULT '{}'::jsonb)
RETURNS TABLE (col_a text, col_b text, pearson_r double precision, n_pairs bigint)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    rec     record;
    v_i     int := 0;
    v_names text[] := '{}';
    v_x     text[] := '{}';
    v_aggs  text[] := '{}';
    v_res   jsonb;
BEGIN
    FOR rec IN SELECT * FROM data_profiling.table_columns(p_schema, p_table)
               WHERE col_name = ANY (p_cols) AND kind IN ('numeric', 'text') ORDER BY ordinal LOOP
        v_i := v_i + 1;
        v_names := v_names || rec.col_name;
        v_x := v_x || format('%s AS %I', data_profiling.numeric_expr(format('%I', rec.col_name), rec.kind, p_sentinels), 'x' || v_i);
    END LOOP;
    IF v_i < 2 THEN
        RETURN;
    END IF;
    FOR i IN 1 .. v_i - 1 LOOP
        FOR j IN i + 1 .. v_i LOOP
            v_aggs := v_aggs || format('corr(%1$I, %2$I) AS %3$I, regr_count(%1$I, %2$I) AS %4$I',
                                       'x' || i, 'x' || j, 'r_' || i || '_' || j, 'n_' || i || '_' || j);
        END LOOP;
    END LOOP;

    EXECUTE format('SELECT to_jsonb(s) FROM (SELECT %s FROM (SELECT %s FROM %I.%I OFFSET 0) q) s',
                   array_to_string(v_aggs, ', '), array_to_string(v_x, ', '), p_schema, p_table)
    INTO v_res;

    FOR i IN 1 .. v_i - 1 LOOP
        FOR j IN i + 1 .. v_i LOOP
            col_a     := v_names[i];
            col_b     := v_names[j];
            pearson_r := (v_res ->> format('r_%s_%s', i, j))::double precision;
            n_pairs   := (v_res ->> format('n_%s_%s', i, j))::bigint;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END
$$;

-- Cleaned numeric columns for rank-based statistics done in Python. Deterministic BERNOULLI sample when p_sample_pct < 100.
-- Returns {names: [...], values: [[col1 values], [col2 values], ...]} (NULL = missing / sentinel).
CREATE OR REPLACE FUNCTION data_profiling.numeric_sample(p_schema text, p_table text, p_cols text[], p_sentinels jsonb DEFAULT '{}'::jsonb, p_sample_pct double precision DEFAULT 100)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    rec     record;
    v_i     int := 0;
    v_names text[] := '{}';
    v_x     text[] := '{}';
    v_aggs  text[] := '{}';
    v_res   jsonb;
BEGIN
    FOR rec IN SELECT * FROM data_profiling.table_columns(p_schema, p_table)
               WHERE col_name = ANY (p_cols) AND kind IN ('numeric', 'text') ORDER BY ordinal LOOP
        v_i := v_i + 1;
        v_names := v_names || rec.col_name;
        v_x := v_x || format('%s AS %I', data_profiling.numeric_expr(format('%I', rec.col_name), rec.kind, p_sentinels), 'x' || v_i);
        v_aggs := v_aggs || format('jsonb_agg(%I)', 'x' || v_i);
    END LOOP;
    EXECUTE format('SELECT jsonb_build_array(%s) FROM (SELECT %s FROM %I.%I %s OFFSET 0) q',
                   array_to_string(v_aggs, ', '), array_to_string(v_x, ', '), p_schema, p_table,
                   CASE WHEN p_sample_pct < 100 THEN format('TABLESAMPLE BERNOULLI (%s) REPEATABLE (42)', p_sample_pct) ELSE '' END)
    INTO v_res;
    RETURN jsonb_build_object('names', to_jsonb(v_names), 'values', v_res);
END
$$;

-- Whole-row duplicate count (extra copies beyond the first). Expensive: the caller gates it by table size.
CREATE OR REPLACE FUNCTION data_profiling.duplicate_rows(p_schema text, p_table text) RETURNS bigint
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v bigint;
BEGIN
    EXECUTE format('SELECT count(*) - count(DISTINCT t::text) FROM %I.%I t', p_schema, p_table) INTO v;
    RETURN v;
END
$$;

-- PostGIS profile of one geometry column in ONE scan; validity is computed once per row.
-- Returns counts (empty / valid / invalid among non-null), distinct geometry types, coord dimension, extent, SRID and CRS authority.
CREATE OR REPLACE FUNCTION data_profiling.profile_geometry(p_schema text, p_table text, p_col text)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_srid int := Find_SRID(p_schema, p_table, p_col);
    v_res  jsonb;
BEGIN
    EXECUTE format($f$
        SELECT jsonb_build_object(
            'total', total, 'non_null', non_null, 'empty', empty, 'valid', valid, 'invalid', invalid,
            'reasons', (reasons)[1:5], 'types', types, 'coord_dim', coord_dim,
            'xmin', ST_XMin(ext), 'ymin', ST_YMin(ext), 'xmax', ST_XMax(ext), 'ymax', ST_YMax(ext))
        FROM (
            SELECT count(*) AS total,
                   count(g) AS non_null,
                   count(*) FILTER (WHERE e) AS empty,
                   count(*) FILTER (WHERE (d).valid) AS valid,
                   count(*) FILTER (WHERE NOT (d).valid) AS invalid,
                   array_agg(DISTINCT (d).reason) FILTER (WHERE NOT (d).valid) AS reasons,
                   array_agg(DISTINCT GeometryType(g)) FILTER (WHERE NOT e) AS types,
                   max(ST_CoordDim(g)) FILTER (WHERE NOT e) AS coord_dim,
                   ST_Extent(g) FILTER (WHERE NOT e) AS ext
            FROM (
                SELECT g, e, CASE WHEN e IS DISTINCT FROM false THEN NULL ELSE ST_IsValidDetail(g) END AS d
                FROM (SELECT %1$I AS g, ST_IsEmpty(%1$I) AS e FROM %2$I.%3$I) x
                OFFSET 0) q
        ) a$f$, p_col, p_schema, p_table)
    INTO v_res;

    RETURN v_res || jsonb_build_object(
        'srid', v_srid,
        'auth_name', (SELECT auth_name FROM spatial_ref_sys WHERE srid = v_srid),
        'auth_srid', (SELECT auth_srid FROM spatial_ref_sys WHERE srid = v_srid));
END
$$;
