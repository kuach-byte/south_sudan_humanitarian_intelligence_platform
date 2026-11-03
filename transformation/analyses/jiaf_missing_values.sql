{% set selects = [] %}
{% for y in ['2025', '2026'] %}
    {% set rel = ref('int_jiaf_' ~ y) %}
    {% for col in adapter.get_columns_in_relation(rel) %}
        {% do selects.append(
            "select '" ~ y ~ "' as reporting_year, '" ~ col.name ~ "' as column_name, "
            ~ "count(*) as total_rows, count(*) - count(" ~ adapter.quote(col.name) ~ ") as missing_count, "
            ~ "round(100.0 * (count(*) - count(" ~ adapter.quote(col.name) ~ ")) / nullif(count(*), 0), 2) as missing_pct "
            ~ "from " ~ rel
        ) %}
    {% endfor %}
{% endfor %}
{{ selects | join('\nunion all\n') }}
order by reporting_year, column_name