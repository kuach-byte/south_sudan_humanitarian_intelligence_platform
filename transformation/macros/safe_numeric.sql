{% macro safe_numeric(col) -%}
case when btrim({{ adapter.quote(col) }}) ~ '^[-+]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][-+]?[0-9]{1,2})?$'
     then btrim({{ adapter.quote(col) }})::numeric end
{%- endmacro %}
