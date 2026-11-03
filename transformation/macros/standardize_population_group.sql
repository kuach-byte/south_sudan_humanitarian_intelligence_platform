{% macro standardize_population_group(column) %}
    case
        when nullif(btrim({{ column }}), '') is null then null
        when lower(regexp_replace(btrim({{ column }}), '\s+', ' ', 'g'))
            in ('idp', 'idps', 'internally displaced', 'internally displaced persons') then 'idp'
        when lower(regexp_replace(btrim({{ column }}), '\s+', ' ', 'g'))
            in ('host community', 'host communities', 'non-displaced', 'non displaced') then 'host_community'
        when lower(regexp_replace(btrim({{ column }}), '\s+', ' ', 'g'))
            in ('returnee', 'returnees') then 'returnee'
        when lower(regexp_replace(btrim({{ column }}), '\s+', ' ', 'g'))
            in ('refugee', 'refugees') then 'refugee'
        else regexp_replace(lower(btrim({{ column }})), '[^a-z0-9]+', '_', 'g')
    end
{% endmacro %}