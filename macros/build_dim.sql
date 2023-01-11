{% macro dim_type1( pk, hub_table, sat_table ) %}
select * 
from 
  {{ ref( hub_table ) }} hub,
  {{ ref( sat_table) }} sat
where
  hub.{{ pk }} = sat.{{ pk }}
and sat.load_date = (
  select 
    max(sat2.load_date)
  from
    {{ ref( sat_table ) }} sat2
  where
    hub.{{ pk }} = sat2.{{ pk }}
)
{% endmacro %}