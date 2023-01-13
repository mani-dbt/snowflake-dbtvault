{%- set source_model = ["v_stg_inventory"] -%}
{%- set src_pk = "PART_PK" -%}
{%- set src_nk = "PARTKEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}