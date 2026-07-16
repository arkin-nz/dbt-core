{% macro fabricspark__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {%- if relation.database -%}
      {#- Schema-enabled lakehouse: schemas live inside the lakehouse (database) -#}
      create schema if not exists {{ relation }}
    {%- else -%}
      {#- Classic lakehouse: single-database, schemas cannot be created -#}
      select 1
    {%- endif -%}
  {% endcall %}
{% endmacro %}

{% macro fabricspark__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    {%- if relation.database -%}
      drop schema if exists {{ relation }} cascade
    {%- else -%}
      select 1
    {%- endif -%}
  {%- endcall -%}
{% endmacro %}

{% macro fabricspark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro fabricspark__generate_database_name(custom_database_name=none, node=none) -%}
  {#- Classic lakehouses are single-database (2-part naming): no database part.
      Schema-enabled lakehouses (profile: lakehouse_schemas: true) carry the
      lakehouse name in target.database. -#}
  {%- if target.database -%}
    {{ return((custom_database_name or target.database) | trim) }}
  {%- else -%}
    {% do return(None) %}
  {%- endif -%}
{%- endmacro %}

{% macro fabricspark__check_schema_exists_like(schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) %}
    show databases like '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}
