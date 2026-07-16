{% macro get_columns_in_relation_raw(relation) -%}
  {{ return(adapter.dispatch('get_columns_in_relation_raw', 'dbt')(relation)) }}
{%- endmacro -%}

{% macro fabricspark__get_columns_in_relation_raw(relation) -%}
  {% call statement('get_columns_in_relation_raw', fetch_result=True) %}
      describe table extended {{ relation }}
  {% endcall %}
  {% do return(load_result('get_columns_in_relation_raw').table) %}
{% endmacro %}

{#- dbt Fusion (core v2 and later) expects Column objects from
    get_columns_in_relation, while dbt-core 1.x expects the raw agate table.
    Encapsulated here so the capability check lives in exactly one place and
    compares the major version numerically rather than by string prefix. -#}
{% macro fabricspark__requires_column_objects() %}
  {% do return((dbt_version.split('.')[0] | int) >= 2) %}
{% endmacro %}

{% macro fabricspark__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      describe table extended {{ relation.render() }}
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {% if fabricspark__requires_column_objects() %}
    {% do return(sql_convert_columns_in_relation(table)) %}
  {% else %}
    {% do return(table) %}
  {% endif %}
{% endmacro %}

{% macro fabricspark__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} alter column {{ column_name }} type {{ new_column_type }};
  {% endcall %}
{% endmacro %}

{% macro fabricspark__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if remove_columns %}
    {% if relation.is_delta %}
      {{ exceptions.raise_compiler_error('Delta tables does not support dropping columns from tables') }}
    {% endif %}
  {% endif %}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}

  {% set sql -%}

     alter {{ relation.type }} {{ relation }}

       {% if add_columns %} add columns {% endif %}
            {% for column in add_columns %}
               {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
            {% endfor %}

  {%- endset -%}

  {% do run_query(sql) %}

{% endmacro %}

{% macro fabricspark__alter_column_comment(relation, column_dict) %}
  {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        {% if relation.is_delta %}
          alter table {{ relation }} change column
              {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
              comment '{{ escaped_comment }}';
        {% else %}
          {{ exceptions.raise_compiler_error('Fabric Spark does not support formats other than delta -'~ relation.is_delta) }}
        {% endif %}
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
{% endmacro %}



{% macro get_column_comment_sql(column_name, column_dict) -%}
  {% if column_name in column_dict and column_dict[column_name]["description"] -%}
    {% set escaped_description = column_dict[column_name]["description"] | replace("'", "\\'") %}
    {% set column_comment_clause = "comment '" ~ escaped_description ~ "'" %}
  {%- endif -%}
  {{ adapter.quote(column_name) }} {{ column_comment_clause }}
{% endmacro %}

{% macro get_persist_docs_column_list(model_columns, query_columns) %}
  {% for column_name in query_columns %}
    {{ get_column_comment_sql(column_name, model_columns) }}
    {{- ", " if not loop.last else "" }}
  {% endfor %}
{% endmacro %}
