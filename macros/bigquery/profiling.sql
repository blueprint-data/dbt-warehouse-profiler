{% macro bigquery__list_schemas(exclude_schemas=[]) %}

  {% set default_region = var('warehouse_profiler:bigquery:default_region', 'us') %}

  {% set default_excludes = var('warehouse_profiler:bigquery:exclude_schemas', []) %}

  {% set all_excludes = exclude_schemas + default_excludes %}

  {% set query = "

    SELECT DISTINCT table_schema

    FROM `region-" + default_region + "`.INFORMATION_SCHEMA.TABLES

  " %}

  {% if all_excludes %}

    {% set query = query + " WHERE table_schema NOT IN ('" + all_excludes | join("','") + "')" %}

  {% endif %}

  {% set query = query + " ORDER BY table_schema" %}

  {% set results = run_query(query) %}

  {% for row in results %}

    {{ log(row[0], info=True) }}

  {% endfor %}

{% endmacro %}

{% macro bigquery__list_tables(schema) %}

  {% set query = "

    SELECT table_name, table_type

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.TABLES`

  " %}

  {% set results = run_query(query) %}

  {% for row in results %}

    {{ log(row[0] + ' (' + row[1] + ')', info=True) }}

  {% endfor %}

{% endmacro %}

{% macro bigquery__list_columns(schema, table) %}

  {% set query = "

    SELECT column_name, data_type, is_nullable

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.COLUMNS`

    WHERE table_name = '" + table + "'

    ORDER BY ordinal_position

  " %}

  {% set results = run_query(query) %}

  {% for row in results %}

    {{ log(row[0] + ': ' + row[1] + ' (' + row[2] + ')', info=True) }}

  {% endfor %}

{% endmacro %}

{% macro bigquery__profile_table(schema, table) %}

  {% set max_rows = var('warehouse_profiler:bigquery:max_preview_rows', 10) %}

  {% set full_table = '`' + target.project + '.' + schema + '.' + table + '`' %}

  {% set metadata_query = "

    SELECT
      table_id,
      row_count,
      size_bytes,
      TIMESTAMP_MILLIS(last_modified_time) AS last_modified
    FROM `" + target.project + "." + schema + ".__TABLES__`
    WHERE table_id = '" + table + "'

  " %}

  {% set metadata_results = run_query(metadata_query) %}

  {% if metadata_results and metadata_results.rows | length > 0 %}

    {{ log('Row count: ' + (metadata_results[0][1] | string), info=True) }}

    {{ log('Size (bytes): ' + (metadata_results[0][2] | string), info=True) }}

    {{ log('Last modified: ' + (metadata_results[0][3] | string), info=True) }}

  {% endif %}

  {% set preview_query = 'SELECT * FROM ' + full_table + ' LIMIT ' + max_rows | string %}

  {% set preview_results = run_query(preview_query) %}

  {{ log('Preview (first ' + max_rows | string + ' rows):', info=True) }}

  {% for row in preview_results %}

    {{ log(row | string, info=True) }}

  {% endfor %}

  {% set ddl_query = "

    SELECT ddl

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.TABLES`

    WHERE table_name = '" + table + "'

  " %}

  {% set ddl_results = run_query(ddl_query) %}

  {% if ddl_results and ddl_results.rows | length > 0 and ddl_results[0][0] %}

    {% set ddl = ddl_results[0][0] %}

    {% if 'PARTITION BY' in ddl %}

      {{ log('Partitioned: Yes', info=True) }}

    {% else %}

      {{ log('Partitioned: No', info=True) }}

    {% endif %}

    {% if 'CLUSTER BY' in ddl %}

      {{ log('Clustered: Yes', info=True) }}

    {% else %}

      {{ log('Clustered: No', info=True) }}

    {% endif %}

  {% endif %}

{% endmacro %}
