{% macro snowflake__list_databases() %}

  {% set query = "
    SHOW DATABASES
  " %}

  {% set results = run_query(query) %}

  {% if results %}
    {# SHOW DATABASES returns: created_on, name, is_default, is_current, origin, owner, comment, options, retention_time #}
    {% for row in results %}
      {{ log(row[1], info=True) }}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro snowflake__list_schemas(database=none, exclude_schemas=[]) %}

  {% set default_excludes = var('dbt_warehouse_profiler:snowflake:exclude_schemas', []) %}

  {% set all_excludes = exclude_schemas + default_excludes %}

  {# Use provided database or fall back to target database #}
  {% set db = database if database else target.database %}

  {% set query = "
    SELECT DISTINCT table_schema
    FROM " + db + ".INFORMATION_SCHEMA.TABLES
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

{% macro snowflake__list_tables(schema, database=none) %}

  {# Use provided database or fall back to target database #}
  {% set db = database if database else target.database %}

  {% set query = "
    SELECT table_name, table_type
    FROM " + db + ".INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '" + schema + "'
    ORDER BY table_name
  " %}

  {% set results = run_query(query) %}

  {% for row in results %}
    {{ log(row[0] + ' (' + row[1] + ')', info=True) }}
  {% endfor %}

{% endmacro %}

{% macro snowflake__list_columns(schema, table, database=none) %}

  {# Use provided database or fall back to target database #}
  {% set db = database if database else target.database %}

  {% set query = "
    SELECT column_name, data_type, is_nullable
    FROM " + db + ".INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '" + schema + "'
      AND table_name = '" + table + "'
    ORDER BY ordinal_position
  " %}

  {% set results = run_query(query) %}

  {% for row in results %}
    {{ log(row[0] + ': ' + row[1] + ' (' + row[2] + ')', info=True) }}
  {% endfor %}

{% endmacro %}

{% macro snowflake__profile_table(schema, table, database=none) %}

  {% set max_rows = var('dbt_warehouse_profiler:snowflake:max_preview_rows', 10) %}

  {# Use provided database or fall back to target database #}
  {% set db = database if database else target.database %}

  {% set full_table = db + '.' + schema + '.' + table %}

  {# Get table metadata from INFORMATION_SCHEMA.TABLES #}
  {% set metadata_query = "
    SELECT
      row_count,
      bytes,
      created,
      last_altered
    FROM " + db + ".INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '" + schema + "'
      AND table_name = '" + table + "'
  " %}

  {% set metadata_results = run_query(metadata_query) %}

  {% if metadata_results and metadata_results.rows | length > 0 %}
    {{ log('Row count: ' + (metadata_results[0][0] | string), info=True) }}
    {{ log('Size (bytes): ' + (metadata_results[0][1] | string), info=True) }}
    {{ log('Created: ' + (metadata_results[0][2] | string), info=True) }}
    {{ log('Last altered: ' + (metadata_results[0][3] | string), info=True) }}
  {% endif %}

  {# Get clustering information if available #}
  {% set clustering_query = "
    SELECT clustering_key
    FROM " + db + ".INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '" + schema + "'
      AND table_name = '" + table + "'
      AND clustering_key IS NOT NULL
  " %}

  {% set clustering_results = run_query(clustering_query) %}

  {% if clustering_results and clustering_results.rows | length > 0 and clustering_results[0][0] %}
    {{ log('Clustering key: ' + clustering_results[0][0], info=True) }}
  {% else %}
    {{ log('Clustering key: None', info=True) }}
  {% endif %}

  {# Preview data #}
  {% set preview_query = 'SELECT * FROM ' + full_table + ' LIMIT ' + max_rows | string %}

  {% set preview_results = run_query(preview_query) %}

  {{ log('Preview (first ' + max_rows | string + ' rows):', info=True) }}

  {% for row in preview_results %}
    {{ log(row | string, info=True) }}
  {% endfor %}

{% endmacro %}
