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

{% macro snowflake__list_database_schemas(database=none, exclude_schemas=[]) %}

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

{% macro snowflake__profile_columns(schema, table, database=none) %}

  {% set max_sample_values = var('dbt_warehouse_profiler:snowflake:max_sample_values', 5) %}

  {# Use provided database or fall back to target database #}
  {% set db = database if database else target.database %}

  {% set full_table = db + '.' + schema + '.' + table %}

  {# Phase 1: Get column metadata #}
  {% set metadata_query %}
    SELECT
      column_name,
      data_type,
      is_nullable
    FROM {{ db }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{{ schema }}'
      AND table_name = '{{ table }}'
    ORDER BY ordinal_position
  {% endset %}

  {% set columns = run_query(metadata_query) %}

  {% if not columns or columns.rows | length == 0 %}
    {{ log('No columns found for table ' + schema + '.' + table, info=True) }}
    {{ return('') }}
  {% endif %}

  {# Get row count first #}
  {% set count_query = 'SELECT COUNT(*) as cnt FROM ' + full_table %}
  {% set count_result = run_query(count_query) %}
  {% set total_rows = count_result[0][0] if count_result and count_result.rows | length > 0 else 0 %}

  {{ log('', info=True) }}
  {{ log('=== Column Profile for ' + schema + '.' + table + ' ===', info=True) }}
  {{ log('', info=True) }}
  {{ log('Total rows: ' + (total_rows | string), info=True) }}

  {% if total_rows == 0 %}
    {{ log('', info=True) }}
    {{ log('Table is empty - no statistics to compute', info=True) }}
    {{ return('') }}
  {% endif %}

  {# Classify columns and build statistics query #}
  {% set numeric_types = ['NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'BYTEINT', 'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DOUBLE PRECISION', 'REAL'] %}
  {% set date_types = ['DATE', 'DATETIME', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ'] %}
  {% set string_types = ['VARCHAR', 'CHAR', 'CHARACTER', 'STRING', 'TEXT', 'BINARY', 'VARBINARY'] %}
  {% set boolean_types = ['BOOLEAN'] %}
  {% set complex_types = ['VARIANT', 'OBJECT', 'ARRAY'] %}

  {# Track columns with nulls for summary #}
  {% set ns = namespace(columns_with_nulls=0) %}

  {# Process each column individually to avoid query size limits #}
  {% for col in columns %}
    {% set col_name = col[0] %}
    {% set col_type = col[1] | upper %}
    {% set col_nullable = col[2] %}

    {# Determine base type (handle parameterized types like NUMBER(10,2)) #}
    {% set base_type = col_type.split('(')[0] %}

    {{ log('', info=True) }}
    {{ log('Column: ' + col_name + ' (' + col_type + ', ' + ('NULLABLE' if col_nullable == 'YES' else 'NOT NULL') + ')', info=True) }}

    {# Build query based on column type #}
    {% if base_type in complex_types %}

      {# Complex types - only null count #}
      {% set stats_query %}
        SELECT
          COUNT_IF("{{ col_name }}" IS NULL) as null_count
        FROM {{ full_table }}
      {% endset %}

      {% set stats = run_query(stats_query) %}
      {% if stats and stats.rows | length > 0 %}
        {% set null_count = stats[0][0] %}
        {% set null_pct = (null_count / total_rows * 100) | round(2) %}
        {{ log('  Nulls: ' + (null_count | string) + ' (' + (null_pct | string) + '%)', info=True) }}
        {{ log('  [Complex type - limited statistics]', info=True) }}
        {% if null_count > 0 %}
          {% set ns.columns_with_nulls = ns.columns_with_nulls + 1 %}
        {% endif %}
      {% endif %}

    {% elif base_type in boolean_types %}

      {# Boolean - null count, distinct, true/false counts #}
      {% set stats_query %}
        SELECT
          COUNT_IF("{{ col_name }}" IS NULL) as null_count,
          COUNT(DISTINCT "{{ col_name }}") as distinct_count,
          COUNT_IF("{{ col_name }}" = TRUE) as true_count,
          COUNT_IF("{{ col_name }}" = FALSE) as false_count
        FROM {{ full_table }}
      {% endset %}

      {% set stats = run_query(stats_query) %}
      {% if stats and stats.rows | length > 0 %}
        {% set null_count = stats[0][0] %}
        {% set distinct_count = stats[0][1] %}
        {% set true_count = stats[0][2] %}
        {% set false_count = stats[0][3] %}
        {% set null_pct = (null_count / total_rows * 100) | round(2) %}
        {% set non_null_total = true_count + false_count %}
        {% set true_pct = ((true_count / non_null_total * 100) | round(1)) if non_null_total > 0 else 0 %}
        {% set false_pct = ((false_count / non_null_total * 100) | round(1)) if non_null_total > 0 else 0 %}

        {{ log('  Nulls: ' + (null_count | string) + ' (' + (null_pct | string) + '%)', info=True) }}
        {{ log('  Distinct: ' + (distinct_count | string), info=True) }}
        {{ log('  True: ' + (true_count | string) + ' (' + (true_pct | string) + '%) | False: ' + (false_count | string) + ' (' + (false_pct | string) + '%)', info=True) }}
        {% if null_count > 0 %}
          {% set ns.columns_with_nulls = ns.columns_with_nulls + 1 %}
        {% endif %}
      {% endif %}

    {% elif base_type in numeric_types %}

      {# Numeric - null count, distinct, min, max, avg #}
      {% set stats_query %}
        SELECT
          COUNT_IF("{{ col_name }}" IS NULL) as null_count,
          COUNT(DISTINCT "{{ col_name }}") as distinct_count,
          MIN("{{ col_name }}") as min_val,
          MAX("{{ col_name }}") as max_val,
          AVG(TRY_CAST("{{ col_name }}" AS FLOAT)) as avg_val
        FROM {{ full_table }}
      {% endset %}

      {% set stats = run_query(stats_query) %}
      {% if stats and stats.rows | length > 0 %}
        {% set null_count = stats[0][0] %}
        {% set distinct_count = stats[0][1] %}
        {% set min_val = stats[0][2] %}
        {% set max_val = stats[0][3] %}
        {% set avg_val = stats[0][4] %}
        {% set null_pct = (null_count / total_rows * 100) | round(2) %}

        {{ log('  Nulls: ' + (null_count | string) + ' (' + (null_pct | string) + '%)', info=True) }}
        {{ log('  Distinct: ' + (distinct_count | string), info=True) }}
        {% if min_val is not none %}
          {% set avg_display = (avg_val | round(2) | string) if avg_val is not none else 'N/A' %}
          {{ log('  Min: ' + (min_val | string) + ' | Max: ' + (max_val | string) + ' | Avg: ' + avg_display, info=True) }}
        {% endif %}
        {% if null_count > 0 %}
          {% set ns.columns_with_nulls = ns.columns_with_nulls + 1 %}
        {% endif %}
      {% endif %}

    {% elif base_type in date_types %}

      {# Date/Time - null count, distinct, min, max #}
      {% set stats_query %}
        SELECT
          COUNT_IF("{{ col_name }}" IS NULL) as null_count,
          COUNT(DISTINCT "{{ col_name }}") as distinct_count,
          MIN("{{ col_name }}") as min_val,
          MAX("{{ col_name }}") as max_val
        FROM {{ full_table }}
      {% endset %}

      {% set stats = run_query(stats_query) %}
      {% if stats and stats.rows | length > 0 %}
        {% set null_count = stats[0][0] %}
        {% set distinct_count = stats[0][1] %}
        {% set min_val = stats[0][2] %}
        {% set max_val = stats[0][3] %}
        {% set null_pct = (null_count / total_rows * 100) | round(2) %}

        {{ log('  Nulls: ' + (null_count | string) + ' (' + (null_pct | string) + '%)', info=True) }}
        {{ log('  Distinct: ' + (distinct_count | string), info=True) }}
        {% if min_val is not none %}
          {{ log('  Min: ' + (min_val | string) + ' | Max: ' + (max_val | string), info=True) }}
        {% endif %}
        {% if null_count > 0 %}
          {% set ns.columns_with_nulls = ns.columns_with_nulls + 1 %}
        {% endif %}
      {% endif %}

    {% else %}

      {# String/Other - null count, distinct, sample values #}
      {% set stats_query %}
        SELECT
          COUNT_IF("{{ col_name }}" IS NULL) as null_count,
          COUNT(DISTINCT "{{ col_name }}") as distinct_count
        FROM {{ full_table }}
      {% endset %}

      {% set stats = run_query(stats_query) %}

      {# Get sample values using a subquery with LIMIT #}
      {% set sample_query %}
        SELECT DISTINCT "{{ col_name }}" as sample_val
        FROM {{ full_table }}
        WHERE "{{ col_name }}" IS NOT NULL
        LIMIT {{ max_sample_values }}
      {% endset %}

      {% set samples = run_query(sample_query) %}

      {% if stats and stats.rows | length > 0 %}
        {% set null_count = stats[0][0] %}
        {% set distinct_count = stats[0][1] %}
        {% set null_pct = (null_count / total_rows * 100) | round(2) %}

        {{ log('  Nulls: ' + (null_count | string) + ' (' + (null_pct | string) + '%)', info=True) }}
        {{ log('  Distinct: ' + (distinct_count | string), info=True) }}

        {% if samples and samples.rows | length > 0 %}
          {% set sample_list = [] %}
          {% for sample in samples %}
            {% if sample[0] is not none %}
              {% set sample_str = sample[0] | string %}
              {% if sample_str | length > 50 %}
                {% set sample_str = sample_str[:47] + '...' %}
              {% endif %}
              {% do sample_list.append("'" + sample_str + "'") %}
            {% endif %}
          {% endfor %}
          {% if sample_list | length > 0 %}
            {{ log('  Samples: [' + sample_list | join(', ') + ']', info=True) }}
          {% endif %}
        {% endif %}

        {% if null_count > 0 %}
          {% set ns.columns_with_nulls = ns.columns_with_nulls + 1 %}
        {% endif %}
      {% endif %}

    {% endif %}

  {% endfor %}

  {# Summary #}
  {{ log('', info=True) }}
  {{ log('=== Summary ===', info=True) }}
  {{ log('Columns analyzed: ' + (columns.rows | length | string), info=True) }}
  {{ log('Columns with nulls: ' + (ns.columns_with_nulls | string), info=True) }}

{% endmacro %}

{% macro snowflake__execute_raw_query(query) %}

  {# Execute the raw query #}
  {% set results = run_query(query) %}

  {# Display results #}
  {% if results %}
    {% set row_count = results.rows | length %}
    {% set col_count = results.columns | length %}
    
    {{ log('', info=True) }}
    {{ log('Query executed successfully', info=True) }}
    {{ log('Rows returned: ' + (row_count | string), info=True) }}
    {{ log('Columns: ' + (col_count | string), info=True) }}
    {{ log('', info=True) }}
    
    {% if row_count > 0 %}
      {# Display column headers #}
      {% set header_parts = [] %}
      {% for col_name in results.columns %}
        {% do header_parts.append(col_name) %}
      {% endfor %}
      {{ log(header_parts | join(' | '), info=True) }}
      {{ log('-' * 80, info=True) }}
      
      {# Display rows #}
      {% for row in results %}
        {{ log(row | string, info=True) }}
      {% endfor %}
    {% else %}
      {{ log('No rows returned', info=True) }}
    {% endif %}
    
  {% else %}
    {{ log('Query executed but returned no results', info=True) }}
  {% endif %}

{% endmacro %}
