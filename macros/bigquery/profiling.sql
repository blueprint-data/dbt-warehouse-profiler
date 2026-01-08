{% macro bigquery__list_databases() %}

  {#
    For BigQuery, a "database" is the project.

    NOTE: This macro only returns the current project from the dbt profile.
    BigQuery's INFORMATION_SCHEMA is project-scoped, so there's no SQL-based
    way to list all accessible projects without using the Cloud Resource Manager API.

    For exploring data sources within the current project, use list_database_schemas
    to see all available datasets.
  #}
  {{ log(target.project, info=True) }}

{% endmacro %}

{% macro bigquery__list_database_schemas(database=none, exclude_schemas=[]) %}

  {% set default_region = var('dbt_warehouse_profiler:bigquery:default_region', 'us') %}

  {% set default_excludes = var('dbt_warehouse_profiler:bigquery:exclude_schemas', []) %}

  {% set all_excludes = exclude_schemas + default_excludes %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}

  {% set query = "
    SELECT schema_name
    FROM `region-" + default_region + ".INFORMATION_SCHEMA.SCHEMATA`
  " %}

  {% if all_excludes %}
    {% set query = query + " WHERE schema_name NOT IN ('" + all_excludes | join("','") + "')" %}
  {% endif %}

  {% set query = query + " ORDER BY schema_name" %}

  {% set results = run_query(query) %}

  {% for row in results %}
    {{ log(row[0], info=True) }}
  {% endfor %}

{% endmacro %}

{% macro bigquery__list_tables(schema, database=none) %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}

  {% set query = "

    SELECT table_name, table_type

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.TABLES`

  " %}

  {% set results = run_query(query) %}

  {% for row in results %}

    {{ log(row[0] + ' (' + row[1] + ')', info=True) }}

  {% endfor %}

{% endmacro %}

{% macro bigquery__list_columns(schema, table, database=none) %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}

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

{% macro bigquery__profile_table(schema, table, database=none) %}

  {% set max_rows = var('dbt_warehouse_profiler:bigquery:max_preview_rows', 10) %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}

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

{% macro bigquery__profile_columns(schema, table, database=none) %}

  {% set max_sample_values = var('dbt_warehouse_profiler:bigquery:max_sample_values', 5) %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}
  {% set full_table = '`' + target.project + '.' + schema + '.' + table + '`' %}

  {# Phase 1: Get column metadata #}
  {% set metadata_query %}
    SELECT
      column_name,
      data_type,
      is_nullable
    FROM `{{ target.project }}.{{ schema }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{{ table }}'
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
  {% set numeric_types = ['INT64', 'INT', 'INTEGER', 'SMALLINT', 'BIGINT', 'FLOAT64', 'FLOAT', 'NUMERIC', 'BIGNUMERIC', 'DECIMAL'] %}
  {% set date_types = ['DATE', 'DATETIME', 'TIMESTAMP', 'TIME'] %}
  {% set string_types = ['STRING', 'BYTES'] %}
  {% set boolean_types = ['BOOL', 'BOOLEAN'] %}
  {% set complex_types = ['ARRAY', 'STRUCT', 'JSON', 'GEOGRAPHY'] %}

  {# Track columns with nulls for summary #}
  {% set ns = namespace(columns_with_nulls=0) %}

  {# Process each column individually to avoid query size limits #}
  {% for col in columns %}
    {% set col_name = col[0] %}
    {% set col_type = col[1] | upper %}
    {% set col_nullable = col[2] %}

    {# Determine base type (handle parameterized types like NUMERIC(10,2)) #}
    {% set base_type = col_type.split('(')[0].split('<')[0] %}

    {{ log('', info=True) }}
    {{ log('Column: ' + col_name + ' (' + col_type + ', ' + ('NULLABLE' if col_nullable == 'YES' else 'NOT NULL') + ')', info=True) }}

    {# Build query based on column type #}
    {% if base_type in complex_types or col_type.startswith('ARRAY') or col_type.startswith('STRUCT') %}

      {# Complex types - only null count #}
      {% set stats_query %}
        SELECT
          COUNTIF(`{{ col_name }}` IS NULL) as null_count
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
          COUNTIF(`{{ col_name }}` IS NULL) as null_count,
          COUNT(DISTINCT `{{ col_name }}`) as distinct_count,
          COUNTIF(`{{ col_name }}` = TRUE) as true_count,
          COUNTIF(`{{ col_name }}` = FALSE) as false_count
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
          COUNTIF(`{{ col_name }}` IS NULL) as null_count,
          COUNT(DISTINCT `{{ col_name }}`) as distinct_count,
          MIN(`{{ col_name }}`) as min_val,
          MAX(`{{ col_name }}`) as max_val,
          AVG(SAFE_CAST(`{{ col_name }}` AS FLOAT64)) as avg_val
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
          COUNTIF(`{{ col_name }}` IS NULL) as null_count,
          COUNT(DISTINCT `{{ col_name }}`) as distinct_count,
          MIN(`{{ col_name }}`) as min_val,
          MAX(`{{ col_name }}`) as max_val
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
          COUNTIF(`{{ col_name }}` IS NULL) as null_count,
          COUNT(DISTINCT `{{ col_name }}`) as distinct_count
        FROM {{ full_table }}
      {% endset %}

      {% set stats = run_query(stats_query) %}

      {# Get sample values separately using APPROX_TOP_COUNT #}
      {% set sample_query %}
        SELECT value
        FROM UNNEST(
          (SELECT APPROX_TOP_COUNT(`{{ col_name }}`, {{ max_sample_values }}) FROM {{ full_table }})
        )
        WHERE value IS NOT NULL
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
