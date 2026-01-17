{% macro bigquery__list_databases(output_format='text') %}

  {#
    For BigQuery, a "database" is the project.

    NOTE: This macro only returns the current project from the dbt profile.
    BigQuery's INFORMATION_SCHEMA is project-scoped, so there's no SQL-based
    way to list all accessible projects without using the Cloud Resource Manager API.

    For exploring data sources within the current project, use list_database_schemas
    to see all available datasets.
  #}
  {% set result = {'databases': [target.project]} %}

  {% if output_format == 'json' %}
    {{ log(tojson(result), info=True) }}
  {% else %}
    {{ log(target.project, info=True) }}
  {% endif %}

{% endmacro %}

{% macro bigquery__list_database_schemas(database=none, exclude_schemas=[], output_format='text') %}

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

  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}

  {% if output_format == 'json' %}
    {{ log(tojson({'schemas': schemas}), info=True) }}
  {% else %}
    {% for schema in schemas %}
      {{ log(schema, info=True) }}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro bigquery__list_tables(schema, database=none, output_format='text') %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}

  {% set query = "

    SELECT table_name, table_type

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.TABLES`

  " %}

  {% set results = run_query(query) %}

  {% set tables = [] %}
  {% for row in results %}
    {% do tables.append({'name': row[0], 'type': row[1]}) %}
  {% endfor %}

  {% if output_format == 'json' %}
    {{ log(tojson({'tables': tables}), info=True) }}
  {% else %}
    {% for t in tables %}
      {{ log(t.name + ' (' + t.type + ')', info=True) }}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro bigquery__list_columns(schema, table, database=none, output_format='text') %}

  {# For BigQuery, database parameter is ignored since we always use target.project #}

  {% set query = "

    SELECT column_name, data_type, is_nullable

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.COLUMNS`

    WHERE table_name = '" + table + "'

    ORDER BY ordinal_position

  " %}

  {% set results = run_query(query) %}

  {% set columns = [] %}
  {% for row in results %}
    {% do columns.append({'name': row[0], 'data_type': row[1], 'is_nullable': row[2]}) %}
  {% endfor %}

  {% if output_format == 'json' %}
    {{ log(tojson({'columns': columns}), info=True) }}
  {% else %}
    {% for col in columns %}
      {{ log(col.name + ': ' + col.data_type + ' (' + col.is_nullable + ')', info=True) }}
    {% endfor %}
  {% endif %}

{% endmacro %}

{% macro bigquery__profile_table(schema, table, database=none, output_format='text') %}

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

  {% set result = {
    'schema': schema,
    'table': table,
    'row_count': none,
    'size_bytes': none,
    'last_modified': none,
    'partitioned': none,
    'clustered': none,
    'preview': []
  } %}

  {% if metadata_results and metadata_results.rows | length > 0 %}
    {% do result.update({'row_count': metadata_results[0][1]}) %}
    {% do result.update({'size_bytes': metadata_results[0][2]}) %}
    {% do result.update({'last_modified': metadata_results[0][3] | string}) %}
  {% endif %}

  {% set preview_query = 'SELECT * FROM ' + full_table + ' LIMIT ' + max_rows | string %}
  {% set preview_results = run_query(preview_query) %}

  {% set preview_rows = [] %}
  {% set column_names = preview_results.column_names | list if preview_results else [] %}
  {% for row in preview_results %}
    {% set row_dict = {} %}
    {% for i in range(column_names | length) %}
      {% do row_dict.update({column_names[i]: row[i] | string}) %}
    {% endfor %}
    {% do preview_rows.append(row_dict) %}
  {% endfor %}
  {% do result.update({'preview': preview_rows}) %}

  {% set ddl_query = "

    SELECT ddl

    FROM `" + target.project + "." + schema + ".INFORMATION_SCHEMA.TABLES`

    WHERE table_name = '" + table + "'

  " %}

  {% set ddl_results = run_query(ddl_query) %}

  {% if ddl_results and ddl_results.rows | length > 0 and ddl_results[0][0] %}
    {% set ddl = ddl_results[0][0] %}
    {% do result.update({'partitioned': 'PARTITION BY' in ddl}) %}
    {% do result.update({'clustered': 'CLUSTER BY' in ddl}) %}
  {% endif %}

  {% if output_format == 'json' %}
    {{ log(tojson(result), info=True) }}
  {% else %}
    {% if result.row_count is not none %}
      {{ log('Row count: ' + (result.row_count | string), info=True) }}
    {% endif %}
    {% if result.size_bytes is not none %}
      {{ log('Size (bytes): ' + (result.size_bytes | string), info=True) }}
    {% endif %}
    {% if result.last_modified is not none %}
      {{ log('Last modified: ' + result.last_modified, info=True) }}
    {% endif %}
    {{ log('Preview (first ' + max_rows | string + ' rows):', info=True) }}
    {% for row in preview_results %}
      {{ log(row | string, info=True) }}
    {% endfor %}
    {% if result.partitioned is not none %}
      {{ log('Partitioned: ' + ('Yes' if result.partitioned else 'No'), info=True) }}
    {% endif %}
    {% if result.clustered is not none %}
      {{ log('Clustered: ' + ('Yes' if result.clustered else 'No'), info=True) }}
    {% endif %}
  {% endif %}

{% endmacro %}

{% macro bigquery__profile_columns(schema, table, database=none, output_format='text') %}

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
    {% if output_format == 'json' %}
      {{ log(tojson({'error': 'No columns found', 'schema': schema, 'table': table}), info=True) }}
    {% else %}
      {{ log('No columns found for table ' + schema + '.' + table, info=True) }}
    {% endif %}
    {{ return('') }}
  {% endif %}

  {# Get row count first #}
  {% set count_query = 'SELECT COUNT(*) as cnt FROM ' + full_table %}
  {% set count_result = run_query(count_query) %}
  {% set total_rows = count_result[0][0] if count_result and count_result.rows | length > 0 else 0 %}

  {# Build result structure #}
  {% set result = {
    'schema': schema,
    'table': table,
    'total_rows': total_rows,
    'columns': [],
    'summary': {
      'columns_analyzed': columns.rows | length,
      'columns_with_nulls': 0
    }
  } %}

  {% if total_rows == 0 %}
    {% if output_format == 'json' %}
      {% do result.update({'message': 'Table is empty - no statistics to compute'}) %}
      {{ log(tojson(result), info=True) }}
    {% else %}
      {{ log('', info=True) }}
      {{ log('=== Column Profile for ' + schema + '.' + table + ' ===', info=True) }}
      {{ log('', info=True) }}
      {{ log('Total rows: ' + (total_rows | string), info=True) }}
      {{ log('', info=True) }}
      {{ log('Table is empty - no statistics to compute', info=True) }}
    {% endif %}
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
  {% set column_results = [] %}

  {# Process each column individually to avoid query size limits #}
  {% for col in columns %}
    {% set col_name = col[0] %}
    {% set col_type = col[1] | upper %}
    {% set col_nullable = col[2] %}

    {# Determine base type (handle parameterized types like NUMERIC(10,2)) #}
    {% set base_type = col_type.split('(')[0].split('<')[0] %}

    {% set col_result = {
      'name': col_name,
      'data_type': col_type,
      'is_nullable': col_nullable == 'YES'
    } %}

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
        {% do col_result.update({'null_count': null_count, 'null_percentage': null_pct, 'type_category': 'complex'}) %}
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

        {% do col_result.update({
          'null_count': null_count,
          'null_percentage': null_pct,
          'distinct_count': distinct_count,
          'true_count': true_count,
          'true_percentage': true_pct,
          'false_count': false_count,
          'false_percentage': false_pct,
          'type_category': 'boolean'
        }) %}
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

        {% do col_result.update({
          'null_count': null_count,
          'null_percentage': null_pct,
          'distinct_count': distinct_count,
          'min': min_val,
          'max': max_val,
          'avg': avg_val | round(2) if avg_val is not none else none,
          'type_category': 'numeric'
        }) %}
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

        {% do col_result.update({
          'null_count': null_count,
          'null_percentage': null_pct,
          'distinct_count': distinct_count,
          'min': min_val | string if min_val is not none else none,
          'max': max_val | string if max_val is not none else none,
          'type_category': 'datetime'
        }) %}
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

        {% set sample_list = [] %}
        {% if samples and samples.rows | length > 0 %}
          {% for sample in samples %}
            {% if sample[0] is not none %}
              {% set sample_str = sample[0] | string %}
              {% if sample_str | length > 50 %}
                {% set sample_str = sample_str[:47] + '...' %}
              {% endif %}
              {% do sample_list.append(sample_str) %}
            {% endif %}
          {% endfor %}
        {% endif %}

        {% do col_result.update({
          'null_count': null_count,
          'null_percentage': null_pct,
          'distinct_count': distinct_count,
          'samples': sample_list,
          'type_category': 'string'
        }) %}
        {% if null_count > 0 %}
          {% set ns.columns_with_nulls = ns.columns_with_nulls + 1 %}
        {% endif %}
      {% endif %}

    {% endif %}

    {% do column_results.append(col_result) %}

    {# Output text format progressively #}
    {% if output_format != 'json' %}
      {{ log('', info=True) }}
      {{ log('Column: ' + col_name + ' (' + col_type + ', ' + ('NULLABLE' if col_nullable == 'YES' else 'NOT NULL') + ')', info=True) }}

      {% if col_result.null_count is defined %}
        {{ log('  Nulls: ' + (col_result.null_count | string) + ' (' + (col_result.null_percentage | string) + '%)', info=True) }}
      {% endif %}

      {% if col_result.type_category == 'complex' %}
        {{ log('  [Complex type - limited statistics]', info=True) }}
      {% elif col_result.type_category == 'boolean' %}
        {{ log('  Distinct: ' + (col_result.distinct_count | string), info=True) }}
        {{ log('  True: ' + (col_result.true_count | string) + ' (' + (col_result.true_percentage | string) + '%) | False: ' + (col_result.false_count | string) + ' (' + (col_result.false_percentage | string) + '%)', info=True) }}
      {% elif col_result.type_category == 'numeric' %}
        {{ log('  Distinct: ' + (col_result.distinct_count | string), info=True) }}
        {% if col_result.min is not none %}
          {% set avg_display = (col_result.avg | string) if col_result.avg is not none else 'N/A' %}
          {{ log('  Min: ' + (col_result.min | string) + ' | Max: ' + (col_result.max | string) + ' | Avg: ' + avg_display, info=True) }}
        {% endif %}
      {% elif col_result.type_category == 'datetime' %}
        {{ log('  Distinct: ' + (col_result.distinct_count | string), info=True) }}
        {% if col_result.min is not none %}
          {{ log('  Min: ' + col_result.min + ' | Max: ' + col_result.max, info=True) }}
        {% endif %}
      {% elif col_result.type_category == 'string' %}
        {{ log('  Distinct: ' + (col_result.distinct_count | string), info=True) }}
        {% if col_result.samples | length > 0 %}
          {% set quoted_samples = [] %}
          {% for s in col_result.samples %}
            {% do quoted_samples.append("'" + s + "'") %}
          {% endfor %}
          {{ log('  Samples: [' + quoted_samples | join(', ') + ']', info=True) }}
        {% endif %}
      {% endif %}
    {% endif %}

  {% endfor %}

  {# Update result with columns and summary #}
  {% do result.update({'columns': column_results}) %}
  {% do result['summary'].update({'columns_with_nulls': ns.columns_with_nulls}) %}

  {% if output_format == 'json' %}
    {{ log(tojson(result), info=True) }}
  {% else %}
    {# Summary in text format #}
    {{ log('', info=True) }}
    {{ log('=== Column Profile for ' + schema + '.' + table + ' ===', info=True) }}
    {{ log('', info=True) }}
    {{ log('Total rows: ' + (total_rows | string), info=True) }}
    {{ log('', info=True) }}
    {{ log('=== Summary ===', info=True) }}
    {{ log('Columns analyzed: ' + (columns.rows | length | string), info=True) }}
    {{ log('Columns with nulls: ' + (ns.columns_with_nulls | string), info=True) }}
  {% endif %}

{% endmacro %}

{% macro bigquery__execute_raw_query(query, output_format='text') %}

  {# Execute the raw query #}
  {% set results = run_query(query) %}

  {# Build result structure #}
  {% set result = {
    'query': query,
    'success': true,
    'row_count': 0,
    'column_count': 0,
    'columns': [],
    'rows': []
  } %}

  {# Display results #}
  {% if results %}
    {% set row_count = results.rows | length %}
    {% set col_count = results.column_names | length %}
    {% set column_names = results.column_names | list %}

    {% do result.update({'row_count': row_count, 'column_count': col_count, 'columns': column_names}) %}

    {% if row_count > 0 %}
      {% set row_list = [] %}
      {% for row in results %}
        {% set row_dict = {} %}
        {% for i in range(column_names | length) %}
          {% do row_dict.update({column_names[i]: row[i] | string}) %}
        {% endfor %}
        {% do row_list.append(row_dict) %}
      {% endfor %}
      {% do result.update({'rows': row_list}) %}
    {% endif %}

    {% if output_format == 'json' %}
      {{ log(tojson(result), info=True) }}
    {% else %}
      {{ log('', info=True) }}
      {{ log('Query executed successfully', info=True) }}
      {{ log('Rows returned: ' + (row_count | string), info=True) }}
      {{ log('Columns: ' + (col_count | string), info=True) }}
      {{ log('', info=True) }}

      {% if row_count > 0 %}
        {# Display column headers #}
        {{ log(column_names | join(' | '), info=True) }}
        {{ log('-' * 80, info=True) }}

        {# Display rows #}
        {% for row in results %}
          {{ log(row | string, info=True) }}
        {% endfor %}
      {% else %}
        {{ log('No rows returned', info=True) }}
      {% endif %}
    {% endif %}

  {% else %}
    {% do result.update({'success': false, 'message': 'Query executed but returned no results'}) %}
    {% if output_format == 'json' %}
      {{ log(tojson(result), info=True) }}
    {% else %}
      {{ log('Query executed but returned no results', info=True) }}
    {% endif %}
  {% endif %}

{% endmacro %}
