{% macro snowflake__validate_source(schema, table, database=none, output_format='text') %}

  {% set ns = namespace(source_found=false, has_documentation=false, source_info=none) %}

  {% for source in graph.sources.values() %}
    {% if source.schema == schema and source.name == table %}
      {% set ns.source_found = true %}
      {% set ns.source_info = source %}

      {% if source.description and source.description | trim | length > 0 %}
        {% set ns.has_documentation = true %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# Build result structure #}
  {% set result = {
    'schema': schema,
    'table': table,
    'source_declared': ns.source_found,
    'source_name': none,
    'has_documentation': false,
    'description': none
  } %}

  {% if ns.source_found %}
    {% do result.update({'source_name': ns.source_info.source_name}) %}
    {% if ns.has_documentation %}
      {% do result.update({
        'has_documentation': true,
        'description': ns.source_info.description
      }) %}
    {% endif %}
  {% endif %}

  {% if output_format == 'json' %}
    {{ log(tojson(result), info=True) }}
  {% else %}
    {{ log('=== Source Validation for ' + schema + '.' + table + ' ===', info=True) }}

    {% if ns.source_found %}
      {{ log('✓ Source declared: Yes', info=True) }}
      {{ log('  Source name: ' + ns.source_info.source_name, info=True) }}

      {% if ns.has_documentation %}
        {{ log('✓ Documentation: Yes', info=True) }}
        {{ log('  Description: ' + ns.source_info.description[:100] + ('...' if ns.source_info.description | length > 100 else ''), info=True) }}
      {% else %}
        {{ log('✗ Documentation: No', info=True) }}
      {% endif %}
    {% else %}
      {{ log('✗ Source declared: No', info=True) }}
      {{ log('  This table is not declared as a source in your dbt project', info=True) }}
    {% endif %}
  {% endif %}

{% endmacro %}

{% macro snowflake__validate_dataset_sources(schema, database=none, output_format='text') %}

  {# Use provided database or fall back to target database #}
  {% set db = database if database else target.database %}

  {% set query = "
    SELECT table_name
    FROM " + db + ".INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '" + schema + "'
      AND table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name
  " %}

  {% set results = run_query(query) %}

  {% set total_tables = results.rows | length %}
  {% set counters = namespace(declared_sources=0, documented_sources=0) %}

  {% set tables_list = [] %}

  {% for row in results %}
    {% set table_name = row[0] %}
    {% set ns = namespace(source_found=false, has_docs=false, source_name=none) %}

    {% for source in graph.sources.values() %}
      {% if source.schema == schema and source.name == table_name %}
        {% set ns.source_found = true %}
        {% set ns.source_name = source.source_name %}
        {% set counters.declared_sources = counters.declared_sources + 1 %}

        {% if source.description and source.description | trim | length > 0 %}
          {% set ns.has_docs = true %}
          {% set counters.documented_sources = counters.documented_sources + 1 %}
        {% endif %}
      {% endif %}
    {% endfor %}

    {% set table_result = {
      'name': table_name,
      'source_declared': ns.source_found,
      'source_name': ns.source_name,
      'has_documentation': ns.has_docs
    } %}

    {% if ns.source_found %}
      {% if ns.has_docs %}
        {% do table_result.update({'status': 'documented'}) %}
      {% else %}
        {% do table_result.update({'status': 'undocumented'}) %}
      {% endif %}
    {% else %}
      {% do table_result.update({'status': 'not_declared'}) %}
    {% endif %}

    {% do tables_list.append(table_result) %}

    {% if output_format != 'json' %}
      {% if not ns.source_found %}
        {{ log(table_name + ' - ✗ Not declared as source', info=True) }}
      {% elif not ns.has_docs %}
        {{ log(table_name + ' - ⚠ Declared but no documentation', info=True) }}
      {% else %}
        {{ log(table_name + ' - ✓ Declared and documented', info=True) }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# Build result structure #}
  {% set declared_pct = ((counters.declared_sources / total_tables * 100) | round(1)) if total_tables > 0 else 0 %}
  {% set documented_pct = ((counters.documented_sources / total_tables * 100) | round(1)) if total_tables > 0 else 0 %}

  {% set result = {
    'database': db,
    'schema': schema,
    'tables': tables_list,
    'summary': {
      'total_tables': total_tables,
      'declared_sources': counters.declared_sources,
      'declared_percentage': declared_pct,
      'documented_sources': counters.documented_sources,
      'documented_percentage': documented_pct
    }
  } %}

  {% if output_format == 'json' %}
    {{ log(tojson(result), info=True) }}
  {% else %}
    {{ log('=== Validating sources in schema: ' + schema + ' ===', info=True) }}
    {{ log('', info=True) }}
    {{ log('', info=True) }}
    {{ log('=== Summary ===', info=True) }}
    {{ log('Total tables: ' + total_tables | string, info=True) }}
    {{ log('Declared as sources: ' + counters.declared_sources | string + ' (' + (declared_pct | string) + '%)', info=True) }}
    {{ log('Documented sources: ' + counters.documented_sources | string + ' (' + (documented_pct | string) + '%)', info=True) }}
  {% endif %}

{% endmacro %}
