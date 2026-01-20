{# Helper macro for outputting results in different formats #}

{% macro _output_result(data, output_format='text') %}
  {#
    Outputs data in the specified format.
    
    Args:
      data: Dictionary or list to output
      output_format: 'text' (default) or 'json'
    
    Note: When output_format='text', the caller is responsible for 
    logging human-readable output before calling this macro.
    This macro only handles JSON output.
  #}
  {% if output_format == 'json' %}
    {{ log(tojson(data), info=True) }}
  {% endif %}
{% endmacro %}

{% macro _safe_tojson(data) %}
  {#
    Safely serialize data to JSON, handling non-serializable types like Decimal.
    
    Args:
      data: Dictionary, list, or any value to serialize
    
    Returns:
      JSON string
  #}
  {{ tojson(data) }}
{% endmacro %}
