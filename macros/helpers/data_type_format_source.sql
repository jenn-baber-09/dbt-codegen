{#
  📊 data_type_format_source
  Dispatcher macro for formatting a column's data type for source definitions
  Routes to adapter-specific implementations via dispatch
  
  Args: column object
  Returns: Formatted data type string (lowercase)
#}
{% macro data_type_format_source(column) -%}
  {# 🔧 Debug: Log dispatch #}
  {%- if execute %}
    {% do log("🚀 [DATA_TYPE_SOURCE] Formatting column data type", info=true) %}
    {% do log("   📌 Column: " ~ column.name, info=true) %}
  {%- endif %}
  
  {# 🔀 Dispatch to adapter-specific implementation #}
  {{ return(adapter.dispatch('data_type_format_source', 'codegen')(column)) }}
{%- endmacro %}

{# 
  📝 default__data_type_format_source
  Default implementation: Extracts and formats column data type for sources
  Converts data type to lowercase for consistency
  
  Args: column object
  Returns: Lowercase data type string
#}
{% macro default__data_type_format_source(column) %}
    {# 🔧 Debug: Start formatting #}
    {%- if execute %}
        {% do log("   ⏳ Extracting data type from source column: " ~ column.name, info=true) %}
    {%- endif %}
    
    {# 📉 Format the column (extracts data_type and other metadata) #}
    {% set formatted = codegen.format_column(column) %}
    
    {# 🔧 Debug: Data type extracted #}
    {%- if execute %}
        {% do log("      ✓ Data type: " ~ formatted['data_type'], info=true) %}
    {%- endif %}
    
    {# 🔤 Convert to lowercase for consistency and return #}
    {% set result = formatted['data_type'] | lower %}
    
    {# 🔧 Debug: Return formatted result #}
    {%- if execute %}
        {% do log("      ✓ Formatted (lowercase): " ~ result, info=true) %}
    {%- endif %}
    
    {{ return(result) }}
{% endmacro %}
