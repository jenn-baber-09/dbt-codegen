{#
  📚 add_model_column_descriptions_to_dict
  Extracts column descriptions from a model/source and adds them to a dictionary
  
  Args: resource_type, model_name, dict_with_descriptions (optional)
  Returns: Dictionary with column_name -> description mappings
#}
{% macro add_model_column_descriptions_to_dict(resource_type, model_name, dict_with_descriptions={}) %}
    
    {# 🔧 Debug: Log macro start #}
    {%- if execute %}
        {% do log("🚀 [COL_DESC] Adding column descriptions", info=true) %}
        {% do log("   📌 Resource type: " ~ resource_type ~ " | Model: " ~ model_name, info=true) %}
    {%- endif %}
    
    {# 🏢 Select appropriate source based on resource type #}
    {# Sources have a different location in dbt graph (graph.sources vs graph.nodes) #}
    {% if resource_type == 'source' %}
        {# Sources aren't part of graph.nodes - use separate sources collection #}
        {% set nodes = graph.sources %}
        {# 🔧 Debug: Using sources #}
        {%- if execute %}
            {% do log("   ✓ Using graph.sources", info=true) %}
        {%- endif %}
    {% else %}
        {# Models, tests, etc. are in graph.nodes #}
        {% set nodes = graph.nodes %}
        {# 🔧 Debug: Using nodes #}
        {%- if execute %}
            {% do log("   ✓ Using graph.nodes", info=true) %}
        {%- endif %}
    {% endif %}
    
    {# 🔍 Filter nodes: find exact matches for this resource type and name #}
    {% set matching_count = 0 %}
    {% for node in nodes.values()
        | selectattr('resource_type', 'equalto', resource_type)
        | selectattr('name', 'equalto', model_name) %}
        
        {# 🔧 Debug: Found matching node #}
        {%- if execute %}
            {% do log("   ✓ Found matching node", info=true) %}
        {%- endif %}
        
        {# 📋 Loop through all columns in this node #}
        {% set col_count = 0 %}
        {% for col_name, col_values in node.columns.items() %}
            {# 📝 Add column description to dictionary #}
            {% do dict_with_descriptions.update( {col_name: col_values.description} ) %}
            {% set col_count = col_count + 1 %}
        {% endfor %}
        
        {# 🔧 Debug: Columns processed #}
        {%- if execute %}
            {% do log("      ✓ Processed " ~ col_count ~ " column(s)", info=true) %}
        {%- endif %}
        
        {% set matching_count = matching_count + 1 %}
    {% endfor %}
    
    {# 🔧 Debug: Return final dictionary #}
    {%- if execute %}
        {% do log("   ✅ Dictionary has " ~ dict_with_descriptions | length ~ " total entries | returning", info=true) %}
    {%- endif %}
    
    {{ return(dict_with_descriptions) }}
{% endmacro %}
