{#
  📚 build_dict_column_descriptions
  Builds a master dictionary of all column descriptions from a model's direct dependencies
  
  Loops through all upstream models/sources and collects column descriptions
  Note: If same column name exists in multiple parents, later description overwrites earlier
  
  Args: model_name
  Returns: Dictionary mapping column_name -> description
#}
{% macro build_dict_column_descriptions(model_name) %}
    {# 🔧 Debug: Log macro start #}
    {%- if execute %}
        {% do log("🚀 [BUILD_DICT] Building column descriptions dictionary", info=true) %}
        {% do log("   📌 Model: " ~ model_name, info=true) %}
    {%- endif %}
    
    {% if execute %}
        {# 📚 Initialize empty dictionary to collect all descriptions #}
        {% set glob_dict = {} %}
        
        {# 🔧 Debug: Starting dependency loop #}
        {% do log("   🔄 Getting upstream dependencies...", info=true) %}
        
        {# 🔗 Get all direct upstream models (parents) of this model #}
        {% set dependencies = codegen.get_model_dependencies(model_name) %}
        
        {# 🔧 Debug: Dependencies retrieved #}
        {% do log("      ✓ Found " ~ dependencies | length ~ " dependencies", info=true) %}
        
        {# 🔁 Loop through each parent model and extract column descriptions #}
        {% for full_model in dependencies %}
            {# 📍 Parse the dependency identifier (resource_type.package.project.name) #}
            {# Extract resource_type (first part) and name (last part) #}
            {% set resource_type = full_model.split('.')[0] %}
            {% set model_from_dep = full_model.split('.')[-1] %}
            
            {# 🔧 Debug: Processing dependency #}
            {% do log("      ⏳ Processing: " ~ resource_type ~ "." ~ model_from_dep, info=true) %}
            
            {# 📝 Add this dependency's column descriptions to the global dictionary #}
            {# Note: This modifies glob_dict in-place (passed by reference) #}
            {% do codegen.add_model_column_descriptions_to_dict(
                resource_type, model_from_dep, glob_dict
            ) %}
            
            {# 🔧 Debug: Dependency processed #}
            {% do log("         ✓ Dictionary now has " ~ glob_dict | length ~ " total entries", info=true) %}
        {% endfor %}
        
        {# 🔧 Debug: Complete and return #}
        {% do log("   ✅ Dictionary building complete | " ~ glob_dict | length ~ " entries | returning", info=true) %}
        
        {# 📤 Return the complete descriptions dictionary #}
        {{ return(glob_dict) }}
    {% endif %}
{% endmacro %}
