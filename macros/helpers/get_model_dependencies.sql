{# ============================================================================
   📌 PURPOSE: Retrieve direct upstream dependencies for a given model
   
   This macro queries the dbt graph to find all nodes (models/sources) that the
   specified model directly depends on. It searches through graph.nodes, finds
   the matching model by name, and returns its depends_on.nodes list.
   
   ARG model_name: Name of the model to find dependencies for (string)
   RETURNS: List of upstream node unique_ids that this model depends on
   ============================================================================ #}

{% macro get_model_dependencies(model_name) %}
    {%- if execute %}
        {% do log("🚀 [get_model_dependencies] Starting dependency lookup", info=true) %}
        {% do log("   📌 Looking for dependencies of model: " ~ model_name, info=true) %}
    {%- endif %}

    {# 🔍 Search through graph nodes to find the matching model by name #}
    {% for node in graph.nodes.values() | selectattr('name', "equalto", model_name) %}
        {%- if execute %}
            {% do log("   ✓ Found model: " ~ node.name ~ " with unique_id: " ~ node.unique_id, info=true) %}
            {% do log("   ✓ Dependencies count: " ~ (node.depends_on.nodes | length), info=true) %}
        {%- endif %}

        {# 📤 Return the list of upstream node unique_ids #}
        {{ return(node.depends_on.nodes) }}
    {% endfor %}

    {%- if execute %}
        {% do log("   ⚠️  Model '" ~ model_name ~ "' not found in graph", info=true) %}
    {%- endif %}
{% endmacro %}
