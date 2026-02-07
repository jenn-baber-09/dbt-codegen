{# ============================================================================
   📌 PURPOSE: Get all models in the project, optionally filtered by directory
              and/or name prefix
   
   This macro retrieves model names from the dbt graph and filters them based on
   optional directory path and/or name prefix. It handles four scenarios:
   1. Both directory AND prefix provided: Return models matching both criteria
   2. Only directory provided: Return all models in that directory
   3. Only prefix provided: Return all models starting with that prefix
   4. No filters: Return all models in the project
   
   ARG directory: Optional directory path to filter by (e.g., "models/marts")
   ARG prefix: Optional name prefix to filter by (e.g., "fact_" or "dim_")
   RETURNS: List of model names matching filter criteria
   ============================================================================ #}

{% macro get_models(directory=None, prefix=None) %}
    {%- if execute %}
        {% do log("🚀 [get_models] Starting model discovery", info=true) %}
        {% if directory %}
            {% do log("   📁 Filtering by directory: " ~ directory, info=true) %}
        {% endif %}
        {% if prefix %}
            {% do log("   📝 Filtering by prefix: " ~ prefix, info=true) %}
        {% endif %}
    {%- endif %}

    {# 📦 Initialize empty list and get all models from the graph #}
    {% set model_names=[] %}
    {% set models = graph.nodes.values() | selectattr('resource_type', "equalto", 'model') %}

    {%- if execute %}
        {% do log("   📊 Total models in project: " ~ (models | list | length), info=true) %}
    {%- endif %}

    {# 🔍 Filter by BOTH directory AND prefix (most specific filter) #}
    {% if directory and prefix %}
        {%- if execute %}
            {% do log("   🔀 Applying BOTH directory and prefix filters", info=true) %}
        {%- endif %}
        {% for model in models %}
            {% set model_path = "/".join(model.path.split("/")[:-1]) %}
            {% if model_path == directory and model.name.startswith(prefix) %}
                {% do model_names.append(model.name) %}
            {% endif %}
        {% endfor %}

    {# 🔍 Filter by DIRECTORY ONLY #}
    {% elif directory %}
        {%- if execute %}
            {% do log("   🔀 Applying directory filter only", info=true) %}
        {%- endif %}
        {% for model in models %}
            {% set model_path = "/".join(model.path.split("/")[:-1]) %}
            {% if model_path == directory %}
                {% do model_names.append(model.name) %}
            {% endif %}
        {% endfor %}

    {# 🔍 Filter by PREFIX ONLY #}
    {% elif prefix %}
        {%- if execute %}
            {% do log("   🔀 Applying prefix filter only", info=true) %}
        {%- endif %}
        {% for model in models if model.name.startswith(prefix) %}
            {% do model_names.append(model.name) %}
        {% endfor %}

    {# 🔍 NO FILTERS: Return all models #}
    {% else %}
        {%- if execute %}
            {% do log("   🔀 No filters applied, returning all models", info=true) %}
        {%- endif %}
        {% for model in models %}
            {% do model_names.append(model.name) %}
        {% endfor %}
    {% endif %}

    {%- if execute %}
        {% do log("   ✓ Models matching filters: " ~ (model_names | length), info=true) %}
        {% do log("   📤 Returning " ~ (model_names | length) ~ " model(s): " ~ (model_names | join(", ")), info=true) %}
    {%- endif %}

    {# 📤 Return the filtered list of model names #}
    {{ return(model_names) }}
{% endmacro %}
