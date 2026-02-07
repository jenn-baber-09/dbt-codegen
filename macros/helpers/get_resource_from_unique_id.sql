{# ============================================================================
   📌 PURPOSE: Retrieve the complete resource dictionary from the dbt graph
              based on a unique_id
   
   This macro parses the unique_id to extract the resource type (first segment
   before the first dot), then routes the lookup to the appropriate graph
   location (graph.sources, graph.exposures, graph.metrics, or graph.nodes).
   
   The unique_id format is: "resource_type.project.package.name"
   Example: "source.my_project.salesforce.accounts" or
            "model.my_project.my_package.staging_customers"
   
   ARG resource_unique_id: The full unique_id of the resource (string)
   RETURNS: Complete resource dictionary with all metadata
   ============================================================================ #}

{% macro get_resource_from_unique_id(resource_unique_id) %}
    {%- if execute %}
        {% do log("🚀 [get_resource_from_unique_id] Looking up resource", info=true) %}
        {% do log("   📌 Resource unique_id: " ~ resource_unique_id, info=true) %}
    {%- endif %}

    {# 🔍 Parse resource type from first segment of unique_id (e.g., "source", "model", "exposure") #}
    {% set resource_type = resource_unique_id.split('.')[0] %}

    {%- if execute %}
        {% do log("   🏷️  Extracted resource type: " ~ resource_type, info=true) %}
    {%- endif %}

    {# 🔀 Route to appropriate graph location based on resource type #}
    {% if resource_type == 'source' %}
        {%- if execute %}
            {% do log("   📍 Routing to graph.sources", info=true) %}
        {%- endif %}
        {% set resource = graph.sources[resource_unique_id] %}

    {% elif resource_type == 'exposure' %}
        {%- if execute %}
            {% do log("   📍 Routing to graph.exposures", info=true) %}
        {%- endif %}
        {% set resource = graph.exposure[resource_unique_id] %}

    {% elif resource_type == 'metric' %}
        {%- if execute %}
            {% do log("   📍 Routing to graph.metrics", info=true) %}
        {%- endif %}
        {% set resource = graph.metrics[resource_unique_id] %}

    {% else %}
        {%- if execute %}
            {% do log("   📍 Routing to graph.nodes (model/seed/test)", info=true) %}
        {%- endif %}
        {% set resource = graph.nodes[resource_unique_id] %}
    {% endif %}

    {%- if execute %}
        {% do log("   ✓ Resource retrieved successfully", info=true) %}
        {% do log("   📤 Returning resource with name: " ~ resource.name, info=true) %}
    {%- endif %}

    {# 📤 Return the complete resource dictionary #}
    {{ return(resource) }}
{% endmacro %}
