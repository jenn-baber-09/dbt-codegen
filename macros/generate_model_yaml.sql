{#
  📋 generate_column_yaml
  Recursively builds YAML for a column, handling nested struct fields
  
  Args: column, model_yaml, column_desc_dict, include_data_types, parent_column_name
#}
{% macro generate_column_yaml(column, model_yaml, column_desc_dict, include_data_types, parent_column_name="", materialized=None) %}
  {{ return(adapter.dispatch('generate_column_yaml', 'codegen')(column, model_yaml, column_desc_dict, include_data_types, parent_column_name, materialized)) }}
{% endmacro %}

{# ⚙️ Default implementation for all adapters #}
{% macro default__generate_column_yaml(column, model_yaml, column_desc_dict, include_data_types, parent_column_name, materialized) %}
    {# 🏗️ Build full column name (parent.child for nested fields) #}
    {% if parent_column_name %}
        {% set column_name = parent_column_name ~ "." ~ column.name %}
    {% else %}
        {% set column_name = column.name %}
    {% endif %}

    {# 📝 Add column name to YAML #}
    {% do model_yaml.append('      - name: ' ~ column_name  | lower ) %}
    
    {# 📊 Add data type if requested #}
    {% if include_data_types %}
        {% do model_yaml.append('        data_type: ' ~ codegen.data_type_format_model(column)) %}
    {% endif %}
    
    
    {# 📄 Add column description from upstream or empty #}
    {% do model_yaml.append('        description: ' ~ (column_desc_dict.get(column.name | lower,'') | tojson)) %}

    {# 🧪 Add generic boolean tests to fill in based on datatype #}
    {% if codegen.data_type_format_model(column) | lower == 'boolean' %}
        {% do model_yaml.append('        data_tests:') %}
        {% do model_yaml.append('          - not_null') %}
        {% do model_yaml.append('          - accepted_values:') %}
        {% do model_yaml.append('              config:') %}
        {% do model_yaml.append('                  arguments:') %}
        {% do model_yaml.append('                      values: [true, false]') %}
        {% do model_yaml.append('                      quote: false') %}
    {% endif %}

    

    {% do model_yaml.append('') %}

    {# 🔄 Recursively process nested struct fields #}
    {% if column.fields|length > 0 %}
        {% for child_column in column.fields %}
            {% set model_yaml = codegen.generate_column_yaml(child_column, model_yaml, column_desc_dict, include_data_types, parent_column_name=column_name) %}
        {% endfor %}
    {% endif %}
    
    {% do return(model_yaml) %}
{% endmacro %}


{#
  📚 generate_model_yaml
  Main macro that orchestrates YAML generation for one or more dbt models
  Includes column descriptions, data types, and nested struct support
  
  Args: model_names (list), upstream_descriptions (bool), include_data_types (bool), materialized (string)
#}
{% macro generate_model_yaml(model_names=[], upstream_descriptions=False, include_data_types=True, materialized=None) -%}
  {{ return(adapter.dispatch('generate_model_yaml', 'codegen')(model_names, upstream_descriptions, include_data_types, materialized)) }}
{%- endmacro %}

{# ⚙️ Default implementation for all adapters #}
{% macro default__generate_model_yaml(model_names, upstream_descriptions, include_data_types, materialized) %}

    {# 🔧 Debug: Log macro inputs #}
    {%- if execute %}
        {% do log("🚀 [MODEL_YAML] Starting YAML generation", info=true) %}
        {% do log("   📌 Models: " ~ model_names | join(", "), info=true) %}
        {% do log("   📌 Upstream descriptions: " ~ upstream_descriptions, info=true) %}
        {% do log("   📌 Include data types: " ~ include_data_types, info=true) %}
    {%- endif %}

    {# 📋 Initialize YAML structure with version and models header #}
    {% set model_yaml=[] %}

    {% do model_yaml.append('version: 2') %}
    {% do model_yaml.append('') %}
    {% do model_yaml.append('models:') %}

    {# ✅ Validate model_names is a list #}
    {% if model_names is string %}
        {{ exceptions.raise_compiler_error("The `model_names` argument must always be a list, even if there is only one model.") }}
    {% else %}
        {# 🔁 Loop: Process each model #}
        {% for model in model_names %}
            {# 🔧 Debug: Processing model #}
            {%- if execute %}
                {% do log("   ⏳ Processing model: " ~ model, info=true) %}
            {%- endif %}
            
            {# 📝 Add model header to YAML #}            
            {% do model_yaml.append('  - name: ' ~ model | lower) %}
            {% do model_yaml.append('    description: ""') %}

            {# 🔐 View, table, and incremental materializations get contract enforcement config #}
            {% if materialized | lower is in ['table', 'view', 'incremental'] %}
                {{ model_yaml.append('    config:') }}
                {{ model_yaml.append('        contract:') }}
                {{ model_yaml.append('            enforced: true') }}
            {% endif %}
            {% do model_yaml.append('    columns:') %}

            {# 📍 Get model relation and columns #}
            {% set relation=ref(model) %}
            {%- set columns = adapter.get_columns_in_relation(relation) -%}
            
            {# 🔧 Debug: Column count #}
            {%- if execute %}
                {% do log("      ✓ Found " ~ columns | length ~ " column(s)", info=true) %}
            {%- endif %}
            
            {# 📊 Build column descriptions from upstream models if enabled #}
            {% set column_desc_dict =  codegen.build_dict_column_descriptions(model) if upstream_descriptions else {} %}

            {# 🔄 Loop: Generate YAML for each column #}
            {% for column in columns %}
                {% set model_yaml = codegen.generate_column_yaml(column, model_yaml, column_desc_dict, include_data_types) %}
            {% endfor %}
            
            {# 🔧 Debug: Model complete #}
            {%- if execute %}
                {% do log("      ✅ Model YAML built", info=true) %}
            {%- endif %}
        {% endfor %}
    {% endif %}

{# 🏃 Execute block: only runs during dbt execution #}
{% if execute %}
    {# 🔧 Debug: Generation complete #}
    {% do log("   ✅ All models processed | joining YAML...", info=true) %}

    {# 📤 Join YAML lines and output #}
    {% set joined = model_yaml | join ('\n') %}
    {{ print(joined) }}
    
    {# 🔧 Debug: Return final output #}
    {% do log("   ✅ YAML generation complete | returning output", info=true) %}
    {% do return(joined) %}

{% endif %}

{% endmacro %}