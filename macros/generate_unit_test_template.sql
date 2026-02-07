{#
  📋 generate_unit_test_template
  Creates dbt unit test YAML template for a model
  Handles dependencies, incremental models, inline/multiline column formats
  
  Args: model_name, inline_columns (bool for output format)
#}
{% macro generate_unit_test_template(model_name, inline_columns=false) %}
  {{ return(adapter.dispatch('generate_unit_test_template', 'codegen')(model_name, inline_columns)) }}
{% endmacro %}

{# ⚙️ Default implementation for all adapters #}
{% macro default__generate_unit_test_template(model_name, inline_columns=false) %}

    {# 🔧 Debug: Log start #}
    {%- if execute %}
        {% do log("🚀 [UNIT_TEST] Starting unit test template generation", info=true) %}
        {% do log("   📌 Model: " ~ model_name ~ " | Inline columns: " ~ inline_columns, info=true) %}
    {%- endif %}

    {# 📦 Initialize namespace for storing dependencies and metadata #}
    {%- set ns = namespace(depends_on_list = []) -%}

    {# 📍 Get model dependencies and materialization type #}
    {%- if execute -%}

    {%- for node in graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", model_name) -%}
        {%- set ns.depends_on_list = ns.depends_on_list + node.depends_on.nodes -%}
        {%- set ns.this_materialization = node.config['materialized'] -%}
    {%- endfor -%}
    
    {# 🔧 Debug: Dependencies and materialization found #}
    {% do log("   ✓ Found " ~ ns.depends_on_list | length ~ " dependencies", info=true) %}
    {% do log("   ✓ Materialization: " ~ ns.this_materialization, info=true) %}

    {%- endif -%}

    {# 📊 Extract columns from each input dependency #}
    {%- set ns.input_columns_list = [] -%}
    
    {# 🔧 Debug: Starting column extraction #}
    {%- if execute %}
        {% do log("   🔄 Extracting input columns...", info=true) %}
    {%- endif %}
    
    {%- for item in ns.depends_on_list -%}
        {%- set input_columns_list = [] -%}
        {%- set item_dict = codegen.get_resource_from_unique_id(item) -%}
        
        {# 🌿 Get columns from source or ref #}
        {%- if item_dict.resource_type == 'source' %}
            {%- set columns = adapter.get_columns_in_relation(source(item_dict.source_name, item_dict.identifier)) -%}
        {%- else -%}
            {%- set columns = adapter.get_columns_in_relation(ref(item_dict.alias)) -%}
        {%- endif -%}
        
        {# 📋 Collect column names #}
        {%- for column in columns -%}
            {{ input_columns_list.append(column.name|lower) }}
        {%- endfor -%}
        {{ ns.input_columns_list.append(input_columns_list) }}
        
        {# 🔧 Debug: Columns for this dependency #}
        {%- if execute %}
            {% do log("      ✓ " ~ item_dict.name ~ ": " ~ input_columns_list | length ~ " column(s)", info=true) %}
        {%- endif %}
    {%- endfor -%}

    {# 📄 Get expected output columns from the model #}
    {% set relation_exists = load_relation(ref(model_name)) is not none %}
    {% if relation_exists %}
        {%- set ns.expected_columns_list = [] -%}
        {%- set columns = adapter.get_columns_in_relation(ref(model_name)) -%}
        {%- for column in columns -%}
            {{ ns.expected_columns_list.append(column.name|lower) }}
        {%- endfor -%}
        
        {# 🔧 Debug: Expected columns logged #}
        {%- if execute %}
            {% do log("   ✓ Model " ~ model_name ~ " has " ~ ns.expected_columns_list | length ~ " column(s)", info=true) %}
        {%- endif %}
    {% endif %}

    {# 📝 Build unit test YAML template #}
    {# Sections: name, model, overrides (for incremental), given (inputs), expect (outputs) #}
    {%- set unit_test_yaml_template -%}
unit_tests:
  - name: unit_test_{{ model_name }}
    model: {{ model_name }}

    {# 🔧 Debug: Template building start #}
    {%- if execute %}
        {% do log("   🔨 Building YAML template...", info=true) %}
        {% do log("      Format: " ~ ("inline" if inline_columns else "multiline"), info=true) %}
    {%- endif %}

{% if ns.this_materialization == 'incremental' %}
    overrides:
      macros:
        is_incremental: true
{% else -%}

{%- endif %}
    {# 📦 Build given section: input tables and rows #}
    given: {%- if ns.depends_on_list|length == 0 and ns.this_materialization != 'incremental' %} []{%- endif %}
    {%- for i in range(ns.depends_on_list|length) -%}
        {%- set item_dict = codegen.get_resource_from_unique_id(ns.depends_on_list[i]) -%}
        {# 🔗 Reference: source or ref #}
        {% if item_dict.resource_type == 'source' %}
      - input: source("{{item_dict.source_name}}", "{{item_dict.identifier}}")
        rows:
        {%- else %}
      - input: ref("{{item_dict.alias}}")
        rows:
        {%- endif -%}
        {# 📊 Format columns inline or multiline #}
        {%- if inline_columns -%}
            {%- set ns.column_string = '- {' -%}
            {%- for column_name in ns.input_columns_list[i] -%}
                {%- if not loop.last -%}
                    {%- set ns.column_string = ns.column_string ~ column_name ~ ': , ' -%}
                {%- else -%}
                    {%- set ns.column_string = ns.column_string ~ column_name ~ ': }' -%}
                {%- endif -%}
            {% endfor %}
        {%- else -%}
            {%- set ns.column_string = '' -%}
            {%- for column_name in ns.input_columns_list[i] -%}
                {%- if loop.first -%}
                    {%- set ns.column_string = ns.column_string ~ '- ' ~ column_name ~ ': ' -%}
                {%- else -%}
                    {%- set ns.column_string = ns.column_string ~ '\n            ' ~ column_name ~ ': ' -%}
                {%- endif -%}
            {% endfor %}
        {%- endif %}
          {{ns.column_string}}
    {%- endfor %}

    {# 🔄 Special handling for incremental models: add 'this' input #}
    {%- if ns.this_materialization == 'incremental' %}
      - input: this
        rows:
        {%- if relation_exists -%}
            {%- if inline_columns -%}
                {%- set ns.column_string = '- {' -%}
                {%- for column_name in ns.expected_columns_list -%}
                    {%- if not loop.last -%}
                        {%- set ns.column_string = ns.column_string ~ column_name ~ ': , ' -%}
                    {%- else -%}
                        {%- set ns.column_string = ns.column_string ~ column_name ~ ': }' -%}
                    {%- endif -%}
                {% endfor %}
            {%- else -%}
                {%- set ns.column_string = '' -%}
                {%- for column_name in ns.expected_columns_list -%}
                    {%- if loop.first -%}
                        {%- set ns.column_string = ns.column_string ~ '- ' ~ column_name ~ ': ' -%}
                    {%- else -%}
                        {%- set ns.column_string = ns.column_string ~ '\n            ' ~ column_name ~ ': ' -%}
                    {%- endif -%}
                {% endfor %}
            {%- endif %}
          {{ns.column_string}}
        {%- endif %}
    {%- endif %}

    {# ⭐ Build expect section: expected output rows #}
    expect:
      rows:
        {%- if relation_exists -%}
            {%- if inline_columns -%}
                {%- set ns.column_string = '- {' -%}
                {%- for column_name in ns.expected_columns_list -%}
                    {%- if not loop.last -%}
                        {%- set ns.column_string = ns.column_string ~ column_name ~ ': , ' -%}
                    {%- else -%}
                        {%- set ns.column_string = ns.column_string ~ column_name ~ ': }' -%}
                    {%- endif -%}
                {% endfor %}
            {%- else -%}
                {%- set ns.column_string = '' -%}
                {%- for column_name in ns.expected_columns_list -%}
                    {%- if loop.first -%}
                        {%- set ns.column_string = ns.column_string ~ '- ' ~ column_name ~ ': ' -%}
                    {%- else -%}
                        {%- set ns.column_string = ns.column_string ~ '\n          ' ~ column_name ~ ': ' -%}
                    {%- endif -%}
                {% endfor %}
            {%- endif %}
        {{ns.column_string}}
    {%- endif -%}

    {%- endset -%}

    {# 🏃 Execute block: output and return the generated template #}
    {% if execute %}
        {# 🔧 Debug: Template generation complete #}
        {% do log("   ✅ Template generated | " ~ unit_test_yaml_template | length ~ " chars", info=true) %}
        {% do log("   ✅ Unit test template ready | returning output", info=true) %}

        {{ print(unit_test_yaml_template) }}
        {% do return(unit_test_yaml_template) %}

    {% endif %}

{% endmacro %}
