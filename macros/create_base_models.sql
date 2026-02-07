{# 🎯 Macro: create_base_models
   Purpose: Generates shell commands to create base models for multiple tables from a source
   This is a wrapper that dispatches to the database-specific implementation
#}
{% macro create_base_models(source_name, tables) %}
    {{ return(adapter.dispatch('create_base_models', 'codegen')(source_name, tables)) }}
{% endmacro %}

{# 📦 Default implementation for all supported adapters #}
{% macro default__create_base_models(source_name, tables) %}

    {# 🔧 Debug: Log macro invocation #}
    {% if execute %}
        {% do log("🚀 [CREATE_BASE_MODELS] Starting macro execution", info=true) %}
        {% do log("   📌 Source name: " ~ source_name, info=true) %}
        {% do log("   📌 Number of tables to process: " ~ tables | length, info=true) %}
    {% endif %}

    {# 🧹 Normalize source_name by wrapping in quotes #}
    {% set source_name = ""~ source_name ~"" %}
    
    {# 🔧 Debug: Log normalized source name #}
    {% if execute %}
        {% do log("   ✓ Source name normalized: " ~ source_name, info=true) %}
    {% endif %}

    {# 📝 Build the base shell command template for generating models #}
    {# This references the bash script that handles the actual model creation #}
    {% set zsh_command_models = "source dbt_packages/codegen/bash_scripts/base_model_creation.sh \""~ source_name ~"\" " %}
    
    {# 🔧 Debug: Log command template #}
    {% if execute %}
        {% do log("   ✓ Command template built", info=true) %}
    {% endif %}

    {# 📚 Initialize empty array to collect all commands #}
    {%- set models_array = [] -%}
    
    {# 🔧 Debug: Starting the loop #}
    {% if execute %}
        {% do log("   🔄 Building commands for each table...", info=true) %}
    {% endif %}

    {# 🔁 Loop through each table and construct its command #}
    {% for t in tables %}
        {# Append table name to the base command #}
        {% set help_command = zsh_command_models + t %}
        {{ models_array.append(help_command) }}
        
        {# 🔧 Debug: Log each table being processed #}
        {% if execute %}
            {% do log("      ✓ Added command for table: " ~ t, info=true) %}
        {% endif %}
    {% endfor %}
    
    {# 🔧 Debug: Log completion of command building #}
    {% if execute %}
        {% do log("   ✅ All " ~ models_array | length ~ " commands generated successfully", info=true) %}
    {% endif %}

    {# 📤 Output the shell commands that user should run #}
    {# These commands will create the SQL files for each table's base model #}
    {{ log("🎉 Run these commands in your shell to generate the models:\n" ~ models_array|join(' && \n'), info=True) }}

{% endmacro %}
