{# 
  📋 generate_base_model
  Creates base model SQL from a source table with optional formatting & config
  
  Args: source_name, table_name, leading_commas, case_sensitive_cols, materialized
#}

{% macro generate_base_model(source_name, table_name, leading_commas=False, case_sensitive_cols=False, materialized=None) %}
  {# Route to adapter-specific implementation #}
  {{ return(adapter.dispatch('generate_base_model', 'codegen')(source_name, table_name, leading_commas, case_sensitive_cols, materialized)) }}

{% endmacro %}

{# ⚙️ Default implementation for all adapters #}
{% macro default__generate_base_model(source_name, table_name, leading_commas, case_sensitive_cols, materialized) %}

    {# 🔧 Log: macro parameters #}
    {% if execute %}
        {% do log("[DEBUG] generate_base_model - Starting with parameters:", info=true) %}
        {% do log("  source_name: " ~ source_name, info=true) %}
        {% do log("  table_name: " ~ table_name, info=true) %}
        {% do log("  leading_commas: " ~ leading_commas, info=true) %}
        {% do log("  case_sensitive_cols: " ~ case_sensitive_cols, info=true) %}
        {% do log("  materialized: " ~ materialized, info=true) %}
    {% endif %}

    {# 📍 Get source table & retrieve all columns #}
    {%- set source_relation = source(source_name, table_name) -%}
    
    {# 🔧 Log: source relation details #}
    {% if execute %}
        {% do log("[DEBUG] Source relation retrieved: " ~ source_relation.name, info=true) %}
    {% endif %}
    
    {# 📊 Extract column names only #}
    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}
    {% set column_names=columns | map(attribute='name') %}
    
    {# 🔧 Log: column details #}
    {% if execute %}
        {% do log("[DEBUG] Total columns found: " ~ columns | length, info=true) %}
        {% do log("[DEBUG] Column names: [" ~ column_names | join(", ") ~ "]", info=true) %}
    {% endif %}
    {# 📝 Build SQL template #}
    {% set base_model_sql %}

        {# ✅ Add config if materialized type specified #}
        {%- if materialized is not none -%}
            {{ "{{ config(materialized='" ~ materialized ~ "') }}" }}
        {%- endif %}

        {# 1️⃣ SELECT all from source #}
        with source as (

            select * 
            from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}

        ),

        {# 2️⃣ Rename/format columns (lowercase or quoted) #}
        renamed as (

            select
                {%- if leading_commas -%}
                {# Format: comma before column (except first) #}
                    {%- for column in column_names %}
                        {{" , " if not loop.first}}{%- if not case_sensitive_cols -%}{{ column | lower }}{%- else -%}{{ adapter.quote(column) }}{%- endif %}
                    {%- endfor %}
                {%- else -%}
                {# Format: comma after column (except last) #}
                    {%- for column in column_names %}
                        {%- if not case_sensitive_cols -%}{{ column | lower }}{%- else -%}{{ adapter.quote(column) }}{%- endif -%}{{"," if not loop.last}}
                    {%- endfor -%}
                {%- endif %}

            from source

        )

        {# 3️⃣ Return final result #}
        select * 
        from renamed

    {% endset %}

    {# 🏃 Execute block: only runs during dbt run (not parse) #}
    {% if execute %}

        {# 🔧 Log: generation complete & return #}
        {% do log("[DEBUG] ✅ SQL generated | leading_commas=" ~ leading_commas ~ " | case_sensitive=" ~ case_sensitive_cols, info=true) %}
        {% do log("[DEBUG] 📋 Preview: " ~ base_model_sql[0:200] ~ "...", info=true) %}
        
        {{ print(base_model_sql) }}
        
        {% do return(base_model_sql) %}

    {% endif %}
{% endmacro %}
