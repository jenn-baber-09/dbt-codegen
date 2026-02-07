{#
  🔍 get_tables_in_schema
  Retrieves list of tables from a schema, optionally filtered by pattern/exclude
  
  Args: schema_name, database_name, table_pattern, exclude
#}
{% macro get_tables_in_schema(schema_name, database_name=target.database, table_pattern='%', exclude='') %}

    {# 📍 Get all relations matching the pattern #}
    {% set tables=dbt_utils.get_relations_by_pattern(
        schema_pattern=schema_name,
        database=database_name,
        table_pattern=table_pattern,
        exclude=exclude
    ) %}

    {# 📋 Extract just the table identifiers #}
    {% set table_list= tables | map(attribute='identifier') %}

    {# ✅ Return sorted list #}
    {{ return(table_list | sort) }}

{% endmacro %}


{#
  📚 generate_source
  Creates dbt source definition YAML with optional columns, descriptions & data types
  Supports filtering by table pattern, case sensitivity options, and more
  
  Args: schema_name, database_name, generate_columns, include_descriptions, include_data_types, 
        table_pattern, exclude, name, table_names, include_database, include_schema, 
        case_sensitive_* options
#}
{% macro generate_source(schema_name, database_name=target.database, generate_columns=False, include_descriptions=False, include_data_types=True, table_pattern='%', exclude='', name=schema_name, table_names=None, include_database=False, include_schema=False, case_sensitive_databases=False, case_sensitive_schemas=False, case_sensitive_tables=False, case_sensitive_cols=False) %}
    {{ return(adapter.dispatch('generate_source', 'codegen')(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols)) }}
{% endmacro %}

{# ⚙️ Default implementation for all adapters #}
{% macro default__generate_source(schema_name, database_name, generate_columns, include_descriptions, include_data_types, table_pattern, exclude, name, table_names, include_database, include_schema, case_sensitive_databases, case_sensitive_schemas, case_sensitive_tables, case_sensitive_cols) %}

{# 🔧 Debug: Log macro inputs #}
{%- if execute %}
    {% do log("🚀 [GENERATE_SOURCE] Starting source YAML generation", info=true) %}
    {% do log("   📌 Schema: " ~ schema_name ~ " | Database: " ~ database_name, info=true) %}
    {% do log("   📌 Columns: " ~ generate_columns ~ " | Descriptions: " ~ include_descriptions ~ " | Data types: " ~ include_data_types, info=true) %}
{%- endif %}

{# 📋 Initialize YAML structure with version and source header #}
{% set sources_yaml=[] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ name | lower) %}

{# 📄 Add description block if requested #}
{% if include_descriptions %}
    {% do sources_yaml.append('    description: ""' ) %}
{% endif %}

{# 🏢 Add database reference if not default target or explicitly requested #}
{% if database_name != target.database or include_database %}
{% do sources_yaml.append('    database: ' ~ (database_name if case_sensitive_databases else database_name | lower)) %}
{% endif %}

{# 📍 Add schema reference if not same as source name or explicitly requested #}
{% if schema_name != name or include_schema %}
{% do sources_yaml.append('    schema: ' ~ (schema_name if case_sensitive_schemas else schema_name | lower)) %}
{% endif %}

{# 📋 Add tables header #}
{% do sources_yaml.append('    tables:') %}

{# 🔍 Get list of tables (either provided or discover from schema) #}
{% if table_names is none %}
    {% set tables=codegen.get_tables_in_schema(schema_name, database_name, table_pattern, exclude) %}
    {# 🔧 Debug: Tables discovered #}
    {%- if execute %}
        {% do log("   ✓ Discovered " ~ tables | length ~ " table(s) in schema", info=true) %}
    {%- endif %}
{% else %}
    {% set tables = table_names %}
    {# 🔧 Debug: Tables provided #}
    {%- if execute %}
        {% do log("   ✓ Using provided " ~ tables | length ~ " table(s)", info=true) %}
    {%- endif %}
{% endif %}

{# 🔁 Loop: Generate YAML for each table #}
{% for table in tables %}
    {# 📝 Add table name #}
    {% do sources_yaml.append('      - name: ' ~ (table if case_sensitive_tables else table | lower) ) %}
    
    {# 📄 Add table description if requested #}
    {% if include_descriptions %}
        {% do sources_yaml.append('        description: ""' ) %}
    {% endif %}
    
    {# 📊 Generate column definitions if requested #}
    {% if generate_columns %}
        {% do sources_yaml.append('        columns:') %}

        {# 🔧 Debug: Processing table columns #}
        {%- if execute %}
            {% do log("      ⏳ Processing columns for: " ~ table, info=true) %}
        {%- endif %}
        
        {# 📍 Get relation and retrieve columns from database #}
        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns=adapter.get_columns_in_relation(table_relation) %}
        
        {# 🔧 Debug: Column count for table #}
        {%- if execute %}
            {% do log("         ✓ Found " ~ columns | length ~ " column(s)", info=true) %}
        {%- endif %}

        {# 🔄 Loop: Add each column to YAML #}
        {% for column in columns %}
            {# 📝 Add column name (case sensitive or lowercase) #}
            {% do sources_yaml.append('          - name: ' ~ (column.name if case_sensitive_cols else column.name | lower)) %}
            
            {# 📊 Add data type if requested #}
            {% if include_data_types %}
                {% do sources_yaml.append('            data_type: ' ~ codegen.data_type_format_source(column)) %}
            {% endif %}
            
            {# 📄 Add column description if requested #}
            {% if include_descriptions %}
                {% do sources_yaml.append('            description: ""' ) %}
            {% endif %}
        {% endfor %}
        
        {# 🔧 Debug: Table columns complete #}
        {%- if execute %}
            {% do log("         ✅ Columns added for: " ~ table, info=true) %}
        {%- endif %}
        
        {% do sources_yaml.append('') %}

    {% endif %}

{% endfor %}

{# 🏃 Execute block: only runs during dbt execution #}
{% if execute %}

    {# 🔧 Debug: YAML generation complete #}
    {% do log("   ✅ All tables processed | joining YAML...", info=true) %}
    
    {# 📤 Join all YAML lines and output #}
    {% set joined = sources_yaml | join ('\n') %}
    {{ print(joined) }}
    
    {# 🔧 Debug: Return final output #}
    {% do log("   ✅ Source YAML generation complete | returning output", info=true) %}
    {% do return(joined) %}

{% endif %}

{% endmacro %}
