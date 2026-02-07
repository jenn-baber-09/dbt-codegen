{#
  📚 generate_model_import_ctes
  Creates CTEs for model dependencies (refs, sources, vars, tables)
  
  Args: model_name, leading_commas
#}

{% macro generate_model_import_ctes(model_name, leading_commas = False) %}
    {{ return(adapter.dispatch('generate_model_import_ctes', 'codegen')(model_name, leading_commas)) }}
{% endmacro %}

{# ⚙️ Default implementation for all adapters #}
{% macro default__generate_model_import_ctes(model_name, leading_commas) %}

    {# 🔧 Debug: Log inputs #}
    {%- if execute -%}
        {% do log("🚀 [IMPORT_CTES] Generating CTEs for model: " ~ model_name, info=true) %}
    {%- endif -%}
    
    {# 📍 Retrieve model from graph #}
    {%- if execute -%}
    {%- set nodes = graph.nodes.values() -%}

    {%- set model = (nodes
        | selectattr('name', 'equalto', model_name) 
        | selectattr('resource_type', 'equalto', 'model')
        | list).pop() -%}

    {%- set model_raw_sql = model.raw_sql or model.raw_code -%}
    
    {# 🔧 Debug: Model found #}
    {% do log("   ✓ Model found in graph", info=true) %}
    {%- else -%}
    {%- set model_raw_sql = '' -%}
    {%- endif -%}

    {# 🔍 Regex patterns to find dependencies in SQL #}
    {# Each pattern captures: ref(), source(), var(), or direct table references #}

    {%- set re = modules.re -%}

    {# 🏷️ Pattern: Match CTE keyword (with) #}
    {%- set with_regex = '(?i)(?s)(^.*\s*|\s+|,)with\s' -%}
    {%- set does_raw_sql_contain_cte = re.search(with_regex, model_raw_sql) -%}
    
    {# 🔧 Debug: Check for existing CTEs #}
    {%- if execute %}
        {% do log("   ✓ Scanning for dependencies in model SQL...", info=true) %}
        {% do log("      Model has existing CTEs: " ~ (does_raw_sql_contain_cte is not none), info=true) %}
    {%- endif %}

    {%- set from_regexes = {
        'from_ref':
            '(?ix)

            # first matching group
            # from or join followed by at least 1 whitespace character
            (from|join)\s+

            # second matching group
            # opening {{, 0 or more whitespace character(s), ref, 0 or more whitespace character(s), an opening parenthesis, 0 or more whitespace character(s), 1 or 0 quotation mark
            ({{\s*ref\s*\(\s*[\'\"]?)
            
            # third matching group
            # at least 1 of anything except a parenthesis or quotation mark
            ([^)\'\"]+)
            
            # fourth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)
            ([\'\"]?\s*)

            # fifth matching group
            # a closing parenthesis, 0 or more whitespace character(s), closing }}
            (\)\s*}})
        
            ',
        'from_source':
            '(?ix)

            # first matching group
            # from or join followed by at least 1 whitespace character
            (from|join)\s+

            # second matching group
            # opening {{, 0 or more whitespace character(s), source, 0 or more whitespace character(s), an opening parenthesis, 0 or more whitespace character(s), 1 or 0 quotation mark
            ({{\s*source\s*\(\s*[\'\"]?)

            # third matching group
            # at least 1 of anything except a parenthesis or quotation mark
            ([^)\'\"]+)

            # fourth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)
            ([\'\"]?\s*)

            # fifth matching group
            # a comma
            (,)

            # sixth matching group
            # 0 or more whitespace character(s), 1 or 0 quotation mark
            (\s*[\'\"]?)

            # seventh matching group
            # at least 1 of anything except a parenthesis or quotation mark
            ([^)\'\"]+)

            # eighth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)
            ([\'\"]?\s*)

            # ninth matching group
            # a closing parenthesis, 0 or more whitespace character(s), closing }}
            (\)\s*}})

            ',
        'from_var_1':
            '(?ix)

            # first matching group
            # from or join followed by at least 1 whitespace character
            (from|join)\s+

            # second matching group
            # opening {{, 0 or more whitespace character(s), var, 0 or more whitespace character(s), an opening parenthesis, 0 or more whitespace character(s), 1 or 0 quotation mark
            ({{\s*var\s*\(\s*[\'\"]?)

            # third matching group
            # at least 1 of anything except a parenthesis or quotation mark
            ([^)\'\"]+)

            # fourth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)
            ([\'\"]?\s*)

            # fifth matching group
            # a closing parenthesis, 0 or more whitespace character(s), closing }}
            (\)\s*}})
            
            ',
        'from_var_2':
            '(?ix)

            # first matching group
            # from or join followed by at least 1 whitespace character
            (from|join)\s+
            
            # second matching group
            # opening {{, 0 or more whitespace character(s), var, 0 or more whitespace character(s), an opening parenthesis, 0 or more whitespace character(s), 1 or 0 quotation mark
            ({{\s*var\s*\(\s*[\'\"]?)

            # third matching group
            # at least 1 of anything except a parenthesis or quotation mark            
            ([^)\'\"]+)
            
            # fourth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)
            ([\'\"]?\s*)

            # fifth matching group
            # a comma
            (,)

            # sixth matching group
            # 0 or more whitespace character(s), 1 or 0 quotation mark            
            (\s*[\'\"]?)

            # seventh matching group
            # at least 1 of anything except a parenthesis or quotation mark            
            ([^)\'\"]+)

            # eighth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)            
            ([\'\"]?\s*)

            # ninth matching group
            # a closing parenthesis, 0 or more whitespace character(s), closing }}            
            (\)\s*}})
            
            ',
        'from_table_1':
            '(?ix)
            
            # first matching group
            # from or join followed by at least 1 whitespace character            
            (from|join)\s+
            
            # second matching group
            # 1 or 0 of (opening bracket, backtick, or quotation mark)
            ([\[`\"\']?)
            
            # third matching group
            # at least 1 word character
            (\w+)
            
            # fouth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark)
            ([\]`\"\']?)
            
            # fifth matching group
            # a period
            (\.)
            
            # sixth matching group
            # 1 or 0 of (opening bracket, backtick, or quotation mark)
            ([\[`\"\']?)
            
            # seventh matching group
            # at least 1 word character
            (\w+)
            
            # eighth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark) folowed by a whitespace character or end of string
            ([\]`\"\']?)(?=\s|$)
            
            ',
        'from_table_2':
            '(?ix)

            # first matching group
            # from or join followed by at least 1 whitespace character 
            (from|join)\s+
            
            # second matching group
            # 1 or 0 of (opening bracket, backtick, or quotation mark)            
            ([\[`\"\']?)
            
            # third matching group
            # at least 1 word character
            (\w+)

            # fouth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark)            
            ([\]`\"\']?)
            
            # fifth matching group
            # a period            
            (\.)
            
            # sixth matching group
            # 1 or 0 of (opening bracket, backtick, or quotation mark)
            ([\[`\"\']?)

            # seventh matching group
            # at least 1 word character            
            (\w+)
            
            # eighth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark) 
            ([\]`\"\']?)
            
            # ninth matching group
            # a period             
            (\.)
            
            # tenth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark)             
            ([\[`\"\']?)
            
            # eleventh matching group
            # at least 1 word character   
            (\w+)

            # twelfth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark) folowed by a whitespace character or end of string
            ([\]`\"\']?)(?=\s|$)
            
            ',
        'from_table_3':
            '(?ix)

            # first matching group
            # from or join followed by at least 1 whitespace character             
            (from|join)\s+
            
            # second matching group
            # 1 or 0 of (opening bracket, backtick, or quotation mark)            
            ([\[`\"\'])
            
            # third matching group
            # at least 1 word character or space 
            ([\w ]+)

            # fourth matching group
            # 1 or 0 of (closing bracket, backtick, or quotation mark) folowed by a whitespace character or end of string
            ([\]`\"\'])(?=\s|$)
            
            ',
        'config_block':'(?i)(?s)^.*{{\s*config\s*\([^)]+\)\s*}}'
    } -%}

    {%- set from_list = [] -%}
    {%- set config_list = [] -%}
    {%- set ns = namespace(model_sql = model_raw_sql) -%}

    {# 🔄 Loop: Process each regex pattern #}
    {%- if execute %}
        {% do log("   🔄 Processing regex patterns...", info=true) %}
    {%- endif %}

    {%- for regex_name, regex_pattern in from_regexes.items() -%}

        {%- set all_regex_matches = re.findall(regex_pattern, model_raw_sql) -%}
        
        {# 🔧 Debug: Log matches for each pattern #}
        {%- if execute and all_regex_matches|length > 0 %}
            {% do log("      ✓ " ~ regex_name ~ ": found " ~ all_regex_matches|length ~ " match(es)", info=true) %}
        {%- endif %}

        {%- for match in all_regex_matches -%}

            {# 📝 Process match and build CTE reference #}
            {%- if regex_name == 'config_block' -%}
                {%- set match_tuple = (match|trim, regex_name) -%}
                {%- do config_list.append(match_tuple) -%}
            {%- elif regex_name == 'from_source' -%}    
                {%- set full_from_clause = match[1:]|join|trim -%}
                {%- set cte_name = 'source_' + match[6]|lower -%}
                {%- set match_tuple = (cte_name, full_from_clause, regex_name) -%}
                {%- do from_list.append(match_tuple) -%}
            {%- elif regex_name == 'from_table_1' -%}
                {%- set full_from_clause = match[1:]|join()|trim -%}
                {%- set cte_name = match[2]|lower + '_' + match[6]|lower -%}
                {%- set match_tuple = (cte_name, full_from_clause, regex_name) -%}
                {%- do from_list.append(match_tuple) -%}   
            {%- elif regex_name == 'from_table_2' -%}
                {%- set full_from_clause = match[1:]|join()|trim -%}
                {%- set cte_name = match[2]|lower + '_' + match[6]|lower + '_' + match[10]|lower -%}
                {%- set match_tuple = (cte_name, full_from_clause, regex_name) -%}
                {%- do from_list.append(match_tuple) -%}                     
            {%- else -%}
                {%- set full_from_clause = match[1:]|join|trim -%}
                {%- set cte_name = match[2]|trim|lower -%}
                {%- set match_tuple = (cte_name, full_from_clause, regex_name) -%}
                {%- do from_list.append(match_tuple) -%}
            {%- endif -%}

        {%- endfor -%}

        {# 🔄 Replace references with CTE names in SQL #}
        {%- if regex_name == 'config_block' -%}
        {%- elif regex_name == 'from_source' -%}
            {%- set ns.model_sql = re.sub(regex_pattern, '\g<1> source_\g<7>', ns.model_sql) -%}            
        {%- elif regex_name == 'from_table_1' -%}
            {%- set ns.model_sql = re.sub(regex_pattern, '\g<1> \g<3>_\g<7>', ns.model_sql) -%}     
        {%- elif regex_name == 'from_table_2' -%}
            {%- set ns.model_sql = re.sub(regex_pattern, '\g<1> \g<3>_\g<7>_\g<11>', ns.model_sql) -%} 
        {%- else -%}   
            {%- set ns.model_sql = re.sub(regex_pattern, '\g<1> \g<3>', ns.model_sql) -%}         
        {% endif %}

    {%- endfor -%}

{# 📋 Build CTE definitions if dependencies found #}
{%- if from_list|length > 0 -%}
    
    {# 🔧 Debug: Dependencies found #}
    {%- if execute %}
        {% do log("   ✅ Found " ~ from_list|unique|length ~ " unique dependencies", info=true) %}
        {% do log("   🔨 Building CTE structure...", info=true) %}
    {%- endif %}

{%- set model_import_ctes -%}

    {%- for config_obj in config_list -%}

    {# Remove config block from original SQL #}
    {%- set ns.model_sql = ns.model_sql|replace(config_obj[0], '') -%}

{{ config_obj[0] }}

{% endfor -%}

    {# 🔁 Loop: Generate CTE for each dependency #}
    {%- for from_obj in from_list|unique|sort -%}

{%- if loop.first -%}with {% else -%}{%- if leading_commas -%},{%- endif -%}{%- endif -%}{{ from_obj[0] }} as (

    select * from {{ from_obj[1] }}
    {%- if from_obj[2] == 'from_source' and from_list|length > 1 %} 
    -- ⚠️ Create staging layer for raw sources
    {%- elif from_obj[2] == 'from_table_1' or from_obj[2] == 'from_table_2' or from_obj[2] == 'from_table_3' %}
    -- ⚠️ Use ref() or source() instead of direct table ref
    {%- elif from_obj[2] == 'from_var_1' or from_obj[2] == 'from_var_2' %}
    -- ⚠️ Use ref() or source() instead of var()
    {%- endif %}
  
){%- if ((loop.last and does_raw_sql_contain_cte) or (not loop.last)) and not leading_commas -%},{%- endif %}

{% endfor -%}

    {# ✅ Add original model SQL with CTE included #}
    {%- if does_raw_sql_contain_cte -%}
        {%- if leading_commas -%}
            {%- set replace_with = '\g<1>,' -%}
        {%- else -%}
            {%- set replace_with = '\g<1>' -%}
        {%- endif -%}
{{ re.sub(with_regex, replace_with, ns.model_sql, 1)|trim }}
    {%- else -%}
{{ ns.model_sql|trim }}
    {%- endif -%}

{%- endset -%}

{%- else -%}

{# 📊 No dependencies found - return original SQL #}
{% set model_import_ctes = model_raw_sql %}

{%- endif -%}

{# 🏃 Output final result #}
{%- if execute -%}
    {# 🔧 Debug: Done #}
    {% do log("   ✅ CTE generation complete | returning SQL", info=true) %}

{{ print(model_import_ctes) }}
{% do return(model_import_ctes) %}

{% endif %}

{% endmacro %}