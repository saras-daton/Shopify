{% if var('ShopifyCountries') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_countries_tbl_ptrn'),
exclude=var('shopify_countries_exclude_tbl_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
            {% set hr = 0 %}
    {% endif %}


    select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(a.id as string) as id,
        a.name,
        a.code,
        a.tax_name,
        a.tax,
        {{extract_nested_value("provinces","id","string")}} as provinces_id,
        {{extract_nested_value("provinces","country_id","string")}} as provinces_country_id,
        {{extract_nested_value("provinces","name","string")}} as provinces_name,
        {{extract_nested_value("provinces","code","string")}} as provinces_code,
        {{extract_nested_value("provinces","shipping_zone_id","string")}} as provinces_shipping_zone_id,
        {{extract_nested_value("provinces","tax","string")}} as provinces_tax,
        {{extract_nested_value("provinces","tax_percentage","string")}} as provinces_tax_percentage,
        {{extract_nested_value("provinces","tax_name","string")}} as provinces_tax_name,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id     
    from  {{i}} a
        {{unnesting("provinces")}} 
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_countries_lookback') }},0) from {{ this }})
        {% endif %}
    qualify dense_rank() over (partition by a.id, {{extract_nested_value("provinces","id","string")}} order by {{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
