{% if var('ShopifyCountries') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_countries_tbl_ptrn','%shopify%countries','shopify_countries_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
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
