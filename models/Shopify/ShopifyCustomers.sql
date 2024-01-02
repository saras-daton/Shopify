{% if var('ShopifyCustomers') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_customers_tbl_ptrn','%shopify%customers','shopify_customers_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(a.id as string) as customers_id,
        email,
        accepts_marketing,
        {{timezone_conversion("created_at")}} as created_at,
        {{timezone_conversion("updated_at")}} as updated_at,
        a.first_name,
        a.last_name,
        orders_count,
        a.state,
        cast(total_spent as numeric) as total_spent,
        verified_email,
        tax_exempt,
        tags,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        a.phone,
        {{timezone_conversion("accepts_marketing_updated_at")}} as accepts_marketing_updated_at,
        a.admin_graphql_api_id,
        {{extract_nested_value("default_address","id","string")}} as default_address_id,
        {{extract_nested_value("default_address","customer_id","string")}} as default_address_customer_id,
        {{extract_nested_value("default_address","first_name","string")}} as default_address_first_name,
        {{extract_nested_value("default_address","last_name","string")}} as default_address_last_name,
        {{extract_nested_value("default_address","address1","string")}} as default_address_address1,
        {{extract_nested_value("default_address","city","string")}} as default_address_city,
        {{extract_nested_value("default_address","province","string")}} as default_address_province,
        {{extract_nested_value("default_address","country","string")}} as default_address_country,
        {{extract_nested_value("default_address","zip","string")}} as default_address_zip,
        {{extract_nested_value("default_address","phone","string")}} as default_address_phone,
        {{extract_nested_value("default_address","name","string")}} as default_address_name,
        {{extract_nested_value("default_address","province_code","string")}} as default_address_province_code,
        {{extract_nested_value("default_address","country_code","string")}} as default_address_country_code,
        {{extract_nested_value("default_address","country_name","string")}} as default_address_country_name,
        {{extract_nested_value("default_address","default","boolean")}} as default_address_default,
        {{extract_nested_value("default_address","address2","string")}} as default_address_address2,
        {{extract_nested_value("default_address","company","string")}} as default_address_company,
        last_order_id,
        last_order_name,
        marketing_opt_in_level,
        note,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from  {{i}} a
                {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(created_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {{unnesting("default_address")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_customers_lookback') }},0) from {{ this }})
                {% endif %}
        qualify dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
