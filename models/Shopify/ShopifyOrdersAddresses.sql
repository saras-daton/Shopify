{% if var('ShopifyOrdersAddresses') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_orders_tbl_ptrn','%shopify%orders','shopify_orders_exclude_tbl_ptrn','%shopify%fulfillment_orders') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(a.id as string) as order_id, 
        cast(checkout_id as string) as checkout_id,
        {{timezone_conversion("created_at")}} as created_at,
        {{ currency_conversion('b.value', 'b.from_currency_code', 'currency') }},
        email,
        {{timezone_conversion("processed_at")}} as processed_at,
        {{timezone_conversion("updated_at")}} as updated_at,
        {{extract_nested_value("billing_address","first_name","string")}} as billing_address_first_name,
        {{extract_nested_value("billing_address","address1","string")}} as billing_address_address1,
        {{extract_nested_value("billing_address","phone","string")}} as billing_address_phone,
        {{extract_nested_value("billing_address","city","string")}} as billing_address_city,
        {{extract_nested_value("billing_address","zip","string")}} as billing_address_zip,
        {{extract_nested_value("billing_address","province","string")}} as billing_address_province,
        {{extract_nested_value("billing_address","country","string")}} as billing_address_country,
        {{extract_nested_value("billing_address","last_name","string")}} as billing_address_last_name,
        {{extract_nested_value("billing_address","address2","string")}} as billing_address_address2,
        {{extract_nested_value("billing_address","latitude","numeric")}} as billing_address_latitude,
        {{extract_nested_value("billing_address","longitude","numeric")}} as billing_address_longitude,
        {{extract_nested_value("billing_address","name","string")}} as billing_address_name,
        {{extract_nested_value("billing_address","country_code","string")}} as billing_address_country_code,
        {{extract_nested_value("billing_address","province_code","string")}} as billing_address_province_code,
        {{extract_nested_value("billing_address","company","string")}} as billing_address_company,
        {{extract_nested_value("shipping_address","first_name","string")}} as shipping_address_first_name,
        {{extract_nested_value("shipping_address","address1","string")}} as shipping_address_address1,
        {{extract_nested_value("shipping_address","phone","string")}} as shipping_address_phone,
        {{extract_nested_value("shipping_address","city","string")}} as shipping_address_city,
        {{extract_nested_value("shipping_address","zip","string")}} as shipping_address_zip,
        {{extract_nested_value("shipping_address","province","string")}} as shipping_address_province,
        {{extract_nested_value("shipping_address","country","string")}} as shipping_address_country,
        {{extract_nested_value("shipping_address","last_name","string")}} as shipping_address_last_name,
        {{extract_nested_value("shipping_address","address2","string")}} as shipping_address_address2,
        {{extract_nested_value("shipping_address","latitude","numeric")}} as shipping_address_latitude,
        {{extract_nested_value("shipping_address","longitude","numeric")}} as shipping_address_longitude,
        {{extract_nested_value("shipping_address","name","string")}} as shipping_address_name,
        {{extract_nested_value("shipping_address","country_code","string")}} as shipping_address_country_code,
        {{extract_nested_value("shipping_address","province_code","string")}} as shipping_address_province_code,
        {{extract_nested_value("shipping_address","company","string")}} as shipping_address_company,
        {{timezone_conversion("closed_at")}} as closed_at,
        {{timezone_conversion("cancelled_at")}} as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
                {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} b on date(created_at) = b.date and currency = b.to_currency_code
                {% endif %}
                {{unnesting("BILLING_ADDRESS")}} 
                {{unnesting("SHIPPING_ADDRESS")}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_addresses_lookback') }},0) from {{ this }})
                {% endif %}
        qualify dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
