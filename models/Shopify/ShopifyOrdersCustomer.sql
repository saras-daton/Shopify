{% if var('ShopifyOrdersCustomer') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_orders_tbl_ptrn","shopify_orders_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        coalesce(cast(a.id as string),'N/A') as order_id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        {{timezone_conversion("a.created_at")}} as created_at,
        cast(current_subtotal_price as numeric) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        coalesce(a.email,'N/A') as email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        a.name,
        number,
        order_number,
        order_status_url,
        payment_gateway_names,
        a.phone,
        presentment_currency,
        {{timezone_conversion("a.processed_at")}} as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        cast(subtotal_price as numeric) as subtotal_price,
        a.tags,
        taxes_included,
        test,
        token,
        cast(total_discounts as numeric) as total_discounts,
        cast(total_line_items_price as numeric) as total_line_items_price,
        cast(total_outstanding as numeric) as total_outstanding,
        cast(total_price as numeric) as total_price,
        cast(total_price_usd as numeric) as total_price_usd,
        cast(total_tax as numeric) as total_tax,
        cast(total_tip_received as numeric) as total_tip_received,
        total_weight,
        {{timezone_conversion("a.updated_at")}} as updated_at,
        {{extract_nested_value("customer","id","string")}} as customer_id,
        {{extract_nested_value("customer","email","string")}} as customer_email,
        {{extract_nested_value("customer","accepts_marketing","boolean")}} as customer_accepts_marketing,
        {{extract_nested_value("customer","created_at","timestamp")}} as customer_created_at,
        {{extract_nested_value("customer","updated_at","timestamp")}} as customer_updated_at,
        {{extract_nested_value("customer","first_name","string")}} as customer_first_name,
        {{extract_nested_value("customer","last_name","string")}} as customer_last_name,
        {{extract_nested_value("customer","orders_count","numeric")}} customer_orders_count,
        {{extract_nested_value("customer","state","string")}} as customer_state,
        {{extract_nested_value("customer","total_spent","numeric")}} as customer_total_spent,
        {{extract_nested_value("customer","last_order_id","numeric")}} as customer_last_order_id,
        {{extract_nested_value("customer","verified_email","boolean")}} as customer_verified_email,
        {{extract_nested_value("customer","tax_exempt","boolean")}} as customer_tax_exempt,
        {{extract_nested_value("customer","phone","string")}} as customer_phone,
        {{extract_nested_value("customer","tags","string")}} as customer_tags,
        {{extract_nested_value("customer","currency","string")}} as customer_currency,
        {{extract_nested_value("customer","last_order_name","string")}} as customer_last_order_name,
        {{extract_nested_value("customer","accepts_marketing_updated_at","timestamp")}} as customer_accepts_marketing_updated_at,
        {{extract_nested_value("customer","admin_graphql_api_id","string")}} as customer_admin_graphql_api_id,
        {{extract_nested_value("default_address","id","string")}} as default_address_id,
        {{extract_nested_value("default_address","customer_id","string")}} as default_address_customer_id,
        {{extract_nested_value("default_address","address1","string")}} as default_address_address1,
        {{extract_nested_value("default_address","address2","string")}} as default_address_address2,
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
        {{extract_nested_value("default_address","first_name","string")}} as default_address_first_name,
        {{extract_nested_value("default_address","last_name","string")}} as default_address_last_name,
        {{extract_nested_value("default_address","company","string")}} as default_address_company,
        cast(app_id as string) as app_id,
        customer_locale,
        a.note,
        {{timezone_conversion("a.closed_at")}} as closed_at,
        fulfillment_status,
        cast(location_id as string) as location_id,
        cancel_reason,
        {{timezone_conversion("a.cancelled_at")}} as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("CUSTOMER")}}
            {{multi_unnesting("CUSTOMER","DEFAULT_ADDRESS")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_customer_lookback') }},0) from {{ this }})
            {% endif %}
            {% if target.type =='snowflake' %}
        qualify dense_rank() over (partition by a.id, CUSTOMER.VALUE:id order by a._daton_batch_runtime desc) = 1
        {% else %}
        qualify dense_rank() over (partition by a.id, CUSTOMER.id order by a._daton_batch_runtime desc) = 1
        {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}