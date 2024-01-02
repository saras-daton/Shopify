
{% if var('ShopifyOrders') %}
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
        a.id as order_id, 
        admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        {{timezone_conversion("created_at")}} as created_at,
        currency,
        cast(current_subtotal_price as numeric) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        {{extract_nested_value("discount_codes","code","string")}} as discount_code,
        {{extract_nested_value("discount_codes","amount","string")}} as discount_amount,
        {{extract_nested_value("discount_codes","type","string")}} as discount_type,
        email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        name,
        number,
        order_number,
        order_status_url,
        payment_gateway_names,
        phone,
        presentment_currency,
        {{timezone_conversion("processed_at")}} as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        cast(subtotal_price as numeric) as subtotal_price,
        tags,
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
        {{timezone_conversion("updated_at")}} as updated_at,
        cast(app_id as string) as app_id,
        customer_locale,
        note,
        {{timezone_conversion("closed_at")}} as closed_at,
        fulfillment_status,
        cast(location_id as string) as location_id,
        cancel_reason,
        {{ currency_conversion('b.value', 'b.from_currency_code', 'currency') }},
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
            {{unnesting("discount_codes")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_lookback') }},0) from {{ this }})
            {% endif %}
        qualify dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
