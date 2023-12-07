{% if var('ShopifyOrders') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
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
        cast(a.id as string) as order_id, 
        admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        {{timezone_conversion("created_at")}} as created_at,
        {{ currency_conversion('b.value', 'b.from_currency_code', 'currency') }},
        current_subtotal_price,
        current_subtotal_price_set,
        current_total_discounts,
        current_total_discounts_set,
        current_total_price,
        current_total_price_set,
        current_total_tax,
        current_total_tax_set,
        {{extract_nested_value("discount_codes","code","string")}} as discount_code,
        {{extract_nested_value("discount_codes","amount","numeric")}} as discount_amount,
        {{extract_nested_value("discount_codes","type","string")}} as discount_type,
        {{extract_nested_value("note_attributes","name","string")}} as note_attributes_name,
        {{extract_nested_value("note_attributes","value","string")}} as note_attributes_value,
        email,
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
        phone,
        presentment_currency,
        {{timezone_conversion("processed_at")}} as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        subtotal_price,
        subtotal_price_set,
        tags,
        tax_lines,
        taxes_included,
        test,
        token,
        total_discounts,
        total_discounts_set,
        total_line_items_price,
        total_line_items_price_set,
        total_outstanding,
        total_price,
        total_price_set,
        total_price_usd,
        total_shipping_price_set,
        total_tax,
        total_tax_set,
        total_tip_received,
        total_weight,
        {{timezone_conversion("updated_at")}} as updated_at,
        billing_address,
        customer,
        discount_applications,
        fulfillments,
        line_items,
        payment_details,
        refunds,
        shipping_address,
        shipping_lines,
        app_id,
        customer_locale,
        note,
        closed_at,
        fulfillment_status,
        cancel_reason,
        cancelled_at,
        user_id,
        a.{{ daton_user_id() }} as _daton_user_id,
        a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
        a.{{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from {{ i }} a
    {% if var('currency_conversion_flag') %}
        left join {{ ref('ExchangeRates') }} b on date(created_at) = b.date and currency = b.to_currency_code
    {% endif %}
    {{ unnesting("discount_codes") }}
    {{ unnesting("note_attributes") }}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where a.{{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_note_attributes_lookback') }},0) from {{ this }})
    {% endif %} 

    qualify dense_rank() over (partition by a.id, {{extract_nested_value("note_attributes","name","string")}} order by a.{{ daton_batch_runtime() }} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}