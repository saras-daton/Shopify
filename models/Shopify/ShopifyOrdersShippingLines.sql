{% if var('ShopifyOrdersShippingLines') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if var('currency_conversion_flag') %}
    -- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_orders_shipping_lines_tbl_ptrn'),
exclude=var('shopify_orders_shipping_lines_exclude_tbl_ptrn'),
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
        '{{ brand }}' as brand,
        '{{ store }}' as store,
        cast(a.id as string) as order_id, 
        admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        currency,
        cast(current_subtotal_price as numeric) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        {{extract_nested_value("discount_codes","code","string")}} as discount_code,
        {{extract_nested_value("discount_codes","amount","numeric")}} as discount_amount,
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
        a.phone,
        presentment_currency,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
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
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        coalesce({{extract_nested_value("shipping_lines","id","string")}},'N/A') as shipping_lines_id,
        {{extract_nested_value("shipping_lines","code","string")}} as shipping_lines_code,
        {{extract_nested_value("shipping_lines","discounted_price","numeric")}} as shipping_lines_discounted_price,
        {{extract_nested_value("shipping_lines","price","numeric")}} as shipping_lines_price,
        {{extract_nested_value("shipping_lines","source","string")}} as shipping_lines_source,
        {{extract_nested_value("shipping_lines","title","string")}} as shipping_lines_title,
        {{extract_nested_value("shipping_lines","carrier_identifier","string")}} as shipping_lines_carrier_identifier,
        cast(app_id as string) as app_id,
        customer_locale,
        note,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        fulfillment_status,
        cast(location_id as string) as location_id,
        cancel_reason,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
    {% if var('currency_conversion_flag') %}
        case when b.value is null then 1 else b.value end as exchange_currency_rate,
        case when b.from_currency_code is null then currency else b.from_currency_code end as exchange_currency_code,
    {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        currency as exchange_currency_code,
    {% endif %}
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
    {{ unnesting("shipping_lines") }}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where a.{{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_shipping_lines_lookback') }},0) from {{ this }})
    {% endif %} 

    qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}