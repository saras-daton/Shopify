{% if var('ShopifyOrdersShippingLines') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
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
        {{timezone_conversion("created_at")}} as created_at,
        {{ currency_conversion('b.value', 'b.from_currency_code', 'currency') }},
        {{extract_nested_value("discount_codes","code","string")}} as discount_code,
        {{extract_nested_value("discount_codes","amount","numeric")}} as discount_amount,
        {{extract_nested_value("discount_codes","type","string")}} as discount_type,
        {{timezone_conversion("processed_at")}} as processed_at,
        {{timezone_conversion("a.updated_at")}} as updated_at,
        {{extract_nested_value("shipping_lines","id","string")}} as shipping_lines_id,
        {{extract_nested_value("shipping_lines","code","string")}} as shipping_lines_code,
        {{extract_nested_value("shipping_lines","discounted_price","numeric")}} as shipping_lines_discounted_price,
        {{extract_nested_value("shipping_lines","price","numeric")}} as shipping_lines_price,
        {{extract_nested_value("shipping_lines","source","string")}} as shipping_lines_source,
        {{extract_nested_value("shipping_lines","title","string")}} as shipping_lines_title,
        {{extract_nested_value("shipping_lines","carrier_identifier","string")}} as shipping_lines_carrier_identifier,
        {{timezone_conversion("closed_at")}} as closed_at,
        {{timezone_conversion("cancelled_at")}} as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
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