{% if var('ShopifyOrders') %}
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
        {{extract_nested_value("note_attributes","name","string")}} as note_attributes_name,
        {{extract_nested_value("note_attributes","value","string")}} as note_attributes_value,
        order_number,
        order_status_url,
        {{timezone_conversion("processed_at")}} as processed_at,
        {{timezone_conversion("updated_at")}} as updated_at,
        {{timezone_conversion("closed_at")}} as closed_at,
        {{timezone_conversion("cancelled_at")}} as cancelled_at,
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