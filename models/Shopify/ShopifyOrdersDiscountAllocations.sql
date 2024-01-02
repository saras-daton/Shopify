{% if var('ShopifyOrdersDiscountAllocations') %}
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
        a.id,
        {{timezone_conversion("created_at")}} as created_at,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        {{timezone_conversion("processed_at")}} as processed_at,
        {{timezone_conversion("updated_at")}} as updated_at,
        {{extract_nested_value("line_items","id","string")}} as line_items_id,
        {{extract_nested_value("discount_allocations","amount","numeric")}} as discount_allocations_amount,
        {{extract_nested_value("shop_money","amount","numeric")}} as shop_money_amount,
        {{extract_nested_value("shop_money","currency_code","string")}} as shop_money_currency_code,
        {{extract_nested_value("presentment_money","amount","numeric")}} as presentment_money_amount,
        {{extract_nested_value("presentment_money","currency_code","string")}} as presentment_money_currency_code,
        {{extract_nested_value("discount_allocations","discount_application_index","numeric")}} as discount_allocations_discount_application_index,
        {{extract_nested_value("line_items","pre_tax_price","string")}} as line_items_pre_tax_price,
        {{extract_nested_value("line_items","tax_code","string")}} as line_items_tax_code,
        {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
        {{extract_nested_value("line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
        {{timezone_conversion("closed_at")}} as closed_at,
        {{timezone_conversion("cancelled_at")}} as cancelled_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
                {{unnesting("line_items")}}
                {{multi_unnesting("line_items","discount_allocations")}}
                {{multi_unnesting("discount_allocations","amount_set")}}
                {{multi_unnesting("amount_set","shop_money")}}
                {{multi_unnesting("amount_set","presentment_money")}}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_discount_allocations_lookback') }},0) from {{ this }})
            {% endif %}
        qualify dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}