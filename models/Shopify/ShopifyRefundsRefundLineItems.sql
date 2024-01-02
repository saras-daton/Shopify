{% if var('ShopifyRefundsRefundLineItems') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_refunds_tbl_ptrn','%shopify%refunds','shopify_refunds_refund_line_items_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}
    
        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {{ currency_conversion('c.value', 'c.from_currency_code', 'b.refund_line_items_subtotal_set_presentment_money_currency_code') }},
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        safe_cast(a.id as string) refund_id,
        safe_cast(order_id as string) order_id,
        {{timezone_conversion("a.created_at")}} as created_at,
        note,
        safe_cast(user_id as string) user_id,
        {{timezone_conversion("a.processed_at")}} as processed_at,
        restock,
        admin_graphql_api_id,
        {{extract_nested_value('refund_line_items','id','string')}} as refund_line_items_id,
        {{extract_nested_value("refund_line_items","quantity","numeric")}} as refund_line_items_quantity,
        {{extract_nested_value("refund_line_items","line_item_id","string")}} as refund_line_items_line_item_id,
        {{extract_nested_value("refund_line_items","location_id","string")}} as refund_line_items_location_id,
        {{extract_nested_value("refund_line_items","restock_type","string")}} as refund_line_items_restock_type,
        {{extract_nested_value("refund_line_items","subtotal","numeric")}} as refund_line_items_subtotal,
        {{extract_nested_value("refund_line_items","total_tax","numeric")}} as refund_line_items_total_tax,
        {{extract_nested_value("presentment_money","amount","numeric")}} as refund_line_items_subtotal_set_presentment_money_amount,
        {{extract_nested_value("presentment_money","currency_code","string")}} as refund_line_items_subtotal_set_presentment_money_currency_code,
        {{extract_nested_value("shop_money","amount","numeric")}} as refund_line_items_subtotal_set_shop_money_amount,
        {{extract_nested_value("shop_money","currency_code","string")}} as refund_line_items_subtotal_set_shop_money_currency_code,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("refund_line_items")}}
            {{multi_unnesting("refund_line_items","subtotal_set")}}
            {{multi_unnesting("subtotal_set","shop_money")}}
            {{multi_unnesting("subtotal_set","presentment_money")}}
            
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_refunds_refund_line_items_lookback') }},0) from {{ this }})
            {% endif %}

            qualify row_number() over (partition by order_id, a.id, {{extract_nested_value('refund_line_items','id','string')}} order by _daton_batch_runtime desc) = 1
        ) b
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.refund_line_items_subtotal_set_presentment_money_currency_code = c.to_currency_code
        {% endif %}
        

    {% if not loop.last %} union all {% endif %}
{% endfor %}