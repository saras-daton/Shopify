{% if var('ShopifyRefundsLineItems') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_refunds_tbl_ptrn','%shopify%refunds','shopify_refunds_line_items_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'b.subtotal_set_presentment_currency_code') }},
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
        safe_cast(a.admin_graphql_api_id as string) admin_graphql_api_id,
        {{extract_nested_value("presentment_money","currency_code","string")}} as subtotal_set_presentment_currency_code,
        {{extract_nested_value("line_item","id","string")}} as line_item_id,
        {{extract_nested_value("line_item","variant_id","string")}} as line_item_variant_id,
        {{extract_nested_value("line_item","title","string")}} as line_item_title,
        {{extract_nested_value("line_item","quantity","numeric")}} as line_item_quantity,
        {{extract_nested_value("line_item","sku","string")}} as line_item_sku,
        {{extract_nested_value("line_item","variant_title","string")}} as line_item_variant_title,
        {{extract_nested_value("line_item","fulfillment_service","string")}} as line_item_fulfillment_service,
        {{extract_nested_value("line_item","product_id","string")}} as line_item_product_id,
        {{extract_nested_value("line_item","requires_shipping","boolean")}} as line_item_requires_shipping,
        {{extract_nested_value("line_item","taxable","boolean")}} as line_item_taxable,
        {{extract_nested_value("line_item","gift_card","boolean")}} as line_item_gift_card,
        {{extract_nested_value("line_item","name","string")}} as line_item_name,
        {{extract_nested_value("line_item","variant_inventory_management","string")}} as line_item_variant_inventory_management,       
        {{extract_nested_value("line_item","product_exists","boolean")}} as line_item_product_exists,
        {{extract_nested_value("line_item","fulfillable_quantity","numeric")}} as line_item_fulfillable_quantity,
        {{extract_nested_value("line_item","grams","numeric")}} as line_item_grams,
        {{extract_nested_value("line_item","price","numeric")}} as line_item_price,
        {{extract_nested_value("line_item","total_discount","numeric")}} as line_item_total_discount,
        {{extract_nested_value("line_item","admin_graphql_api_id","string")}} as line_item_admin_graphql_api_id,
        {{extract_nested_value("line_item","vendor","string")}} as line_item_vendor,
        {{extract_nested_value("line_item","tax_code","string")}} as line_item_tax_code,
        {{extract_nested_value("line_item","pre_tax_price","numeric")}} as line_item_pre_tax_price,
        {{extract_nested_value("line_item","fulfillment_status","string")}} as line_item_fulfillment_status,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("refund_line_items")}}
            {{multi_unnesting("refund_line_items","line_item")}}
            {{multi_unnesting("refund_line_items","subtotal_set")}}
            {{multi_unnesting("subtotal_set","shop_money")}}
            {{multi_unnesting("subtotal_set","presentment_money")}}

            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_refunds_line_items_lookback') }},0) from {{ this }})
            {% endif %}

            qualify row_number() over (partition by a.id, {{extract_nested_value('refund_line_items','id','string')}}, {{extract_nested_value("line_item","variant_id","string")}} order by _daton_batch_runtime desc) = 1

        ) b
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.subtotal_set_presentment_currency_code = c.to_currency_code
        {% endif %}


    {% if not loop.last %} union all {% endif %}
{% endfor %}