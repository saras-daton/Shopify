{% if var('ShopifyOrdersLineItems') %}
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
    safe_cast(a.id as string) as order_id, 
    {{timezone_conversion("a.created_at")}} as created_at,
    {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
    {{timezone_conversion("a.processed_at")}} as processed_at,
    {{timezone_conversion("a.updated_at")}} as updated_at,
    {{extract_nested_value("line_items","id","string")}} as line_items_id,
    {{extract_nested_value("line_items","admin_graphql_api_id","string")}} as line_items_admin_graphql_api_id,
    {{extract_nested_value("line_items","fulfillable_quantity","numeric")}} as line_items_fulfillable_quantity,
    {{extract_nested_value("line_items","fulfillment_service","string")}} as line_items_fulfillment_service,
    {{extract_nested_value("line_items","gift_card","boolean")}} as line_items_gift_card,
    {{extract_nested_value("line_items","grams","numeric")}} as line_items_grams, 
    {{extract_nested_value("line_items","name","string")}} as line_items_name,
    {{extract_nested_value("line_items","price","numeric")}} as line_items_price,
    {{extract_nested_value("line_items","product_exists","boolean")}} as line_items_product_exists,
    {{extract_nested_value("line_items","product_id","string")}} as line_items_product_id,
    {{extract_nested_value("line_items","quantity","numeric")}} as line_items_quantity,
    {{extract_nested_value("line_items","requires_shipping","boolean")}} as line_items_requires_shipping,
    {{extract_nested_value("line_items","sku","string")}} as line_items_sku,
    {{extract_nested_value("line_items","taxable","boolean")}} as line_items_taxable,
    {{extract_nested_value("line_items","title","string")}} as line_items_title,
    {{extract_nested_value("line_items","total_discount","numeric")}} as line_items_total_discount,
    {{extract_nested_value("line_items","variant_id","string")}} as line_items_variant_id,
    {{extract_nested_value("line_items","variant_inventory_management","string")}} as line_items_variant_inventory_management,
    {{extract_nested_value("line_items","variant_title","string")}} as line_items_variant_title,
    {{extract_nested_value("line_items","pre_tax_price","numeric")}} as line_items_pre_tax_price,
    {{extract_nested_value("line_items","tax_code","string")}} as line_items_tax_code,
    {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
    {{extract_nested_value("line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
    {{timezone_conversion("a.closed_at")}} as closed_at,
    {{timezone_conversion("a.cancelled_at")}} as cancelled_at,
    a.{{ daton_user_id() }} as _daton_user_id,
    a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
    a.{{ daton_batch_id() }} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from {{ i }} a
    {% if var('currency_conversion_flag') %}
        left join {{ ref('ExchangeRates') }} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
    {% endif %}
    {{ unnesting("line_items") }}
    {{ unnesting("billing_address") }}
    {{ unnesting("shipping_address") }}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where a.{{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_line_items_lookback') }},0) from {{ this }})
    {% endif %}

qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %}