{% if var('ShopifyRefundsLineItems') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%shopify%refunds')}}
{% endset %}  
 
{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then b.subtotal_set_presentment_currency_code else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            b.subtotal_set_presentment_currency_code as exchange_currency_code, 
        {% endif %}
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        cast(a.id as string) refund_id,
        cast(order_id as string) order_id,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        note,
        cast(user_id as string) user_id,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        restock,
        a.admin_graphql_api_id,
        {{extract_nested_value('refund_line_items','id','string')}} as refund_line_items_id,
        {{extract_nested_value("refund_line_items","quantity","numeric")}} as refund_line_items_quantity,
        {{extract_nested_value("refund_line_items","line_item_id","string")}} as refund_line_items_line_item_id,
        {{extract_nested_value("refund_line_items","location_id","string")}} as refund_line_items_location_id,
        {{extract_nested_value("refund_line_items","restock_type","string")}} as refund_line_items_restock_type,
        {{extract_nested_value("refund_line_items","subtotal","numeric")}} as refund_line_items_subtotal,
        {{extract_nested_value("refund_line_items","total_tax","numeric")}} as refund_line_items_total_tax,
        {{extract_nested_value("presentment_money","amount","string")}} as subtotal_set_presentment_amount,
        {{extract_nested_value("presentment_money","currency_code","string")}} as subtotal_set_presentment_currency_code,
        {{extract_nested_value("shop_money","amount","string")}} as subtotal_set_shop_amount,
        {{extract_nested_value("shop_money","currency_code","string")}} as subtotal_set_currency_code,
        {{extract_nested_value("refund_line_items","total_tax_set","string")}} as refund_line_items_total_tax_set,
        {{extract_nested_value("line_item","id","string")}} as line_item_id,
        {{extract_nested_value("line_item","variant_id","string")}} as line_item_variant_id,
        {{extract_nested_value("line_item","title","string")}} as line_item_title,
        {{extract_nested_value("line_item","quantity","numeric")}} as line_item_quantity,
        {{extract_nested_value("line_item","sku","string")}} as line_item_sku,
        {{extract_nested_value("line_item","variant_title","string")}} as line_item_variant_id,
        {{extract_nested_value("line_item","fulfillment_service","string")}} as line_item_fulfillment_service,
        {{extract_nested_value("line_item","product_id","string")}} as line_item_product_id,
        {{extract_nested_value("line_item","requires_shipping","string")}} as line_item_requires_shipping,
        {{extract_nested_value("line_item","taxable","string")}} as line_item_taxable,
        {{extract_nested_value("line_item","gift_card","string")}} as line_item_gift_card,
        {{extract_nested_value("line_item","name","string")}} as line_item_name,
        {{extract_nested_value("line_item","variant_inventory_management","string")}} as line_item_variant_inventory_management,       
        {{extract_nested_value("line_item","product_exists","string")}} as line_item_id_product_exists,
        {{extract_nested_value("line_item","fulfillable_quantity","numeric")}} as line_item_fulfillable_quantity,
        {{extract_nested_value("line_item","grams","string")}} as line_item_grams,
        {{extract_nested_value("line_item","price","numeric")}} as line_item_price,
        {{extract_nested_value("line_item","total_discount","numeric")}} as line_item_total_discount,
        {{extract_nested_value("line_item","price_set","string")}} as line_item_price_set,
        {{extract_nested_value("line_item","total_discount_set","string")}} as line_item_total_discount_set,
        {{extract_nested_value("line_item","discount_allocations","string")}} as line_item_discount_allocations,
        {{extract_nested_value("line_item","admin_graphql_api_id","string")}} as line_item_admin_graphql_api_id,
        {{extract_nested_value("line_item","properties","string")}} as line_item_properties,
        {{extract_nested_value("line_item","tax_lines","string")}} as line_item_tax_lines,
        {{extract_nested_value("line_item","pre_tax_price_set","string")}} as line_item_pre_tax_price_set,
        {{extract_nested_value("line_item","vendor","string")}} as line_item_vendor,
        {{extract_nested_value("line_item","tax_code","string")}} as line_item_tax_code,
        {{extract_nested_value("line_item","pre_tax_price","numeric")}} as line_item_pre_tax_price,
        {{extract_nested_value("line_item","fulfillment_status","string")}} as line_item_fulfillment_status,
        {{extract_nested_value("line_item","origin_location","string")}} as line_item_origin_location,
        {{extract_nested_value("line_item","destination_location","string")}} as line_item_destination_location,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("refund_line_items")}}
            {{multi_unnesting("refund_line_items","line_item")}}
            {{multi_unnesting("refund_line_items","subtotal_set")}}
            {{multi_unnesting("subtotal_set","shop_money")}}
            {{multi_unnesting("subtotal_set","presentment_money")}}

            qualify  
            {% if target.type =='snowflake' %}
            row_number() over (partition by a.id, refund_line_items.value:id, line_item.value:variant_id order by _daton_batch_runtime desc) = 1
            {% else %}
            row_number() over (partition by a.id, refund_line_items.id, line_item.variant_id order by _daton_batch_runtime desc) = 1
            {% endif %}

            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}

        ) b
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.subtotal_set_presentment_currency_code = c.to_currency_code
        {% endif %}


    {% if not loop.last %} union all {% endif %}
{% endfor %}
