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
{{set_table_name('%shopify%refunds')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
        {% if target.type =='snowflake' %}
        coalesce(refund_line_items.value:id::varchar,'N/A') as refund_line_items_id,
        refund_line_items.value:quantity::numeric as refund_line_items_quantity,
        coalesce(refund_line_items.value:line_item_id::varchar,'N/A') as refund_line_items_line_item_id,
        refund_line_items.value:location_id::varchar as refund_line_items_location_id,
        refund_line_items.value:restock_type::varchar as refund_line_items_restock_type,
        refund_line_items.value:subtotal::numeric as refund_line_items_subtotal,
        refund_line_items.value:total_tax::numeric as refund_line_items_total_tax,
        presentment_money.value:amount as subtotal_set_presentment_amount,
        presentment_money.value:currency_code as subtotal_set_presentment_currency_code,
        shop_money.value:amount as subtotal_set_shop_amount,
        shop_money.value:currency_code as subtotal_set_shop_currency_code,
        line_item.value:id::varchar as line_item_id,
        coalesce(line_item.value:variant_id::varchar,'N/A') as line_item_variant_id,
        line_item.value:title::varchar as line_item_title,
        line_item.value:quantity::numeric as line_item_quantity,
        line_item.value:sku::varchar as line_item_sku,
        line_item.value:variant_title::varchar as line_item_variant_title,
        line_item.value:fulfillment_service::varchar as line_item_fulfillment_service,
        line_item.value:product_id::varchar as line_item_product_id,
        line_item.value:requires_shipping::varchar as line_item_requires_shipping,
        line_item.value:taxable::varchar as line_item_taxable,
        line_item.value:gift_card::varchar as line_item_gift_card,
        line_item.value:name::varchar as line_item_name,
        line_item.value:variant_inventory_management::varchar as line_item_variant_inventory_management,
        line_item.value:product_exists::varchar as line_item_product_exists,
        line_item.value:fulfillable_quantity::numeric as line_item_fulfillable_quantity,
        line_item.value:grams::varchar as line_item_grams,
        line_item.value:price::numeric as line_item_price,
        line_item.value:total_discount::numeric as line_item_total_discount,
        line_item.value:admin_graphql_api_id as line_item_admin_graphql_api_id,
        line_item.value:vendor as line_item_vendor,
        line_item.value:tax_code as line_item_tax_code,
        line_item.value:pre_tax_price::numeric as line_item_pre_tax_price,
        line_item.value:fulfillment_status::varchar as line_item_fulfillment_status,
        {% else %}
        coalesce(cast(refund_line_items.id as string),'N/A') as refund_line_items_id,
        refund_line_items.quantity as refund_line_items_quantity,
        coalesce(cast(refund_line_items.line_item_id as string),'N/A') as refund_line_items_line_item_id,
        refund_line_items.location_id as refund_line_items_location_id,
        refund_line_items.restock_type as refund_line_items_restock_type,
        refund_line_items.subtotal as refund_line_items_subtotal,
        refund_line_items.total_tax as refund_line_items_total_tax,
        presentment_money.amount as subtotal_set_presentment_amount,
        presentment_money.currency_code as subtotal_set_presentment_currency_code,
        shop_money.amount as subtotal_set_shop_amount,
        shop_money.currency_code as subtotal_set_shop_currency_code,
        cast(line_item.id as string) as line_item_id,
        coalesce(cast(line_item.variant_id as string),'N/A') as line_item_variant_id,
        line_item.title as line_item_title,
        line_item.quantity as line_item_quantity,
        line_item.sku as line_item_sku,
        line_item.variant_title as line_item_variant_title,
        line_item.fulfillment_service as line_item_fulfillment_service,
        cast(line_item.product_id as string) as line_item_product_id,
        line_item.requires_shipping as line_item_requires_shipping,
        line_item.taxable as line_item_taxable,
        line_item.gift_card as line_item_gift_card,
        line_item.name as line_item_name,
        line_item.variant_inventory_management as line_item_variant_inventory_management,
        line_item.product_exists as line_item_product_exists,
        line_item.fulfillable_quantity as line_item_fulfillable_quantity,
        line_item.grams as line_item_grams,
        cast(line_item.price as numeric) as line_item_price,
        cast(line_item.total_discount as numeric) as line_item_total_discount,
        line_item.admin_graphql_api_id as line_item_admin_graphql_api_id,
        line_item.vendor as line_item_vendor,
        line_item.tax_code as line_item_tax_code,
        line_item.pre_tax_price as line_item_pre_tax_price,
        line_item.fulfillment_status as line_item_fulfillment_status,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    
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
