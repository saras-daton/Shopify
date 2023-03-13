{% if var('ShopifyOrdersFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

with unnested_fulfillment_orders as(
{% set table_name_query %}
{{set_table_name('%shopify%orders%')}}    
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

    SELECT * 
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        admin_graphql_api_id,
        CAST(a.id as string) order_id, 
        a.email,
        closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
        {% if target.type =='snowflake' %}
        COALESCE(fulfillment_orders.VALUE:id::VARCHAR,'') as fulfillment_orders_id,
        fulfillment_orders.VALUE:shop_id,
        fulfillment_orders.VALUE:order_id::VARCHAR as fulfillment_orders_order_id,
        fulfillment_orders.VALUE:assigned_location_id,
        fulfillment_orders.VALUE:request_status,
        fulfillment_orders.VALUE:status,
        fulfillment_orders.VALUE:supported_actions,
        destination.VALUE:id::VARCHAR as destination_id,
        destination.VALUE:address1,
        destination.VALUE:address2,
        destination.VALUE:city,
        destination.VALUE:country,
        destination.VALUE:email::VARCHAR as destination_email,
        destination.VALUE:first_name,
        destination.VALUE:last_name,
        destination.VALUE:phone::VARCHAR as destination_phone,
        destination.VALUE:province,
        destination.VALUE:zip,
        COALESCE(line_items.VALUE:id::VARCHAR,'') as line_items_id,
        line_items.VALUE:shop_id::VARCHAR as line_items_shop_id,
        line_items.VALUE:fulfillment_order_id,
        line_items.VALUE:quantity,
        line_items.VALUE:line_item_id,
        line_items.VALUE:inventory_item_id,
        line_items.VALUE:fulfillable_quantity,
        line_items.VALUE:variant_id,
        fulfillment_orders.VALUE:fulfill_at::timestamp as fulfill_at,
        delivery_method.VALUE:id::VARCHAR as delivery_method_id,
        delivery_method.VALUE:method_type,
        assigned_location.VALUE:address1::VARCHAR as assigned_location_address1,
        assigned_location.VALUE:address2::VARCHAR as assigned_location_address2,
        assigned_location.VALUE:city assigned_location_city,
        assigned_location.VALUE:country_code,
        assigned_location.VALUE:location_id::VARCHAR as assigned_location_location_id,
        assigned_location.VALUE:name::VARCHAR as assigned_location_name,
        assigned_location.VALUE:phone::VARCHAR as assigned_location_phone,
        assigned_location.VALUE:province::VARCHAR as assigned_location_province,
        assigned_location.VALUE:zip::VARCHAR as assigned_location_zip,
        {% else %}
        COALESCE(CAST(fulfillment_orders.id as string),'') as fulfillment_orders_id,
        fulfillment_orders.shop_id as shop_id,
        fulfillment_orders.order_id as fulfillment_orders_order_id,
        fulfillment_orders.assigned_location_id as assigned_location_id,
        fulfillment_orders.request_status as request_status,
        fulfillment_orders.status as status,
        fulfillment_orders.supported_actions as supported_actions,
        destination.id as destination_id,
        destination.address1,
        destination.address2,
        destination.city,
        destination.country,
        destination.email as destination_email,
        destination.first_name,
        destination.last_name,
        destination.phone as destination_phone,
        destination.province,
        destination.zip,
        COALESCE(CAST(line_items.id as string),'') as line_items_id,
        line_items.shop_id as line_items_shop_id,
        line_items.fulfillment_order_id,
        line_items.quantity,
        line_items.line_item_id,
        line_items.inventory_item_id,
        line_items.fulfillable_quantity,
        line_items.variant_id,
        CAST(fulfillment_orders.fulfill_at as timestamp) fulfill_at,
        delivery_method.id as delivery_method_id,
        delivery_method.method_type,
        assigned_location.address1 as assigned_location_address1,
        assigned_location.address2 as assigned_location_address2,
        assigned_location.city assigned_location_city,
        assigned_location.country_code,
        assigned_location.location_id as assigned_location_location_id,
        assigned_location.name as assigned_location_name,
        assigned_location.phone as assigned_location_phone,
        assigned_location.province as assigned_location_province,
        assigned_location.zip as assigned_location_zip,
        {% endif %}
        number,
        note,
        token,
        gateway,
        test,
        total_price,
        subtotal_price,
        total_weight,
        total_tax,
        taxes_included,
        currency,
        financial_status,
        confirmed,
        total_discounts,
        total_line_items_price,
        cart_token,
        buyer_accepts_marketing,
        a.name,
        referring_site,
        landing_site,
        cancelled_at,
        cancel_reason,
        total_price_usd,
        checkout_token,
        reference,
        user_id,
        a.location_id,
        source_identifier,
        source_url,
        CAST(a.processed_at as timestamp) processed_at,
        device_id,
        a.phone,
        customer_locale,
        app_id,
        browser_ip,
        landing_site_ref,
        order_number,
        payment_gateway_names,
        processing_method,
        checkout_id,
        source_name,
        fulfillment_status,
        tags,
        contact_email,
        order_status_url,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
                {{unnesting("fulfillment_orders")}}
                {{multi_unnesting("fulfillment_orders","destination")}}
                {{multi_unnesting("fulfillment_orders","line_items")}}
                {{multi_unnesting("fulfillment_orders","delivery_method")}}
                {{multi_unnesting("fulfillment_orders","assigned_location")}}
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}

),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY order_id order by _daton_batch_runtime desc) row_num
from unnested_fulfillment_orders 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
