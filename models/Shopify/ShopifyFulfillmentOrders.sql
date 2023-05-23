{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
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

{% set table_name_query %}
{{set_table_name('%shopify%fulfillment_orders%')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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

    select * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        a.id,
        -- id as order_id,
        a.shop_id,
        order_id,
        -- COALESCE(order_id,'') as fulfillment_orders_order_id, (fulfillment_order_id already in lineitems)
        assigned_location_id,
        request_status,
        status,
        supported_actions,
        {% if target.type =='snowflake' %}
        destination.VALUE:id::VARCHAR as destination_id,
        destination.VALUE:address1 as destination_address1,
        destination.VALUE:address2 as destination_address2,
        destination.VALUE:city as destination_city,
        destination.VALUE:country as destination_country,
        destination.VALUE:email::VARCHAR as destination_email,
        destination.VALUE:first_name as destination_first_name,
        destination.VALUE:last_name as destination_last_name,
        destination.VALUE:phone::VARCHAR as destination_phone,
        destination.VALUE:province as destination_province,
        destination.VALUE:zip as destination_zip,
        destination.VALUE:company as destination_company,
        COALESCE(line_items.VALUE:id::VARCHAR,'') as line_items_id,
        line_items.VALUE:shop_id::VARCHAR as line_items_shop_id,
        line_items.VALUE:fulfillment_order_id, as line_items_fulfillment_order_id
        line_items.VALUE:quantity as line_items_quantity,
        line_items.VALUE:line_item_id as line_items_line_item_id,
        line_items.VALUE:inventory_item_id as line_items_inventory_item_id,
        line_items.VALUE:fulfillable_quantity as line_items_fulfillable_quantity,
        line_items.VALUE:variant_id as line_items_variant_id,
        {% else %}
        destination.id as destination_id,
        destination.address1 as destination_address1,
        destination.address2 as destination_address2,
        destination.city as destination_city,
        destination.country as destination_country,
        destination.email as destination_email,
        destination.first_name as destination_first_name,
        destination.last_name as destination_last_name,
        destination.phone as destination_phone,
        destination.province as destination_province,
        destination.zip as destination_zip,
        destination.company as destination_company,
        COALESCE(CAST(line_items.id as string),'') as line_items_id,
        line_items.shop_id as line_items_shop_id,
        line_items.fulfillment_order_id as line_items_fulfillment_order_id,
        line_items.quantity as line_items_quantity,
        line_items.line_item_id line_items_line_item_id,
        line_items.inventory_item_id as line_items_inventory_item_id,
        line_items.fulfillable_quantity as line_items_fulfillable_quantity,
        line_items.variant_id as line_items_variant_id,
        {% endif %}
        fulfillment_service_handle,
        {% if target.type =='snowflake' %}
        assigned_location.VALUE:country_code,
        assigned_location.VALUE:location_id::VARCHAR as assigned_location_location_id,
        assigned_location.VALUE:name::VARCHAR as assigned_location_name,
        assigned_location.VALUE:address1::VARCHAR as assigned_location_address1,
        assigned_location.VALUE:address2::VARCHAR as assigned_location_address2,
        assigned_location.VALUE:city as assigned_location_city,
        assigned_location.VALUE:phone::VARCHAR as assigned_location_phone,
        assigned_location.VALUE:province::VARCHAR as assigned_location_province,
        assigned_location.VALUE:zip::VARCHAR as assigned_location_zip,
        delivery_method.VALUE:id::VARCHAR as delivery_method_id,
        delivery_method.VALUE:method_type as delivery_method_method_type,
        {% else %}
        assigned_location.country_code,
        assigned_location.location_id as assigned_location_location_id,
        assigned_location.name as assigned_location_name,
        assigned_location.address1 as assigned_location_address1,
        assigned_location.address2 as assigned_location_address2,
        assigned_location.city assigned_location_city,
        assigned_location.phone as assigned_location_phone,
        assigned_location.province as assigned_location_province,
        assigned_location.zip as assigned_location_zip,
        delivery_method.id as delivery_method_id,
        delivery_method.method_type as delivery_method_method_type,
        {% endif %}
        fulfill_at,
        created_at,
        updated_at,
        merchant_requests,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY a.id order by _daton_batch_runtime desc) row_num
        from {{i}} a
                {{unnesting("destination")}}
                {{unnesting("line_items")}}
                {{unnesting("assigned_location")}}
                {{unnesting("delivery_method")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}

