{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_fulfillment_orders_tbl_ptrn","shopify_fulfillment_orders_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(a.id as string) as id,
        cast(a.shop_id as string) as shop_id,
        cast(order_id as string) as order_id,
        cast(assigned_location_id as string) as assigned_location_id,
        request_status,
        status,
        supported_actions,
        {% if target.type=='snowflake' %}
        cast(destination.value:id as string) as destination_id,
        {% else %}
        cast(destination.id as string) as destination_id, {% endif %}
        {{extract_nested_value("destination","address1","string")}} as destination_address1,
        {{extract_nested_value("destination","address2","string")}} as destination_address2,
        {{extract_nested_value("destination","city","string")}} as destination_city,
        {{extract_nested_value("destination","country","string")}} as destination_country,
        {{extract_nested_value("destination","email","string")}} as destination_email,
        {{extract_nested_value("destination","first_name","string")}} as destination_first_name,
        {{extract_nested_value("destination","last_name","string")}} as destination_last_name,
        {{extract_nested_value("destination","phone","string")}} as destination_phone,
        {{extract_nested_value("destination","province","string")}} as destination_province,
        {{extract_nested_value("destination","zip","string")}} as destination_zip,
        {{extract_nested_value("destination","company","string")}} as destination_company,
        {% if target.type=='snowflake' %}
        cast(line_items.value:id as string) as line_items_id,
        coalesce(cast(line_items.value:shop_id as string), 'N/A') as line_items_shop_id,
        {% else %}
        cast(line_items.id as string) as line_items_id,
        coalesce(cast(line_items.shop_id as string), 'N/A') as line_items_shop_id,
        {% endif %}
        {{extract_nested_value("line_items","fulfillment_order_id","string")}} as line_items_fulfillment_order_id,
        {{extract_nested_value("line_items","quantity","int")}} as line_items_quantity,
        {{extract_nested_value("line_items","line_item_id","string")}} as line_items_line_item_id,
        {{extract_nested_value("line_items","inventory_item_id","string")}} as line_items_inventory_item_id,
        {{extract_nested_value("line_items","fulfillable_quantity","string")}} as line_items_fulfillable_quantity,
        {{extract_nested_value("line_items","variant_id","string")}} as line_items_variant_id,
        fulfillment_service_handle,
        {{extract_nested_value("assigned_location","country_code","string")}} as assigned_location_country_code,
        {{extract_nested_value("assigned_location","location_id","string")}} as assigned_location_location_id,
        {{extract_nested_value("assigned_location","name","string")}} as assigned_location_name,
        {{extract_nested_value("assigned_location","address1","string")}} as assigned_location_address1,
        {{extract_nested_value("assigned_location","address2","string")}} as assigned_location_address2,
        {{extract_nested_value("assigned_location","city","string")}} as assigned_location_city,
        {{extract_nested_value("assigned_location","phone","string")}} as assigned_location_phone,
        {{extract_nested_value("assigned_location","province","string")}} as assigned_location_province,
        {{extract_nested_value("assigned_location","zip","string")}} as assigned_location_zip,
        {{extract_nested_value("delivery_method","id","string")}} as delivery_method_id,
        {{extract_nested_value("delivery_method","method_type","string")}} as delivery_method_method_type,
        {{timezone_conversion("fulfill_at")}} as fulfill_at,
        {{timezone_conversion("fulfill_at")}} as created_at,
        {{timezone_conversion("fulfill_at")}} as updated_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {{unnesting("destination")}}
        {{unnesting("line_items")}}
        {{unnesting("assigned_location")}}
        {{unnesting("delivery_method")}}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_fulfillment_orders_lookback') }},0) from {{ this }})
        {% endif %}               
        qualify row_number() over (partition by a.id order by _daton_batch_runtime desc) = 1
        
        {% if not loop.last %} union all {% endif %}
{% endfor %}
