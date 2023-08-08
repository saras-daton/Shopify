{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(a.id as string) as id,
        cast(a.shop_id as string) as shop_id,
        cast(order_id as string) as order_id,
        cast(assigned_location_id as string) as assigned_location_id,
        request_status,
        status,
        supported_actions,
        {% if target.type =='snowflake' %}
        cast(destination.value:id as string) as destination_id,
        destination.value:address1 as destination_address1,
        destination.value:address2 as destination_address2,
        destination.value:city as destination_city,
        destination.value:country as destination_country,
        destination.value:email::string as destination_email,
        destination.value:first_name as destination_first_name,
        destination.value:last_name as destination_last_name,
        destination.value:phone::string as destination_phone,
        destination.value:province as destination_province,
        destination.value:zip as destination_zip,
        destination.value:company as destination_company,
        coalesce(line_items.value:id::string,'N/A') as line_items_id,
        line_items.value:shop_id::string as line_items_shop_id,
        line_items.value:fulfillment_order_id, as line_items_fulfillment_order_id
        cast(line_items.value:quantity as int) as line_items_quantity,
        line_items.value:line_item_id as line_items_line_item_id,
        line_items.value:inventory_item_id as line_items_inventory_item_id,
        line_items.value:fulfillable_quantity as line_items_fulfillable_quantity,
        line_items.value:variant_id as line_items_variant_id,
        {% else %}
        cast(destination.id as string) as destination_id,
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
        coalesce(cast(line_items.id as string),'N/A') as line_items_id,
        cast(line_items.shop_id as string) as line_items_shop_id,
        cast(line_items.fulfillment_order_id as string) as line_items_fulfillment_order_id,
        cast(line_items.quantity as int) as line_items_quantity,
        cast(line_items.line_item_id as string) as line_items_line_item_id,
        cast(line_items.inventory_item_id as string) asline_items_inventory_item_id,
        cast(line_items.fulfillable_quantity as int) as line_items_fulfillable_quantity,
        cast(line_items.variant_id as string) as line_items_variant_id,
        {% endif %}
        fulfillment_service_handle,
        {% if target.type =='snowflake' %}
        assigned_location.value:country_code,
        assigned_location.value:location_id::string as assigned_location_location_id,
        assigned_location.value:name::string as assigned_location_name,
        assigned_location.value:address1::string as assigned_location_address1,
        assigned_location.value:address2::string as assigned_location_address2,
        assigned_location.value:city as assigned_location_city,
        assigned_location.value:phone::string as assigned_location_phone,
        assigned_location.value:province::string as assigned_location_province,
        assigned_location.value:zip::string as assigned_location_zip,
        delivery_method.value:id::string as delivery_method_id,
        delivery_method.value:method_type as delivery_method_method_type,
        {% else %}
        assigned_location.country_code,
        cast(assigned_location.location_id as string) as assigned_location_location_id,
        assigned_location.name as assigned_location_name,
        assigned_location.address1 as assigned_location_address1,
        assigned_location.address2 as assigned_location_address2,
        assigned_location.city assigned_location_city,
        assigned_location.phone as assigned_location_phone,
        assigned_location.province as assigned_location_province,
        assigned_location.zip as assigned_location_zip,
        cast(delivery_method.id as string) as delivery_method_id,
        delivery_method.method_type as delivery_method_method_type,
        {% endif %}
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfill_at") }} as {{ dbt.type_timestamp() }}) as fulfill_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}} a
                {{unnesting("destination")}}
                {{unnesting("line_items")}}
                {{unnesting("assigned_location")}}
                {{unnesting("delivery_method")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}                
        qualify row_number() over (partition by a.id order by _daton_batch_runtime desc) row_num = 1
        
    {% if not loop.last %} union all {% endif %}
{% endfor %}

