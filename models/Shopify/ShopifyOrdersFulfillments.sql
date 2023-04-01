{% if var('ShopifyOrdersFulfillments') %}
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

with unnested_fulfillments as(
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
        cast(a.id as string) order_id, 
        email,
        closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
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
        phone,
        customer_locale,
        app_id,
        browser_ip,
        landing_site_ref,
        order_number,
        payment_gateway_names,
        processing_method,
        checkout_id,
        source_name,
        a.fulfillment_status as order_fulfillment_status,
        tags,
        contact_email,
        order_status_url,
        {% if target.type =='snowflake' %}
        COALESCE(fulfillments.VALUE:id::VARCHAR,'') as fulfillments_id,
        fulfillments.VALUE:order_id::VARCHAR as fulfillments_orders_id,
        fulfillments.VALUE:status as fulfillments_status,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(fulfillments.VALUE:created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as fulfillments_created_at,
        fulfillments.VALUE:service as fulfillments_service,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(fulfillments.VALUE:updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as fulfillments_updated_at,
        fulfillments.VALUE:tracking_company as fulfillments_tracking_company,
        fulfillments.VALUE:shipment_status as fulfillments_shipment_status,
        fulfillments.VALUE:tracking_number as fulfillments_tracking_number,
        fulfillments.VALUE:tracking_numbers as fulfillments_tracking_numbers,
        fulfillments.VALUE:tracking_url as fulfillments_tracking_url,
        fulfillments.VALUE:tracking_urls as fulfillments_tracking_urls,
        receipt.VALUE:testcase as receipt_testcase,
        receipt.VALUE:authorization as receipt_authorization,
        COALESCE(fulfillments_line_items.VALUE:id::VARCHAR,'') as fulfillments_line_items_id,
        fulfillments_line_items.VALUE:variant_id as fulfillments_line_items_variant_id,
        fulfillments_line_items.VALUE:title::VARCHAR as fulfillments_line_items_title,
        fulfillments_line_items.VALUE:quantity as fulfillments_line_items_quantity,
        fulfillments_line_items.VALUE:price::VARCHAR as fulfillments_line_items_price,
        fulfillments_line_items.VALUE:sku as fulfillments_line_items_sku,
        fulfillments_line_items.VALUE:variant_title as fulfillments_line_items_variant_title,
        fulfillments_line_items.VALUE:vendor as fulfillments_line_items_vendor,
        fulfillments_line_items.VALUE:fulfillment_service as fulfillment_service,
        fulfillments_line_items.VALUE:product_id as fulfillments_line_items_product_id,
        fulfillments_line_items.VALUE:requires_shipping as requires_shipping,
        fulfillments_line_items.VALUE:taxable as fulfillments_line_items_taxable,
        fulfillments_line_items.VALUE:gift_card as fulfillments_line_items_gift_card,
        fulfillments_line_items.VALUE:name::VARCHAR as fulfillments_line_items_name,
        fulfillments_line_items.VALUE:variant_inventory_management,
        fulfillments_line_items.VALUE:properties as fulfillments_line_items_properties,
        fulfillments_line_items.VALUE:product_exists as fulfillments_line_items_product_exists,
        fulfillments_line_items.VALUE:fulfillable_quantity,
        fulfillments_line_items.VALUE:grams as fulfillments_line_items_grams,
        fulfillments_line_items.VALUE:total_discount as fulfillments_line_items_total_discount,
        fulfillments_line_items.VALUE:fulfillment_status as fulfillments_line_items_status,
        fulfillments_line_items.VALUE:tax_lines as fulfillments_line_items_tax_lines,
        fulfillments_line_items.VALUE:origin_location as fulfillments_line_items_origin_location,
        fulfillments.VALUE:location_id::VARCHAR as fulfillments_location_id,
        {% else %}
        COALESCE(CAST(fulfillments.id as string),'') as fulfillments_id,
        fulfillments.order_id as fulfillments_orders_id,
        fulfillments.status as fulfillments_status,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(fulfillments.created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as fulfillments_created_at,
        fulfillments.service as fulfillments_service,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(fulfillments.updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as fulfillments_updated_at,
        fulfillments.tracking_company as fulfillments_tracking_company,
        fulfillments.shipment_status as fulfillments_shipment_status,
        fulfillments.tracking_number as fulfillments_tracking_number,
        fulfillments.tracking_numbers as fulfillments_tracking_numbers,
        fulfillments.tracking_url as fulfillments_tracking_url,
        fulfillments.tracking_urls as fulfillments_tracking_urls,
        receipt.testcase as receipt_testcase,
        receipt.authorization as receipt_authorization,
        COALESCE(CAST(fulfillments_line_items.id as string),'') as fulfillments_line_items_id,
        fulfillments_line_items.variant_id as fulfillments_line_items_variant_id,
        fulfillments_line_items.title as fulfillments_line_items_title,
        fulfillments_line_items.quantity as fulfillments_line_items_quantity,
        fulfillments_line_items.price as fulfillments_line_items_price,
        fulfillments_line_items.sku as fulfillments_line_items_sku,
        fulfillments_line_items.variant_title as fulfillments_line_items_variant_title,
        fulfillments_line_items.vendor as fulfillments_line_items_vendor,
        fulfillments_line_items.fulfillment_service as fulfillment_service,
        fulfillments_line_items.product_id as fulfillments_line_items_product_id,
        fulfillments_line_items.requires_shipping as requires_shipping,
        fulfillments_line_items.taxable as fulfillments_line_items_taxable,
        fulfillments_line_items.gift_card as fulfillments_line_items_gift_card,
        fulfillments_line_items.name as fulfillments_line_items_name,
        fulfillments_line_items.variant_inventory_management as variant_inventory_management,
        fulfillments_line_items.properties as fulfillments_line_items_properties,
        fulfillments_line_items.product_exists as fulfillments_line_items_product_exists,
        fulfillments_line_items.fulfillable_quantity as fulfillable_quantity,
        fulfillments_line_items.grams as fulfillments_line_items_grams,
        fulfillments_line_items.total_discount as fulfillments_line_items_total_discount,
        fulfillments_line_items.fulfillment_status as fulfillments_line_items_status,
        fulfillments_line_items.tax_lines as fulfillments_line_items_tax_lines,
        fulfillments_line_items.origin_location as fulfillments_line_items_origin_location,
        fulfillments.location_id as fulfillments_location_id,
        {% endif %}
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
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{unnesting("FULFILLMENTS")}}
            {{multi_unnesting("FULFILLMENTS","RECEIPT")}}
            {% if target.type =='snowflake' %}
            , LATERAL FLATTEN( input => PARSE_JSON(fulfillments.VALUE:"line_items")) as fulfillments_line_items
            {% else %}
            left join unnest(fulfillments.line_items) as fulfillments_line_items
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
from unnested_fulfillments 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
