{% if var('ShopifyOrdersLineItems') %}
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

{% set table_name_query %}
{{set_table_name('%shopify%orders')}} and lower(table_name) not like '%shopify%fulfillment_orders' and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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

    -- {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
    --     {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    -- {% else %}
    --     {% set hr = 0 %}
    -- {% endif %}

    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'') as order_id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        currency,
        current_subtotal_price,
        current_subtotal_price_set,
        current_total_discounts,
        current_total_discounts_set,
        current_total_price,
        current_total_price_set,
        current_total_tax,
        current_total_tax_set,
        discount_codes,
        coalesce(email,'') as email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        a.name,
        note_attributes,
        number,
        order_number,
        order_status_url,
        payment_gateway_names,
        a.phone,
        presentment_currency,
        CAST(a.processed_at as timestamp) processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        subtotal_price,
        subtotal_price_set,
        tags,
        a.tax_lines,
        taxes_included,
        test,
        token,
        total_discounts,
        total_discounts_set,
        total_line_items_price,
        total_line_items_price_set,
        total_outstanding,
        total_price,
        total_price_set,
        total_price_usd,
        total_shipping_price_set,
        total_tax,
        total_tax_set,
        total_tip_received,
        total_weight,
        CAST(a.updated_at as timestamp) updated_at,
        {% if target.type =='snowflake' %}
        BILLING_ADDRESS.VALUE:first_name::VARCHAR as billing_address_first_name,
        BILLING_ADDRESS.VALUE:address1::VARCHAR as billing_address_address1,
        BILLING_ADDRESS.VALUE:phone::VARCHAR as billing_address_phone,
        BILLING_ADDRESS.VALUE:city::VARCHAR as billing_address_city,
        BILLING_ADDRESS.VALUE:zip::VARCHAR as billing_address_zip,
        BILLING_ADDRESS.VALUE:province::VARCHAR as billing_address_province,
        BILLING_ADDRESS.VALUE:country::VARCHAR as billing_address_country,
        BILLING_ADDRESS.VALUE:last_name::VARCHAR as billing_address_last_name,
        BILLING_ADDRESS.VALUE:address2::VARCHAR as billing_address_address2,
        BILLING_ADDRESS.VALUE:latitude::VARCHAR as billing_address_latitude,
        BILLING_ADDRESS.VALUE:longitude::VARCHAR as billing_address_longitude,
        BILLING_ADDRESS.VALUE:name::VARCHAR as billing_address_name,
        BILLING_ADDRESS.VALUE:country_code::VARCHAR as billing_address_country_code,
        BILLING_ADDRESS.VALUE:province_code::VARCHAR as billing_address_provinceas_code,
        BILLING_ADDRESS.VALUE:longitude_bn as billing_address_longitude_bn,
        BILLING_ADDRESS.VALUE:latitude_bn as billing_address_latitude_bn,
        BILLING_ADDRESS.VALUE:company::VARCHAR as billing_address_company,
        {% else %}
        BILLING_ADDRESS.first_name as billing_address_first_name,
        BILLING_ADDRESS.address1 as billing_address_address1,
        BILLING_ADDRESS.phone as billing_address_phone,
        BILLING_ADDRESS.city as billing_address_city,
        BILLING_ADDRESS.zip as billing_address_zip,
        BILLING_ADDRESS.province as billing_address_province,
        BILLING_ADDRESS.country as billing_address_country,
        BILLING_ADDRESS.last_name as billing_address_last_name,
        BILLING_ADDRESS.address2 as billing_address_address2,
        BILLING_ADDRESS.latitude as billing_address_latitude,
        BILLING_ADDRESS.longitude as billing_address_longitude,
        BILLING_ADDRESS.name as billing_address_name,
        BILLING_ADDRESS.country_code as billing_address_country_code,
        BILLING_ADDRESS.province_code as billing_address_province_code,
        BILLING_ADDRESS.longitude_bn as billing_address_longitude_bn,
        BILLING_ADDRESS.latitude_bn as billing_address_latitude_bn,
        BILLING_ADDRESS.company as billing_address_company,
        {% endif %}
        customer,
        discount_applications,
        fulfillments,
        {% if target.type =='snowflake' %}
        line_items.VALUE:id::VARCHAR as line_items_id,
        line_items.VALUE:admin_graphql_api_id::VARCHAR as line_items_admin_graphql_api_id,
        line_items.VALUE:fulfillable_quantity::VARCHAR as line_items_fulfillable_quantity,
        line_items.VALUE:fulfillment_service::VARCHAR as line_items_fulfillment_service,
        line_items.VALUE:gift_card::VARCHAR as line_items_gift_card,
        line_items.VALUE:grams::VARCHAR as line_items_grams, 
        line_items.VALUE:name::VARCHAR as line_items_name,
        line_items.VALUE:price::FLOAT as line_items_price,
        line_items.VALUE:price_set as line_items_price_set,
        line_items.VALUE:product_exists::VARCHAR as line_items_product_exists,
        line_items.VALUE:product_id::VARCHAR as line_items_product_id,
        line_items.VALUE:properties::VARCHAR as line_items_properties,
        line_items.VALUE:quantity::FLOAT as line_items_quantity,
        line_items.VALUE:requires_shipping::VARCHAR as line_items_requires_shipping,
        line_items.VALUE:sku::VARCHAR as line_items_sku,
        line_items.VALUE:taxable::VARCHAR as line_items_taxable,
        line_items.VALUE:title::VARCHAR as line_items_title,
        line_items.VALUE:total_discount::numeric as line_items_total_discount,
        line_items.VALUE:total_discount_set as line_items_total_discount_set,
        line_items.VALUE:variant_id::VARCHAR as line_items_variant_id,
        line_items.VALUE:variant_inventory_management::VARCHAR as line_items_variant_inventory_management,
        line_items.VALUE:variant_title::VARCHAR as line_items_variant_title,
        line_items.VALUE:tax_lines as line_items_tax_lines,
        line_items.VALUE:discount_allocations as line_items_discount_allocations,
        line_items.VALUE:pre_tax_price_set as line_items_pre_tax_price_set,
        line_items.VALUE:pre_tax_price as line_items_pre_tax_price,
        line_items.VALUE:tax_code as line_items_tax_code,
        line_items.VALUE:vendor::VARCHAR as vendor,
        line_items.VALUE:fulfillment_status::VARCHAR as line_items_fulfillment_status,
        {% else %}
        line_items.id as line_items_id,
        line_items.admin_graphql_api_id as line_items_admin_graphql_api_id,
        line_items.fulfillable_quantity as line_items_fulfillable_quantity,
        line_items.fulfillment_service as line_items_fulfillment_service,
        line_items.gift_card as line_items_gift_card,
        line_items.grams as line_items_grams, 
        line_items.name as line_items_name,
        cast(line_items.price as numeric) line_items_price,
        line_items.price_set as line_items_price_set,
        line_items.product_exists as line_items_product_exists,
        line_items.product_id as line_items_product_id,
        line_items.properties as line_items_properties,
        line_items.quantity as line_items_quantity,
        line_items.requires_shipping as line_items_requires_shipping,
        line_items.sku as line_items_sku,
        line_items.taxable as line_items_taxable,
        line_items.title as line_items_title,
        cast(line_items.total_discount as numeric) line_items_total_discount,
        line_items.total_discount_set as line_items_total_discount_set,
        line_items.variant_id as line_items_variant_id,
        line_items.variant_inventory_management as line_items_variant_inventory_management,
        line_items.variant_title as line_items_variant_title,
        line_items.tax_lines as line_items_tax_lines,
        line_items.discount_allocations as line_items_discount_allocations,
        line_items.pre_tax_price_set as line_items_pre_tax_price_set,
        line_items.pre_tax_price as line_items_pre_tax_price,
        line_items.tax_code as line_items_tax_code,
        line_items.vendor as vendor,
        line_items.fulfillment_status as line_items_fulfillment_status,
        {% endif %}
        payment_details,
        refunds,
        {% if target.type =='snowflake' %}
        SHIPPING_ADDRESS.VALUE:first_name::VARCHAR as shipping_address_first_name,
        SHIPPING_ADDRESS.VALUE:address1::VARCHAR as shipping_address_address1,
        SHIPPING_ADDRESS.VALUE:phone::VARCHAR as shipping_address_phone,
        SHIPPING_ADDRESS.VALUE:city::VARCHAR as shipping_address_city,
        SHIPPING_ADDRESS.VALUE:zip::VARCHAR as shipping_address_zip,
        SHIPPING_ADDRESS.VALUE:province::VARCHAR as shipping_address_province,
        SHIPPING_ADDRESS.VALUE:country::VARCHAR as shipping_address_country,
        SHIPPING_ADDRESS.VALUE:last_name::VARCHAR as shipping_address_last_name,
        SHIPPING_ADDRESS.VALUE:address2::VARCHAR as shipping_address_address2,
        SHIPPING_ADDRESS.VALUE:latitude::VARCHAR as shipping_address_latitude,
        SHIPPING_ADDRESS.VALUE:longitude::VARCHAR as shipping_address_longitude,
        SHIPPING_ADDRESS.VALUE:name::VARCHAR as shipping_address_name,
        SHIPPING_ADDRESS.VALUE:country_code::VARCHAR as shipping_address_country_code,
        SHIPPING_ADDRESS.VALUE:province_code::VARCHAR as shipping_address_province_code,
        SHIPPING_ADDRESS.VALUE:latitude_bn as shipping_address_latitude_bn,
        SHIPPING_ADDRESS.VALUE:longitude_bn as shipping_address_longitude_bn,
        SHIPPING_ADDRESS.VALUE:company::VARCHAR as shipping_address_company,
        {% else %}
        SHIPPING_ADDRESS.first_name as shipping_address_first_name,
        SHIPPING_ADDRESS.address1 as shipping_address_address1,
        SHIPPING_ADDRESS.phone as shipping_address_phone,
        SHIPPING_ADDRESS.city as shipping_address_city,
        SHIPPING_ADDRESS.zip as shipping_address_zip,
        SHIPPING_ADDRESS.province as shipping_address_province,
        SHIPPING_ADDRESS.country as shipping_address_country,
        SHIPPING_ADDRESS.last_name as shipping_address_last_name,
        SHIPPING_ADDRESS.address2 as shipping_address_address2,
        SHIPPING_ADDRESS.latitude as shipping_address_latitude,
        SHIPPING_ADDRESS.longitude as shipping_address_longitude,
        SHIPPING_ADDRESS.name as shipping_address_name,
        SHIPPING_ADDRESS.country_code as shipping_address_country_code,
        SHIPPING_ADDRESS.province_code as shipping_address_province_code,
        SHIPPING_ADDRESS.latitude_bn as shipping_address_latitude_bn,
        SHIPPING_ADDRESS.longitude_bn as shipping_address_longitude_bn,
        SHIPPING_ADDRESS.company as shipping_address_company,
        {% endif %}
        shipping_lines,
        app_id,
        customer_locale,
        note,
        closed_at,
        a.fulfillment_status,
        location_id,
        cancel_reason,
        cancelled_at,
        user_id,
        device_id,
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        Dense_Rank() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {{unnesting("line_items")}}
                {{unnesting("billing_address")}}
                {{unnesting("shipping_address")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
