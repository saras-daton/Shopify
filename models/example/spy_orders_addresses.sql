{% if var('spy_orders_addresses') %}
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


    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        admin_graphql_api_id,
        coalesce(cast(a.id as string),'') as order_id, 
        coalesce(email,'') as email,
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
        location_id,
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
        BILLING_ADDRESS.VALUE:company::VARCHAR as billing_address_company,
        BILLING_ADDRESS.VALUE:latitude::VARCHAR as billing_address_latitude,
        BILLING_ADDRESS.VALUE:longitude::VARCHAR as billing_address_longitude,
        BILLING_ADDRESS.VALUE:name::VARCHAR as billing_address_name,
        BILLING_ADDRESS.VALUE:country_code::VARCHAR as billing_address_country_code,
        BILLING_ADDRESS.VALUE:province_code::VARCHAR as billing_address_provinceas_code,
        shipping_address.VALUE:first_name::VARCHAR as shipping_address_first_name,
        SHIPPING_ADDRESS.VALUE:address1::VARCHAR as shipping_address_address1,
        SHIPPING_ADDRESS.VALUE:phone::VARCHAR as shipping_address_phone,
        SHIPPING_ADDRESS.VALUE:city::VARCHAR as shipping_address_city,
        SHIPPING_ADDRESS.VALUE:zip::VARCHAR as shipping_address_zip,
        SHIPPING_ADDRESS.VALUE:province::VARCHAR as shipping_address_province,
        SHIPPING_ADDRESS.VALUE:country::VARCHAR as shipping_address_country,
        SHIPPING_ADDRESS.VALUE:last_name::VARCHAR as shipping_address_last_name,
        SHIPPING_ADDRESS.VALUE:address2::VARCHAR as shipping_address_address2,
        SHIPPING_ADDRESS.VALUE:company::VARCHAR as shipping_address_company,
        SHIPPING_ADDRESS.VALUE:latitude::VARCHAR as shipping_address_latitude,
        SHIPPING_ADDRESS.VALUE:longitude::VARCHAR as shipping_address_longitude,
        SHIPPING_ADDRESS.VALUE:name::VARCHAR as shipping_address_name,
        SHIPPING_ADDRESS.VALUE:country_code::VARCHAR as shipping_address_country_code,
        SHIPPING_ADDRESS.VALUE:province_code::VARCHAR as shipping_address_province_code,
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
        BILLING_ADDRESS.company as billing_address_company,
        BILLING_ADDRESS.latitude as billing_address_latitude,
        BILLING_ADDRESS.longitude as billing_address_longitude,
        BILLING_ADDRESS.name as billing_address_name,
        BILLING_ADDRESS.country_code as billing_address_country_code,
        BILLING_ADDRESS.province_code as billing_address_province_code,
        SHIPPING_ADDRESS.first_name as shipping_address_first_name,
        SHIPPING_ADDRESS.address1 as shipping_address_address1,
        SHIPPING_ADDRESS.phone as shipping_address_phone,
        SHIPPING_ADDRESS.city as shipping_address_city,
        SHIPPING_ADDRESS.zip as shipping_address_zip,
        SHIPPING_ADDRESS.province as shipping_address_province,
        SHIPPING_ADDRESS.country as shipping_address_country,
        SHIPPING_ADDRESS.last_name as shipping_address_last_name,
        SHIPPING_ADDRESS.address2 as shipping_address_address2,
        SHIPPING_ADDRESS.company as shipping_address_company,
        SHIPPING_ADDRESS.latitude as shipping_address_latitude,
        SHIPPING_ADDRESS.longitude as shipping_address_longitude,
        SHIPPING_ADDRESS.name as shipping_address_name,
        SHIPPING_ADDRESS.country_code as shipping_address_country_code,
        SHIPPING_ADDRESS.province_code as shipping_address_province_code,
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        Dense_Rank() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) row_num
            from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {{unnesting("BILLING_ADDRESS")}} 
                {{unnesting("SHIPPING_ADDRESS")}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
