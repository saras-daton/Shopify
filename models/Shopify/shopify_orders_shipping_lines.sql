{% if var('shopify_orders') %}
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
{{set_table_name('%shopify%orders')}}    
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
        cast(a.id as string) as order_id, 
        email,
        closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) as updated_at,
        fulfillment_orders,
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
        CAST(a.processed_at as timestamp) as processed_at,
        device_id,
        a.phone,
        customer_locale,
        app_id,
        browser_ip,
        landing_site_ref,
        order_number,
        {% if target.type =='snowflake' %}
        discount_codes.VALUE:code::VARCHAR as discount_code,
        discount_codes.VALUE:amount::NUMERIC as discount_amount,
        discount_codes.VALUE:type::VARCHAR as discount_type,
        shipping_lines.VALUE:id::VARCHAR as shipping_lines_id,
        shipping_lines.VALUE:title::VARCHAR as shipping_lines_title,
        shipping_lines.VALUE:price::VARCHAR as price,
        shipping_lines.VALUE:code::VARCHAR as code,
        shipping_lines.VALUE:source::VARCHAR as source,
        shipping_lines.VALUE:phone::VARCHAR as shipping_lines_phone,
        shipping_lines.VALUE:requested_fulfillment_service_id::VARCHAR as requested_fulfillment_service_id,
        shipping_lines.VALUE:delivery_category::VARCHAR as delivery_category,
        shipping_lines.VALUE:carrier_identifier::VARCHAR as carrier_identifier,
        shipping_lines.VALUE:discounted_price::FLOAT as discounted_price,
        {% else %}
        discount_codes.code as discount_code,
        discount_codes.amount as discount_amount,
        discount_codes.type as discount_type,
        COALESCE(CAST(shipping_lines.id as string),'') as shipping_lines_id,
        shipping_lines.title as shipping_lines_title,
        shipping_lines.price as price,
        shipping_lines.code as code,
        shipping_lines.source as source,
        shipping_lines.phone as shipping_lines_phone,
        shipping_lines.requested_fulfillment_service_id as requested_fulfillment_service_id,
        shipping_lines.delivery_category as delivery_category,
        shipping_lines.carrier_identifier as carrier_identifier,
        shipping_lines.discounted_price as discounted_price,
        {% endif %}
        note_attributes,
        payment_gateway_names,
        processing_method,
        checkout_id,
        source_name,
        a.fulfillment_status,
        a.tax_lines,
        tags,
        contact_email,
        order_status_url,
        line_items,
        shipping_lines,
        billing_address,
        shipping_address,
        fulfillments,
        client_details,
        refunds,
        payment_details,
        customer,
        transactions,

        {% if var('currency_conversion_flag') %}
            case when b.value is null then 1 else b.value end as exchange_currency_rate,
            case when b.from_currency_code is null then currency else b.from_currency_code end as exchange_currency_code,
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
                    left join {{ref('ExchangeRates')}} b on date(created_at) = b.date and currency = b.to_currency_code
                {% endif %}
                {{unnesting("discount_codes")}}
                {{unnesting("shipping_lines")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
