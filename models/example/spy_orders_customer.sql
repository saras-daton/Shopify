{% if var('spy_orders_customer') %}
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

with unnested_customers as(
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
        a.email,
        closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
        number,
        a.note,
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
        a.tags,
        contact_email,
        order_status_url,
        {% if target.type =='snowflake' %}
        CUSTOMER.VALUE:id::VARCHAR as customer_id,
        CUSTOMER.VALUE:email::VARCHAR as customer_email,
        CUSTOMER.VALUE:accepts_marketing,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(CUSTOMER.VALUE:created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as customer_created_at,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(CUSTOMER.VALUE:updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as customer_updated_at,
        CUSTOMER.VALUE:first_name::VARCHAR as customer_first_name,
        CUSTOMER.VALUE:last_name::VARCHAR as customer_last_name,
        CUSTOMER.VALUE:orders_count,
        CUSTOMER.VALUE:state,
        CUSTOMER.VALUE:total_spent,
        CUSTOMER.VALUE:last_order_id,
        CUSTOMER.VALUE:note::VARCHAR as customer_note,
        CUSTOMER.VALUE:verified_email,
        CUSTOMER.VALUE:multipass_identifier,
        CUSTOMER.VALUE:tax_exempt,
        CUSTOMER.VALUE:phone::VARCHAR as customer_phone,
        CUSTOMER.VALUE:tags::VARCHAR as customer_tags,
        CUSTOMER.VALUE:last_order_name,
        DEFAULT_ADDRESS.VALUE:id::VARCHAR as default_address_id,
        DEFAULT_ADDRESS.VALUE:customer_id::VARCHAR as default_address_customer_id,
        DEFAULT_ADDRESS.VALUE:first_name::VARCHAR as default_address_first_name,
        DEFAULT_ADDRESS.VALUE:last_name::VARCHAR as default_address_last_name,
        DEFAULT_ADDRESS.VALUE:company,
        DEFAULT_ADDRESS.VALUE:address1,
        DEFAULT_ADDRESS.VALUE:address2,
        DEFAULT_ADDRESS.VALUE:city,
        DEFAULT_ADDRESS.VALUE:province,
        DEFAULT_ADDRESS.VALUE:country,
        DEFAULT_ADDRESS.VALUE:zip,
        DEFAULT_ADDRESS.VALUE:phone::VARCHAR as default_address_phone,
        DEFAULT_ADDRESS.VALUE:name::VARCHAR as default_address_name,
        DEFAULT_ADDRESS.VALUE:province_code,
        DEFAULT_ADDRESS.VALUE:country_code,
        DEFAULT_ADDRESS.VALUE:country_name,
        DEFAULT_ADDRESS.VALUE:default,
        {% else %}
        CUSTOMER.id as customer_id,
        CUSTOMER.email as customer_email,
        CUSTOMER.accepts_marketing,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(CUSTOMER.created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as customer_created_at,
        CAST({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(CUSTOMER.updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as customer_updated_at,
        CUSTOMER.first_name as customer_first_name,
        CUSTOMER.last_name as customer_last_name,
        CUSTOMER.orders_count,
        CUSTOMER.state,
        CUSTOMER.total_spent,
        CUSTOMER.last_order_id,
        CUSTOMER.note as customer_note,
        CUSTOMER.verified_email,
        CUSTOMER.multipass_identifier,
        CUSTOMER.tax_exempt,
        CUSTOMER.phone as customer_phone,
        CUSTOMER.tags as customer_tags,
        CUSTOMER.last_order_name,
        DEFAULT_ADDRESS.id as default_address_id,
        DEFAULT_ADDRESS.customer_id as default_address_customer_id,
        DEFAULT_ADDRESS.first_name as default_address_first_name,
        DEFAULT_ADDRESS.last_name as default_address_last_name,
        DEFAULT_ADDRESS.company,
        DEFAULT_ADDRESS.address1,
        DEFAULT_ADDRESS.address2,
        DEFAULT_ADDRESS.city,
        DEFAULT_ADDRESS.province,
        DEFAULT_ADDRESS.country,
        DEFAULT_ADDRESS.zip,
        DEFAULT_ADDRESS.phone as default_address_phone,
        DEFAULT_ADDRESS.name as default_address_name,
        DEFAULT_ADDRESS.province_code,
        DEFAULT_ADDRESS.country_code,
        DEFAULT_ADDRESS.country_name,
        DEFAULT_ADDRESS.default,
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
            {{unnesting("CUSTOMER")}}
            {{multi_unnesting("CUSTOMER","DEFAULT_ADDRESS")}}
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
DENSE_RANK() OVER (PARTITION BY order_id, customer_id order by _daton_batch_runtime desc) row_num
from unnested_customers 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
