{% if var('ShopifyOrdersCustomer') %}
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'N/A') as order_id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        a.currency,
        cast(current_subtotal_price as numeric) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        coalesce(a.email,'N/A') as email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        a.name,
        number,
        order_number,
        order_status_url,
        payment_gateway_names,
        a.phone,
        presentment_currency,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        cast(subtotal_price as numeric) as subtotal_price,
        a.tags,
        taxes_included,
        test,
        token,
        cast(total_discounts as numeric) as total_discounts,
        cast(total_line_items_price as numeric) as total_line_items_price,
        cast(total_outstanding as numeric) as total_outstanding,
        cast(total_price as numeric) as total_price,
        cast(total_price_usd as numeric) as total_price_usd,
        cast(total_tax as numeric) as total_tax,
        cast(total_tip_received as numeric) as total_tip_received,
        total_weight,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {% if target.type =='snowflake' %}
        customer.value:id::varchar as customer_id,
        customer.value:email::varchar as customer_email,
        customer.value:accepts_marketing as customer_accepts_marketing,
        customer.value:created_at::timestamp as customer_created_at,
        customer.value:updated_at::timestamp as customer_updated_at,
        customer.value:first_name::varchar as customer_first_name,
        customer.value:last_name::varchar as customer_last_name,
        customer.value:orders_count as customer_orders_count,
        customer.value:state as customer_state,
        customer.value:total_spent as customer_total_spent,
        customer.value:last_order_id as customer_last_order_id,
        customer.value:verified_email as customer_verified_email,
        customer.value:tax_exempt as customer_tax_exempt,
        customer.value:phone::varchar as customer_phone,
        customer.value:tags::varchar as customer_tags,
        customer.value:currency::varchar as customer_currency,
        customer.value:last_order_name as customer_last_order_name,
        customer.value:accepts_marketing_updated_at as customer_accepts_marketing_updated_at,
        customer.value:admin_graphql_api_id as customer_admin_graphql_api_id,
        default_address.value:id::varchar as default_address_id,
        default_address.value:customer_id::varchar as default_address_customer_id,
        default_address.value:address1::varchar as default_address_address1,
        default_address.value:address2::varchar as default_address_address2,
        default_address.value:city::varchar as default_address_city,
        default_address.value:province::varchar as default_address_province,
        default_address.value:country::varchar as default_address_country,
        default_address.value:zip::varchar as default_address_zip,
        default_address.value:phone::varchar as default_address_phone,
        default_address.value:name::varchar as default_address_name,
        default_address.value:province_code as default_address_province_code,
        default_address.value:country_code as default_address_country_code,
        default_address.value:country_name as default_address_country_name,
        default_address.value:default as default_address_default,
        default_address.value:first_name::varchar as default_address_first_name,
        default_address.value:last_name::varchar as default_address_last_name,
        default_address.value:company::varchar as default_address_company,
        {% else %}
        cast(customer.id as string) as customer_id,
        customer.email as customer_email,
        customer.accepts_marketing as customer_accepts_marketing,
        customer.created_at as customer_created_at,
        customer.updated_at as customer_updated_at,
        customer.first_name as customer_first_name,
        customer.last_name as customer_last_name,
        customer.orders_count as customer_orders_count,
        customer.state as customer_state,
        customer.total_spent as customer_total_spent,
        customer.last_order_id as customer_last_order_id,
        customer.verified_email as customer_verified_email,
        customer.tax_exempt as customer_tax_exempt,
        customer.phone as customer_phone,
        customer.tags as customer_tags,
        customer.currency as customer_currency,
        customer.last_order_name as customer_last_order_name,
        customer.accepts_marketing_updated_at as customer_accepts_marketing_updated_at,
        customer.admin_graphql_api_id as customer_admin_graphql_api_id,
        default_address.id as default_address_id,
        default_address.customer_id as default_address_customer_id,
        default_address.address1 as default_address_address1,
        default_address.address2 as default_address_address2,
        default_address.city as default_address_city,
        default_address.province as default_address_province,
        default_address.country as default_address_country,
        default_address.zip as default_address_zip,
        default_address.phone as default_address_phone,
        default_address.name as default_address_name,
        default_address.province_code as default_address_province_code,
        default_address.country_code as default_address_country_code,
        default_address.country_name as default_address_country_name,
        default_address.default as default_address_default,
        default_address.first_name as default_address_first_name,
        default_address.last_name as default_address_last_name,
        default_address.company as default_address_company,
        {% endif %}
        cast(app_id as string) as app_id,
        customer_locale,
        a.note,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        fulfillment_status,
        cast(location_id as string) as location_id,
        cancel_reason,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
                {{unnesting("customer")}}
                {{multi_unnesting("customer","default_address")}}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        {% if target.type =='snowflake' %}
        qualify dense_rank() over (partition by a.id, customer.value:id order by a._daton_batch_runtime desc) = 1
        {% else %}
        qualify dense_rank() over (partition by a.id, customer.id order by a._daton_batch_runtime desc) = 1
        {% endif %}
    {% if not loop.last %} union all {% endif %}
{% endfor %}


