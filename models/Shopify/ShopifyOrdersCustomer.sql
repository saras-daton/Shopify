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
{{set_table_name('%shopify%orders')}} and lower(table_name) not like '%shopify%fulfillment_orders'
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
        coalesce(safe_cast(a.id as string),'N/A') as order_id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        safe_cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        a.currency,
        safe_cast(current_subtotal_price as numeric) as current_subtotal_price,
        safe_cast(current_total_discounts as numeric) as current_total_discounts,
        safe_cast(current_total_price as numeric) as current_total_price,
        safe_cast(current_total_tax as numeric) as current_total_tax,
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
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        safe_cast(subtotal_price as numeric) as subtotal_price,
        a.tags,
        taxes_included,
        test,
        token,
        safe_cast(total_discounts as numeric) as total_discounts,
        safe_cast(total_line_items_price as numeric) as total_line_items_price,
        safe_cast(total_outstanding as numeric) as total_outstanding,
        safe_cast(total_price as numeric) as total_price,
        safe_cast(total_price_usd as numeric) as total_price_usd,
        safe_cast(total_tax as numeric) as total_tax,
        safe_cast(total_tip_received as numeric) as total_tip_received,
        total_weight,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {{extract_nested_value("customer","id","string")}} as customer_id,
        {{extract_nested_value("customer","email","string")}} as customer_email,
        {{extract_nested_value("customer","accepts_marketing","boolean")}} as customer_accepts_marketing,
        {{extract_nested_value("customer","created_at","timestamp")}} as customer_created_at,
        {{extract_nested_value("customer","updated_at","timestamp")}} as customer_updated_at,
        {{extract_nested_value("customer","first_name","string")}} as customer_first_name,
        {{extract_nested_value("customer","last_name","string")}} as customer_last_name,
        {{extract_nested_value("customer","orders_count","numeric")}} customer_orders_count,
        {{extract_nested_value("customer","state","string")}} as customer_state,
        {{extract_nested_value("customer","total_spent","numeric")}} as customer_total_spent,
        {{extract_nested_value("customer","last_order_id","numeric")}} as customer_last_order_id,
        {{extract_nested_value("customer","verified_email","boolean")}} as customer_verified_email,
        {{extract_nested_value("customer","tax_exempt","boolean")}} as customer_tax_exempt,
        {{extract_nested_value("customer","phone","string")}} as customer_phone,
        {{extract_nested_value("customer","tags","string")}} as customer_tags,
        {{extract_nested_value("customer","currency","string")}} as customer_currency,
        {{extract_nested_value("customer","last_order_name","string")}} as customer_last_order_name,
        {{extract_nested_value("customer","accepts_marketing_updated_at","timestamp")}} as customer_accepts_marketing_updated_at,
        {{extract_nested_value("customer","admin_graphql_api_id","string")}} as customer_admin_graphql_api_id,
        {{extract_nested_value("default_address","id","string")}} as default_address_id,
        {{extract_nested_value("default_address","customer_id","string")}} as default_address_customer_id,
        {{extract_nested_value("default_address","address1","string")}} as default_address_address1,
        {{extract_nested_value("default_address","address2","string")}} as default_address_address2,
        {{extract_nested_value("default_address","city","string")}} as default_address_city,
        {{extract_nested_value("default_address","province","string")}} as default_address_province,
        {{extract_nested_value("default_address","country","string")}} as default_address_country,
        {{extract_nested_value("default_address","zip","string")}} as default_address_zip,
        {{extract_nested_value("default_address","phone","string")}} as default_address_phone,
        {{extract_nested_value("default_address","name","string")}} as default_address_name,
        {{extract_nested_value("default_address","province_code","string")}} as default_address_province_code,
        {{extract_nested_value("default_address","country_code","string")}} as default_address_country_code,
        {{extract_nested_value("default_address","country_name","string")}} as default_address_country_name,
        {{extract_nested_value("default_address","default","boolean")}} as default_address_default,
        {{extract_nested_value("default_address","first_name","string")}} as default_address_first_name,
        {{extract_nested_value("default_address","last_name","string")}} as default_address_last_name,
        {{extract_nested_value("default_address","company","string")}} as default_address_company,
        safe_cast(app_id as string) as app_id,
        customer_locale,
        a.note,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        fulfillment_status,
        safe_cast(location_id as string) as location_id,
        cancel_reason,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        safe_cast(user_id as string) as user_id,
        safe_cast(device_id as string) as device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            safe_cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code,
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