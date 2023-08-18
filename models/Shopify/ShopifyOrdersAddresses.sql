{% if var('ShopifyOrdersAddresses') %}
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

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'N/A') as order_id, 
        admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        currency,
        cast(current_subtotal_price as string) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        discount_codes,
        coalesce(email,'N/A') as email,
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
        subtotal_price,
        tags,
        taxes_included,
        test,
        token,
        cast(total_discounts as numeric) as total_discounts,
        cast(total_line_items_price as numeric) as total_line_items,
        total_outstanding,
        cast(total_price as numeric) as total_price,
        cast(total_price_usd as numeric) as total_price_usd,
        cast(total_tax as numeric) as total_tax,
        cast(total_tip_received as numeric) as total_tip_received,
        total_weight,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {{extract_nested_value("billing_address","first_name","string")}} as billing_address_first_name,
        {{extract_nested_value("billing_address","address1","string")}} as billing_address_address1,
        {{extract_nested_value("billing_address","phone","string")}} as billing_address_phone,
        {{extract_nested_value("billing_address","city","string")}} as billing_address_city,
        {{extract_nested_value("billing_address","zip","string")}} as billing_address_zip,
        {{extract_nested_value("billing_address","province","string")}} as billing_address_province,
        {{extract_nested_value("billing_address","country","string")}} as billing_address_country,
        {{extract_nested_value("billing_address","last_name","string")}} as billing_address_last_name,
        {{extract_nested_value("billing_address","address2","string")}} as billing_address_address2,
        {{extract_nested_value("billing_address","latitude","numeric")}} as billing_address_latitude,
        {{extract_nested_value("billing_address","longitude","numeric")}} as billing_address_longitude,
        {{extract_nested_value("billing_address","name","string")}} as billing_address_name,
        {{extract_nested_value("billing_address","country_code","string")}} as billing_address_country_code,
        {{extract_nested_value("billing_address","province_code","string")}} as billing_address_province_code,
        {{extract_nested_value("billing_address","longitude_bn","bignumeric")}} as billing_address_longitude_bn,
        {{extract_nested_value("billing_address","latitude_bn","bignumeric")}} as billing_address_latitude_bn,
        {{extract_nested_value("billing_address","company","string")}} as billing_address_company,
        {{extract_nested_value("shipping_address","first_name","string")}} as shipping_address_first_name,
        {{extract_nested_value("shipping_address","address1","string")}} as shipping_address_address1,
        {{extract_nested_value("shipping_address","phone","string")}} as shipping_address_phone,
        {{extract_nested_value("shipping_address","city","string")}} as shipping_address_city,
        {{extract_nested_value("shipping_address","zip","string")}} as shipping_address_zip,
        {{extract_nested_value("shipping_address","province","string")}} as shipping_address_province,
        {{extract_nested_value("shipping_address","country","string")}} as shipping_address_country,
        {{extract_nested_value("shipping_address","last_name","string")}} as shipping_address_last_name,
        {{extract_nested_value("shipping_address","address2","string")}} as shipping_address_address2,
        {{extract_nested_value("shipping_address","latitude","numeric")}} as shipping_address_latitude,
        {{extract_nested_value("shipping_address","longitude","numeric")}} as shipping_address_longitude,
        {{extract_nested_value("shipping_address","name","string")}} as shipping_address_name,
        {{extract_nested_value("shipping_address","country_code","string")}} as shipping_address_country_code,
        {{extract_nested_value("shipping_address","province_code","string")}} as shipping_address_province_code,
        {{extract_nested_value("shipping_address","longitude_bn","bignumeric")}} as shipping_address_latitude_bn,
        {{extract_nested_value("shipping_address","latitude_bn","bignumeric")}} as shipping_address_longitude_bn,
        {{extract_nested_value("shipping_address","company","string")}} as shipping_address_company,
        cast(app_id as string) as app_id,
        customer_locale,
        note,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        fulfillment_status,
        location_id,
        cancel_reason,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
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
                {{unnesting("BILLING_ADDRESS")}} 
                {{unnesting("SHIPPING_ADDRESS")}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        qualify dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
