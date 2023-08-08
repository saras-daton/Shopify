{% if var('ShopifyCustomers') %}
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
{{set_table_name('%shopify%customers')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
        coalesce(cast(a.id as string),'n/a') as customers_id,
        email,
        accepts_marketing,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(a.created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as created_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(a.updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as updated_at,
        a.first_name,
        a.last_name,
        orders_count,
        a.state,
        cast(total_spent as numeric) as total_spent,
        verified_email,
        tax_exempt,
        tags,
        currency,
        a.phone,
        accepts_marketing_updated_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="accepts_marketing_updated_at") }} as {{ dbt.type_timestamp() }}) as accepts_marketing_updated_at,
        a.admin_graphql_api_id,
        {% if target.type =='snowflake' %}
        default_address.value:id::VARCHAR as default_address_id,
        default_address.value:customer_id::VARCHAR as default_address_customer_id,
        default_address.value:first_name::VARCHAR as default_address_first_name,
        default_address.value:last_name::VARCHAR as default_address_last_name,
        default_address.value:address1::VARCHAR as default_address_address1,
        default_address.value:city::VARCHAR as default_address_city,
        default_address.value:province::VARCHAR as default_address_province,
        default_address.value:country::VARCHAR as default_address_country,
        default_address.value:zip::VARCHAR as default_address_zip,
        default_address.value:phone::VARCHAR as default_address_phone,
        default_address.value:name::VARCHAR as default_address_name,
        default_address.value:province_code::VARCHAR as default_address_province_code,
        default_address.value:country_code::VARCHAR as default_address_country_code,
        default_address.value:country_name::VARCHAR as default_address_country_name,
        default_address.value:default::VARCHAR as default_address_default,
        default_address.value:address2::VARCHAR as default_address_address2,
        default_address.value:company::VARCHAR as default_address_company,
        {% else %}
        default_address.id as default_address_id,
        default_address.customer_id as default_address_customer_id,
        default_address.first_name as default_address_first_name,
        default_address.last_name as default_address_last_name,
        default_address.address1 as default_address_address1,
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
        default_address.address2 as default_address_address2,
        default_address.company as default_address_company,
        {% endif %}
        last_order_id,
        last_order_name,
        marketing_opt_in_level,
        note,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code, 
        {% endif %} 
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from  {{i}} a
                {{unnesting("default_address")}} 
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.updated_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        qualify dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
