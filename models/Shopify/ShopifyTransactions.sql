{% if var('ShopifyTransactions') %}
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
{{set_table_name('%shopify%transactions')}} and lower(table_name) not like '%tender%' and lower(table_name) not like '%balance%'
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
    safe_cast(id as string) id,
    safe_cast(order_id as string) order_id,
    kind,
    gateway,
    status,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
    test,
    authorization,
    parent_id,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
    source_name,
    safe_cast(amount as numeric) amount,
    currency,
    payment_id,
    admin_graphql_api_id,
    message,
    error_code,
    safe_cast(user_id as string) user_id,
    safe_cast(location_id as string) location_id,
    safe_cast(device_id as string) device_id,
    {% if var('currency_conversion_flag') %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
    {% else %}
        safe_cast(1 as decimal) as exchange_currency_rate,
        currency as exchange_currency_code,
    {% endif %}
    a.{{daton_user_id()}} as _daton_user_id,
    a.{{daton_batch_runtime()}} as _daton_batch_runtime,
    a.{{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	    from {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.processed_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}

        qualify
        row_number() over (partition by id order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
