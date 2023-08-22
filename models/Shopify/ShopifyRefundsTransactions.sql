{% if var('ShopifyRefundsTransactions') %}
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

with unnested_refunds as(
{% set table_name_query %}
{{set_table_name('%shopify%refunds')}} 
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
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then b.transactions_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            safe_cast(1 as decimal) as exchange_currency_rate,
            b.transactions_currency as exchange_currency_code, 
        {% endif %}
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        safe_cast(a.id as string) refund_id,
        safe_cast(a.order_id as string) as order_id,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        note,
        safe_cast(a.user_id as string) user_id,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        restock,
        a.admin_graphql_api_id,
        {{extract_nested_value("transactions","id","string")}} as transactions_id,
        {{extract_nested_value("transactions","order_id","string")}} as transactions_order_id,
        {{extract_nested_value("transactions","kind","string")}} as transactions_kind,
        {{extract_nested_value("transactions","gateway","string")}} as transactions_gateway,
        {{extract_nested_value("transactions","status","string")}} as transactions_status,
        {{extract_nested_value("transactions","created_at","timestamp")}} as transactions_created_at,
        {{extract_nested_value("transactions","test","boolean")}} as transactions_test,
        {{extract_nested_value("transactions","authorization","string")}} as transactions_authorization,
        {{extract_nested_value("transactions","parent_id","string")}} as transactions_parent_id,
        {{extract_nested_value("transactions","processed_at","timestamp")}} as transactions_processed_at,
        {{extract_nested_value("transactions","source_name","string")}} as transactions_source_name,
        {{extract_nested_value("transactions","amount","string")}} as transactions_amount,
        {{extract_nested_value("transactions","currency","string")}} as transactions_currency,
        {{extract_nested_value("transactions","admin_graphql_api_id","string")}} as transactions_admin_graphql_api_id,
        {{extract_nested_value("transactions","message","string")}} as  transactions_message,
        {{extract_nested_value("transactions","user_id","string")}} as transactions_user_id,
        {{extract_nested_value("transactions","payment_id","string")}} as transactions_payment_id,
        {{extract_nested_value("transactions","error_code","string")}} as transactions_error_code,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("transactions")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
            ) b 
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.transactions_currency = c.to_currency_code
            {% endif %}

        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

SELECT *, row_number() over (partition by refund_id order by _daton_batch_runtime desc) _seq_id
from (
select *
from unnested_refunds 
qualify
dense_rank() over (partition by refund_id order by _daton_batch_runtime desc) = 1
)