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
SELECT coalesce(max(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
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
{{set_table_name('%shopify%refunds')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
            cast(1 as decimal) as exchange_currency_rate,
            b.transactions_currency as exchange_currency_code, 
        {% endif %}
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        cast(a.id as string) refund_id,
        cast(a.order_id as string) as order_id,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        note,
        cast(a.user_id as string) user_id,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        restock,
        a.admin_graphql_api_id,
        {% if target.type =='snowflake' %}
            coalesce(transactions.VALUE:id::VARCHAR,'') as transactions_id,
            transactions.VALUE:order_id::VARCHAR as transactions_order_id,
            transactions.VALUE:kind::VARCHAR as transactions_kind,
            transactions.VALUE:gateway::VARCHAR as transactions_gateway,
            transactions.VALUE:status::VARCHAR as transactions_status,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="transactions.VALUE:created_at::TIMESTAMP") }} as {{ dbt.type_timestamp() }}) as transactions_created_at,
            transactions.VALUE:test::VARCHAR as transactions_test,
            transactions.VALUE:authorization::VARCHAR as transactions_authorization,
            transactions.VALUE:parent_id::VARCHAR as transactions_parent_id,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="transactions.VALUE:processed_at::TIMESTAMP") }} as {{ dbt.type_timestamp() }}) as transactions_processed_at,
            transactions.VALUE:source_name::VARCHAR as transactions_source_name,
            transactions.VALUE:amount::NUMERIC as transactions_amount,
            transactions.VALUE:currency as transactions_currency,
            transactions.VALUE:admin_graphql_api_id as transactions_admin_graphql_api_id,
            transactions.VALUE:message as transactions_message,
            transactions.VALUE:user_id::VARCHAR as transactions_user_id,
            transactions.VALUE:payment_id as transactions_payment_id,
            transactions.VALUE:error_code::VARCHAR as transactions_error_code,
        {% else %}
            coalesce(cast(transactions.id as string),'') as transactions_id,
            cast(transactions.order_id as string) as transactions_order_id,
            transactions.kind as transactions_kind,
            transactions.gateway as transactions_gateway,
            transactions.status as transactions_status,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="transactions.created_at") }} as {{ dbt.type_timestamp() }}) as transactions_created_at,
            transactions.test as transactions_test,
            transactions.authorization as transactions_authorization,
            cast(transactions.parent_id as string) as transactions_parent_id,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="transactions.processed_at") }} as {{ dbt.type_timestamp() }}) as transactions_processed_at,
            transactions.processed_at as transactions_processed_at,
            transactions.source_name as transactions_source_name,
            cast(transactions.amount as numeric) as transactions_amount,
            transactions.currency as transactions_currency,
            transactions.admin_graphql_api_id as transactions_admin_graphql_api_id,
            transactions.message as transactions_message,
            cast(transactions.user_id as string) as transactions_user_id,
            transactions.payment_id as transactions_payment_id,
            transactions.error_code as transactions_error_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
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
),

dedup as (
select *
from unnested_refunds 
qualify
dense_rank() over (partition by refund_id order by _daton_batch_runtime desc) row_num = 1
)

SELECT *, row_number() over (partition by refund_id order by _daton_batch_runtime desc) _seq_id
from dedup
