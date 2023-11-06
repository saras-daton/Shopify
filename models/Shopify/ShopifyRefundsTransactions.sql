{% if var('ShopifyRefundsTransactions') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_refunds_transactions_tbl_ptrn'),
exclude=var('shopify_refunds_transactions_exclude_tbl_ptrn'),
database=var('raw_database')) %}

with unnested_refunds as(
{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
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
        {{extract_nested_value("transactions","id","numeric")}} as transactions_id,
        {{extract_nested_value("transactions","order_id","numeric")}} as transactions_order_id,
        {{extract_nested_value("transactions","kind","string")}} as transactions_kind,
        {{extract_nested_value("transactions","gateway","string")}} as transactions_gateway,
        {{extract_nested_value("transactions","status","string")}} as transactions_status,
        {{extract_nested_value("transactions","created_at","timestamp")}} as transactions_created_at,
        {{extract_nested_value("transactions","test","boolean")}} as transactions_test,
        {{extract_nested_value("transactions","authorization","string")}} as transactions_authorization,
        {{extract_nested_value("transactions","parent_id","numeric")}} as transactions_parent_id,
        {{extract_nested_value("transactions","processed_at","timestamp")}} as transactions_processed_at,
        {{extract_nested_value("transactions","source_name","string")}} as transactions_source_name,
        {{extract_nested_value("transactions","amount","string")}} as transactions_amount,
        {{extract_nested_value("transactions","currency","string")}} as transactions_currency,
        {{extract_nested_value("transactions","admin_graphql_api_id","string")}} as transactions_admin_graphql_api_id,
        {{extract_nested_value("transactions","message","string")}} as  transactions_message,
        {{extract_nested_value("transactions","user_id","numeric")}} as transactions_user_id,
        {{extract_nested_value("transactions","payment_id","string")}} as transactions_payment_id,
        {{extract_nested_value("transactions","error_code","string")}} as ttransactions_error_code,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id
        from {{i}} a
            {{unnesting("transactions")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_refunds_transactions_lookback') }},0) from {{ this }})
            {% endif %}
            ) b 
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(b.created_at) = c.date and b.transactions_currency = c.to_currency_code
            {% endif %}

        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select *, row_number() over (partition by refund_id order by _daton_batch_runtime desc) _seq_id
from (
select *
from unnested_refunds 
qualify
dense_rank() over (partition by refund_id order by _daton_batch_runtime desc) = 1
)