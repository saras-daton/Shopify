{% if var('ShopifyRefundsTransactions') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_refunds_tbl_ptrn','%shopify%refunds','shopify_refunds_transactions_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        b.* {{exclude()}} (_daton_user_id, _daton_batch_runtime, _daton_batch_id),
        {{currency_conversion('c.value', 'c.from_currency_code', 'b.transactions_currency') }},
        b._daton_user_id,
        b._daton_batch_runtime,
        b._daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from (
        select
        cast(a.id as string) refund_id,
        cast(a.order_id as string) as order_id,
        {{timezone_conversion("a.created_at")}} as created_at,
        note,
        cast(a.user_id as string) user_id,
        {{timezone_conversion("a.processed_at")}} as processed_at,
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
            qualify dense_rank() over (partition by refund_id order by _daton_batch_runtime desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
