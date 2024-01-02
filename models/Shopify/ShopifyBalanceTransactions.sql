{% if var('ShopifyBalanceTransactions') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_balance_transactions_tbl_ptrn",'%shopify%balance_transactions',"shopify_balance_transactions_exclude_tbl_ptrn",'') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select
            {{ extract_brand_and_store_name_from_table(i, var("brandname_position_in_tablename"), var("get_brandname_from_tablename_flag"), var("default_brandname")) }} as brand,
            {{ extract_brand_and_store_name_from_table(i, var("storename_position_in_tablename"), var("get_storename_from_tablename_flag"), var("default_storename")) }} as store,
            cast(id as string) as id,
            type,
            test,
            cast(payout_id as string) as payout_id,
            payout_status,
            {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
            currency,
            cast(amount as numeric) as amount,
            cast(fee as numeric) as fee,
            cast(net as numeric) as net,
            cast( source_id as string) as source_id,
            source_type,
            {{timezone_conversion("processed_at")}} as processed_at,
            cast(source_order_id as string) as source_order_id,
            cast(source_order_transaction_id as string) as source_order_transaction_id,
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
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_balance_transactions_lookback') }},0) from {{ this }})
                {% endif %}
        qualify dense_rank() over (partition by id order by a.{{daton_batch_runtime()}} desc) = 1    
    {% if not loop.last %} union all {% endif %}
{% endfor %}
