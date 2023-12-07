{% if var('ShopifyDisputes') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_disputes_tbl_ptrn","shopify_disputes_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(id as string) as id,
        cast(order_id as string) as order_id,
        type,
        cast(amount as numeric) as amount,
        currency,
        reason,
        network_reason_code,
        status,
        {{timezone_conversion("evidence_due_by")}} as evidence_due_by,
        {{timezone_conversion("evidence_sent_on")}} as evidence_sent_on,
        {{timezone_conversion("finalized_on")}} as finalized_on,
        {{timezone_conversion("initiated_at")}} as initiated_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_disputes_lookback') }},0) from {{ this }})
                {% endif %}
        qualify dense_rank() over (partition by id order by a.{{daton_batch_runtime()}} desc)  = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
