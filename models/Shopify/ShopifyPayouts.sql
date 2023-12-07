{% if var('ShopifyPayouts') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_payouts_tbl_ptrn","shopify_payouts_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,        cast(id as string) as id,
        status,
        cast(date as date) as date,
        currency,
        cast(amount as numeric) as amount,
        {{extract_nested_value("summary","adjustments_fee_amount","numeric")}} as summary_adjustments_fee_amount,
        {{extract_nested_value("summary","adjustments_gross_amount","numeric")}} as summary_adjustments_gross_amount,
        {{extract_nested_value("summary","charges_fee_amount","numeric")}} as summary_charges_fee_amount,
        {{extract_nested_value("summary","charges_gross_amount","numeric")}} as summary_charges_gross_amount,
        {{extract_nested_value("summary","refunds_fee_amount","numeric")}} as summary_refunds_fee_amount,
        {{extract_nested_value("summary","refunds_gross_amount","numeric")}} as summary_refunds_gross_amount,
        {{extract_nested_value("summary","reserved_funds_fee_amount","numeric")}} as summary_reserved_funds_fee_amount,
        {{extract_nested_value("summary","reserved_funds_gross_amount","numeric")}} as summary_reserved_funds_gross_amount,
        {{extract_nested_value("summary","retried_payouts_fee_amount","numeric")}} as summary_retried_payouts_fee_amount,
        {{extract_nested_value("summary","retried_payouts_gross_amount","numeric")}} as summary_retried_payouts_gross_amount,
        {{ daton_user_id() }} as _daton_user_id,
        {{ daton_batch_runtime() }} as _daton_batch_runtime,
        {{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from  {{ i }} a
    {{ unnesting("summary") }} 
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_payouts_lookback') }},0) from {{ this }})
    {% endif %}

    qualify dense_rank() over (partition BY a.id order by {{ daton_batch_runtime() }} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}