{% if var('ShopifyPayouts') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if is_incremental() %}
    {%- set max_loaded_query -%}
    select coalesce(max(_daton_batch_runtime) - 2592000000, 0) from {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
        {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
        {% set max_loaded = 0 %}
    {%- endif -%}
{% endif %}

{% set table_name_query %}
    {{ set_table_name('%shopify%payouts') }} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
        {% set brand = i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store = i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    select 
        '{{ brand }}' as brand,
        '{{ store }}' as store,
        cast(id as string) as id,
        status,
        cast(date as date) as date,
        currency,
        cast(amount as numeric) as amount,
        {% if target.type == 'snowflake' %}
            cast(summary.value:adjustments_fee_amount as numeric) as summary_adjustments_fee_amount,
            cast(summary.value:adjustments_gross_amount as numeric) as summary_adjustments_gross_amount,
            cast(summary.value:charges_fee_amount as numeric) as summary_charges_fee_amount,
            cast(summary.value:charges_gross_amount as numeric) as summary_charges_gross_amount,
            cast(summary.value:refunds_fee_amount as numeric) as summary_refunds_fee_amount,
            cast(summary.value:refunds_gross_amount as numeric) as summary_refunds_gross_amount,
            cast(summary.value:reserved_funds_fee_amount as numeric) as summary_reserved_funds_fee_amount,
            cast(summary.value:reserved_funds_gross_amount as numeric) as summary_reserved_funds_gross_amount,
            cast(summary.value:retried_payouts_fee_amount as numeric) as summary_retried_payouts_fee_amount,
            cast(summary.value:retried_payouts_gross_amount as numeric) as summary_retried_payouts_gross_amount,
        {% else %}
            cast(summary.adjustments_fee_amount as numeric) as summary_adjustments_fee_amount,
            cast(summary.adjustments_gross_amount as numeric) as summary_adjustments_gross_amount,
            cast(summary.charges_fee_amount as numeric) as summary_charges_fee_amount,
            cast(summary.charges_gross_amount as numeric) as summary_charges_gross_amount,
            cast(summary.refunds_fee_amount as numeric) as summary_refunds_fee_amount,
            cast(summary.refunds_gross_amount as numeric) as summary_refunds_gross_amount,
            cast(summary.reserved_funds_fee_amount as numeric) as summary_reserved_funds_fee_amount,
            cast(summary.reserved_funds_gross_amount as numeric) as summary_reserved_funds_gross_amount,
            cast(summary.retried_payouts_fee_amount as numeric) as summary_retried_payouts_fee_amount,
            cast(summary.retried_payouts_gross_amount as numeric) as summary_retried_payouts_gross_amount,
        {% endif %}
        {{ daton_user_id() }} as _daton_user_id,
        {{ daton_batch_runtime() }} as _daton_batch_runtime,
        {{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from  {{ i }} a
    {{ unnesting("summary") }} 
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{ daton_batch_runtime() }} >= {{ max_loaded }}
    {% endif %}

    qualify dense_rank() over (partition BY a.id order by {{ daton_batch_runtime() }} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}