{% if var('ShopifyOrdersRefundsTransactions') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
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
{{set_table_name('%shopify%orders%')}} and lower(table_name) not like '%googleanalytics%'
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
        admin_graphql_api_id,
        cast(a.id as string) order_id, 
        a.email,
        a.closed_at,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        CAST(a.updated_at as timestamp) updated_at,
        number,
        a.note,
        token,
        a.gateway,
        a.test,
        total_price,
        subtotal_price,
        total_weight,
        total_tax,
        taxes_included,
        a.currency,
        financial_status,
        confirmed,
        total_discounts,
        total_line_items_price,
        cart_token,
        buyer_accepts_marketing,
        a.name,
        referring_site,
        landing_site,
        cancelled_at,
        cancel_reason,
        total_price_usd,
        checkout_token,
        reference,
        a.user_id,
        a.location_id,
        source_identifier,
        source_url,
        CAST(a.processed_at as timestamp) processed_at,
        a.device_id,
        a.phone,
        customer_locale,
        app_id,
        browser_ip,
        landing_site_ref,
        order_number,
        payment_gateway_names,
        processing_method,
        checkout_id,
        a.source_name,
        a.fulfillment_status,
        a.tags,
        contact_email,
        order_status_url,
        {% if target.type =='snowflake' %}
        refunds.VALUE:id::VARCHAR as refunds_id,
        refunds.VALUE:order_id::VARCHAR as refunds_order_id,
        refunds.VALUE:created_at::timestamp as refunds_created_at,
        refunds.VALUE:note::VARCHAR as ref_note,
        refunds.VALUE:user_id::VARCHAR as refunds_user_id,
        refunds.VALUE:processed_at::timestamp as refunds_processed_at,
        refunds.VALUE:restock as restock,
        refunds.VALUE:refund_line_items,
        COALESCE(transactions.VALUE:id::VARCHAR,'') as transactions_id,
        transactions.VALUE:order_id::VARCHAR as transactions_order_id,
        transactions.VALUE:amount::numeric as transactions_amount,
        transactions.VALUE:kind::VARCHAR as kind,
        transactions.VALUE:gateway::VARCHAR as transactions_gateway,
        transactions.VALUE:status::VARCHAR as status,
        transactions.VALUE:message::VARCHAR as message,
        transactions.VALUE:created_at::VARCHAR as transactions_created_at,
        transactions.VALUE:test::VARCHAR as transactions_test,
        transactions.VALUE:authorization::VARCHAR as authorization,
        transactions.VALUE:currency::VARCHAR as transactions_currency,
        transactions.VALUE:location_id::VARCHAR as transactions_location_id,
        transactions.VALUE:user_id::VARCHAR as transactions_user_id,
        transactions.VALUE:parent_id::VARCHAR as parent_id,
        transactions.VALUE:device_id::VARCHAR as transactions_device_id,
        transactions.VALUE:receipt::VARCHAR as receipt,
        transactions.VALUE:error_code::VARCHAR as error_code,
        transactions.VALUE:source_name::VARCHAR as transactions_source_name,
        {% else %}
        refunds.id as refunds_id,
        refunds.order_id as refunds_order_id,
        CAST(refunds.created_at as timestamp) refunds_created_at,
        refunds.note as ref_note,
        refunds.user_id as refunds_user_id,
        CAST(refunds.processed_at as timestamp) refunds_processed_at,
        refunds.restock as restock,
        refunds.refund_line_items,
        COALESCE(CAST(transactions.id as string),'') as transactions_id,
        transactions.order_id as transactions_order_id,
        transactions.amount as transactions_amount,
        transactions.kind,
        transactions.gateway as transactions_gateway,
        transactions.status,
        transactions.message,
        transactions.created_at as transactions_created_at,
        transactions.test as transactions_test,
        transactions.authorization,
        transactions.currency as transactions_currency,
        transactions.location_id as transactions_location_id,
        transactions.user_id as transactions_user_id,
        transactions.parent_id,
        transactions.device_id as transactions_device_id,
        transactions.receipt,
        transactions.error_code,
        transactions.source_name as transactions_source_name,
        {% endif %}
        {% if var('currency_conversion_flag') %}
            case when d.value is null then 1 else d.value end as exchange_currency_rate,
            case when d.from_currency_code is null then a.currency else d.from_currency_code end as exchange_currency_code, 
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            a.currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} d on date(a.created_at) = d.date and a.currency = d.to_currency_code
            {% endif %}
            {{unnesting("refunds")}}
            {{multi_unnesting("refunds","transactions")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}

        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY order_id order by _daton_batch_runtime desc) row_num
from unnested_refunds 
)

SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id order by _daton_batch_runtime desc) _seq_id
from (
select * {{exclude()}} (row_num)
from dedup 
where row_num = 1)
