{% if var('ShopifyRefundsRefundLineItems') %}
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

    select * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        CAST(a.id as string) refund_id,
        order_id,
        cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
        note,
        user_id,
        CAST(a.processed_at as timestamp) processed_at,
        restock,
        admin_graphql_api_id,
        {% if target.type =='snowflake' %}
        COALESCE(refund_line_items.VALUE:id::VARCHAR,'') as refund_line_items_id,
        refund_line_items.VALUE:quantity::NUMERIC as refund_line_items_quantity,
        COALESCE(refund_line_items.VALUE:line_item_id::VARCHAR,'') as refund_line_items_line_item_id,
        refund_line_items.VALUE:location_id::VARCHAR as refund_line_items_location_id,
        refund_line_items.VALUE:restock_type::VARCHAR as refund_line_items_restock_type,
        refund_line_items.VALUE:subtotal::NUMERIC as refund_line_items_subtotal,
        refund_line_items.VALUE:total_tax::NUMERIC as refund_line_items_total_tax,
        refund_line_items.VALUE:subtotal_set as refund_line_items_subtotal_set,
        refund_line_items.VALUE:total_tax_set as refund_line_items_total_tax_set,
        refund_line_items.VALUE:line_item as refund_line_items_line_item,
        {% else %}
        COALESCE(CAST(refund_line_items.id as string),'') as refund_line_items_id,
        refund_line_items.quantity as refund_line_items_quantity,
        COALESCE(CAST(refund_line_items.line_item_id as string),'') as refund_line_items_line_item_id,
        refund_line_items.location_id as refund_line_items_location_id,
        refund_line_items.restock_type as refund_line_items_restock_type,
        refund_line_items.subtotal as refund_line_items_subtotal,
        refund_line_items.total_tax as refund_line_items_total_tax,
        refund_line_items.subtotal_set as refund_line_items_subtotal_set,
        refund_line_items.total_tax_set as refund_line_items_total_tax_set,
        refund_line_items.line_item as refund_line_items_line_item,
        {% endif %}
        transactions,
        total_duties_set,
        order_adjustments,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
        ROW_NUMBER() OVER (PARTITION BY order_id, a.id, refund_line_items.VALUE:id order by _daton_batch_runtime desc) row_num
        {% else %}
        ROW_NUMBER() OVER (PARTITION BY order_id, a.id, refund_line_items.id order by _daton_batch_runtime desc) row_num
        {% endif %}
        from {{i}} a
            {{unnesting("refund_line_items")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}

        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}