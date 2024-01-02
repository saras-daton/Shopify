{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_orders_tbl_ptrn','%shopify%orders','shopify_orders_exclude_tbl_ptrn','%shopify%fulfillment_orders') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        safe_cast(a.id as string) as order_id,
        {{timezone_conversion("a.created_at")}} as created_at,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        {{timezone_conversion("processed_at")}} as processed_at,
        {{timezone_conversion("a.updated_at")}} as updated_at,
        {% if target.type == 'snowflake' %}
        {{timezone_conversion("fulfillments.value:created_at")}} as fulfillments_created_at,
        {{timezone_conversion("fulfillments.value:updated_at")}} as fulfillments_updated_at,
        {% else %}
        {{timezone_conversion("fulfillments.created_at")}} as fulfillments_created_at,
        {{timezone_conversion("fulfillments.updated_at")}} as fulfillments_updated_at,
        {% endif %}
        {{extract_nested_value("fulfillments","id","string")}} as fulfillments_id,
        {{extract_nested_value("fulfillments","admin_graphql_api_id","string")}} as fulfillments_admin_graphql_api_id,
        {{extract_nested_value("fulfillments","location_id","string")}} as fulfillments_location_id,
        {{extract_nested_value("fulfillments","name","string")}} as fulfillments_name,
        {{extract_nested_value("fulfillments","order_id","string")}} as fulfillments_order_id,
        {{extract_nested_value("receipt","testcase","boolean")}} as receipt_testcase,
        {{extract_nested_value("receipt","authorization","string")}} as receipt_authorization,
        {{extract_nested_value("fulfillments","service","string")}} as fulfillments_service,
        {{extract_nested_value("fulfillments","status","string")}} as fulfillments_status,
        {{extract_nested_value("fulfillments","tracking_company","string")}} as fulfillments_tracking_company,
        {{extract_nested_value("fulfillments","tracking_number","string")}} as fulfillments_tracking_number,
        {{extract_nested_value("fulfillments","tracking_numbers","string")}} as fulfillments_tracking_numbers,
        {{extract_nested_value("fulfillments","tracking_url","string")}} as fulfillments_tracking_url,
        {{extract_nested_value("fulfillments","tracking_urls","string")}} as fulfillments_tracking_urls,
        {{extract_nested_value("fulfillments","shipment_status","string")}} as fulfillments_shipment_status,
        {{timezone_conversion("closed_at")}} as closed_at,
        {{timezone_conversion("cancelled_at")}} as cancelled_at,
        a.{{ daton_user_id() }} as _daton_user_id,
        a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
        a.{{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from {{ i }} a
        {% if var('currency_conversion_flag') %}
            left join {{ ref('ExchangeRates') }} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
        {% endif %}
        {{ unnesting("fulfillments") }}
        {{ multi_unnesting("fulfillments", "receipt") }}
        {{ multi_unnesting("fulfillments", "line_items") }}

        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_fulfillments_lookback') }},0) from {{ this }})
        {% endif %}

    qualify dense_rank() over (partition by a.id order by a._daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}