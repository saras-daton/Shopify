{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_orders_tbl_ptrn","shopify_orders_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        safe_cast(a.id as string) as order_id,
        -- a.admin_graphql_api_id,
        -- browser_ip,
        -- buyer_accepts_marketing,
        -- cart_token,
        -- safe_cast(checkout_id as string) as checkout_id,
        -- checkout_token,
        -- confirmed,
        -- contact_email,
        {{timezone_conversion("a.created_at")}} as created_at,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
        -- cast(current_subtotal_price as numeric) as current_subtotal_price,
        -- cast(current_total_discounts as numeric) as current_total_discounts,
        -- cast(current_total_price as numeric) as current_total_price,
        -- cast(current_total_tax as numeric) as current_total_tax,
        -- email,
        -- estimated_taxes,
        -- financial_status,
        -- gateway,
        -- landing_site,
        -- landing_site_ref,
        -- a.name,
        -- number,
        -- order_number,
        -- order_status_url,
        -- payment_gateway_names,
        -- phone,
        -- presentment_currency,
        {{timezone_conversion("processed_at")}} as processed_at,
        -- processing_method,
        -- reference,
        -- referring_site,
        -- source_identifier,
        -- source_name,
        -- cast(subtotal_price as numeric) as subtotal_price,
        -- tags,
        -- taxes_included,
        -- test,
        -- token,
        -- cast(total_discounts as numeric) as total_discounts,
        -- cast(total_outstanding as numeric) as total_outstanding,
        -- cast(total_price as numeric) as total_price,
        -- cast(total_price_usd as numeric) as total_price_usd,
        -- cast(total_tax as numeric) as total_tax,
        -- cast(total_tip_received as numeric) as total_tip_received,
        -- total_weight,
        -- cast(total_line_items_price as numeric) as total_line_items_price,
        {{timezone_conversion("a.updated_at")}} as updated_at,
        {% if target.type == 'snowflake' %}
        {{timezone_conversion("fulfillments.value:created_at")}} as fulfillments_created_at,
        {{timezone_conversion("fulfillments.value:updated_at")}} as fulfillments_updated_at,
        {% else %}
        {{timezone_conversion("fulfillments.created_at")}} as fulfillments_created_at,
        {{timezone_conversion("fulfillments.updated_at")}} as fulfillments_updated_at,
        {% endif %}
        coalesce({{extract_nested_value("fulfillments","id","string")}},'N/A') as fulfillments_id,
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
        -- coalesce({{extract_nested_value("line_items","id","string")}},'N/A') as line_items_id,
        -- {{extract_nested_value("line_items","admin_graphql_api_id","string")}} as line_items_admin_graphql_api_id,
        -- {{extract_nested_value("line_items","fulfillable_quantity","numeric")}} as line_items_fulfillable_quantity,
        -- {{extract_nested_value("line_items","fulfillment_service","string")}} as line_items_fulfillment_service,
        -- {{extract_nested_value("line_items","gift_card","string")}} as line_items_gift_card,
        -- {{extract_nested_value("line_items","grams","numeric")}} as line_items_grams,
        -- {{extract_nested_value("line_items","name","string")}} as line_items_name,
        -- {{extract_nested_value("line_items","price","numeric")}} as line_items_price,
        -- {{extract_nested_value("line_items","product_exists","boolean")}} as line_items_product_exists,
        -- {{extract_nested_value("line_items","product_id","string")}} as line_items_product_id,
        -- {{extract_nested_value("line_items","quantity","numeric")}} as line_items_quantity,
        -- {{extract_nested_value("line_items","requires_shipping","boolean")}} as line_items_requires_shipping,
        -- {{extract_nested_value("line_items","sku","string")}} as line_items_sku,
        -- {{extract_nested_value("line_items","taxable","boolean")}} as line_items_taxable,
        -- {{extract_nested_value("line_items","title","string")}} as line_items_title,
        -- {{extract_nested_value("line_items","total_discount","numeric")}} as line_items_total_discount,
        -- {{extract_nested_value("line_items","variant_id","string")}} as line_items_variant_id,
        -- {{extract_nested_value("line_items","variant_inventory_management","string")}} as line_items_variant_inventory_management,
        -- {{extract_nested_value("line_items","variant_title","string")}} as line_items_variant_title,
        -- {{extract_nested_value("line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
        -- {{extract_nested_value("line_items","pre_tax_price","numeric")}} as line_items_pre_tax_price,
        -- {{extract_nested_value("line_items","tax_code","string")}} as line_items_tax_code,
        -- {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
        {{extract_nested_value("fulfillments","shipment_status","string")}} as fulfillments_shipment_status,
        -- safe_cast(app_id as string) app_id,
        -- customer_locale,
        -- note,
        {{timezone_conversion("closed_at")}} as closed_at,
        -- a.fulfillment_status,
        -- safe_cast(a.location_id as string) location_id,
        -- cancel_reason,
        {{timezone_conversion("cancelled_at")}} as cancelled_at,
        -- safe_cast(user_id as string) user_id,
        -- safe_cast(device_id as string) device_id,
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