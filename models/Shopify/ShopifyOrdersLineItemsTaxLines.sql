{% if var('ShopifyOrdersLineItemsTaxLines') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if var('currency_conversion_flag') %}
    -- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_orders_tbl_ptrn","shopify_orders_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select *, row_number() over (partition by order_id order by _daton_batch_runtime desc) _seq_id
    from (
        select 
            {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
            {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
            coalesce(safe_cast(a.id as string),'N/A') as order_id, 
            a.admin_graphql_api_id,
            browser_ip,
            buyer_accepts_marketing,
            cart_token,
            safe_cast(checkout_id as string) as checkout_id,
            checkout_token,
            confirmed,
            contact_email,
            {{timezone_conversion("a.created_at")}} as created_at,
            {{ currency_conversion('c.value', 'c.from_currency_code', 'currency') }},
            coalesce(email,'N/A') as email,
            estimated_taxes,
            financial_status,
            gateway,
            landing_site,
            landing_site_ref,
            a.name,
            number,
            order_number,
            order_status_url,
            payment_gateway_names,
            phone,
            presentment_currency,
            {{timezone_conversion("a.processed_at")}} as processed_at,
            processing_method,
            reference,
            referring_site,
            source_identifier,
            source_name,
            tags,
            taxes_included,
            test,
            token,
            {{timezone_conversion("a.updated_at")}} as updated_at,
            {{extract_nested_value("line_items","id","string")}} as line_items_id,
            {{extract_nested_value("line_items","admin_graphql_api_id","string")}} as line_items_admin_graphql_api_id,
            {{extract_nested_value("line_items","fulfillable_quantity","numeric")}} as line_items_fulfillable_quantity,
            {{extract_nested_value("line_items","fulfillment_service","string")}} as line_items_fulfillment_service,
            {{extract_nested_value("line_items","gift_card","boolean")}} as line_items_gift_card,
            {{extract_nested_value("line_items","grams","numeric")}} as line_items_grams,
            {{extract_nested_value("line_items","name","string")}} as line_items_name,
            {{extract_nested_value("line_items","price","numeric")}} as line_items_price,
            {{extract_nested_value("line_items","product_exists","boolean")}} as line_items_product_exists,
            {{extract_nested_value("line_items","product_id","string")}} as line_items_product_id,
            {{extract_nested_value("line_items","quantity","numeric")}} as line_items_quantity,
            {{extract_nested_value("line_items","requires_shipping","boolean")}} as line_items_requires_shipping,
            {{extract_nested_value("line_items","sku","string")}} as line_items_sku,
            {{extract_nested_value("line_items","taxable","boolean")}} as line_items_taxable,
            {{extract_nested_value("line_items","title","string")}} as line_items_title,
            {{extract_nested_value("line_items","total_discount","numeric")}} as line_items_total_discount,
            {{extract_nested_value("line_items","variant_id","string")}} as line_items_variant_id,
            {{extract_nested_value("line_items","variant_inventory_management","string")}} as line_items_variant_inventory_management,
            {{extract_nested_value("line_items","variant_title","string")}} as line_items_variant_title,
            {{extract_nested_value("tax_lines","price","numeric")}} as tax_lines_price,
            {{extract_nested_value("tax_lines","rate","numeric")}} as tax_lines_rate,
            coalesce({{extract_nested_value("tax_lines","title","string")}},'N/A') as tax_lines_title,
            {{extract_nested_value("tax_lines","channel_liable","boolean")}} as tax_lines_channel_liable,
            {{extract_nested_value("line_items","pre_tax_price","numeric")}} as line_items_pre_tax_price,
            {{extract_nested_value("line_items","tax_code","string")}} as line_items_tax_code,
            {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
            {{extract_nested_value("line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
            safe_cast(app_id as string) as app_id,
            customer_locale,
            note,
            {{timezone_conversion("a.closed_at")}} as closed_at,
            a.fulfillment_status,
            safe_cast(location_id as string) as location_id,
            cancel_reason,
            {{timezone_conversion("a.cancelled_at")}} as cancelled_at,
            safe_cast(user_id as string) as user_id,
            safe_cast(device_id as string) as device_id,
            a.{{ daton_user_id() }} as _daton_user_id,
            a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
            a.{{ daton_batch_id() }} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
        from {{ i }} a
            {% if var('currency_conversion_flag') %}
                left join {{ ref('ExchangeRates') }} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
            {{ unnesting("line_items") }}
            {{ multi_unnesting("line_items", "tax_lines") }}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where a.{{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_line_items_tax_lines_lookback') }},0) from {{ this }})
            {% endif %}
        qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}