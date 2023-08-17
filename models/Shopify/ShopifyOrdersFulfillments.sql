{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
    {{ set_table_name('%shopify%orders') }} 
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    select
        '{{ brand }}' as brand,
        '{{ store }}' as store,
        cast(a.id as string) as order_id,
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        currency,
        cast(current_subtotal_price as numeric) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        email,
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
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        cast(subtotal_price as numeric) as subtotal_price,
        tags,
        taxes_included,
        test,
        token,
        cast(total_discounts as numeric) as total_discounts,
        cast(total_line_items_price as numeric) as total_line_items_price,
        cast(total_outstanding as numeric) as total_outstanding,
        cast(total_price as numeric) as total_price,
        cast(total_price_usd as numeric) as total_price_usd,
        cast(total_tax as numeric) as total_tax,
        cast(total_tip_received as numeric) as total_tip_received,
        total_weight,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {% if target.type == 'snowflake' %}
            coalesce(fulfillments.value:id::varchar, 'N/A') as fulfillments_id,
            fulfillments.value:admin_graphql_api_id as fulfillments_admin_graphql_api_id,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.value:created_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_created_at,
            cast(fulfillments.value:location_id as string) as fulfillments_location_id,
            fulfillments.value:name as fulfillments_name,
            fulfillments.value:order_id::varchar as fulfillments_orders_id,
            receipt.value:testcase as receipt_testcase,
            receipt.value:authorization as receipt_authorization,
            receipt.value:gift_cards as receipt_gift_cards,
            fulfillments.value:service as fulfillments_service,
            fulfillments.value:status as fulfillments_status,
            fulfillments.value:tracking_company as fulfillments_tracking_company,
            fulfillments.value:tracking_number as fulfillments_tracking_number,
            fulfillments.value:tracking_numbers as fulfillments_tracking_numbers,
            fulfillments.value:tracking_url as fulfillments_tracking_url,
            fulfillments.value:tracking_urls as fulfillments_tracking_urls,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.value:updated_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_updated_at,
            fulfillments_line_items.value:id::string as line_items_id,
            fulfillments_line_items.value:admin_graphql_api_id::varchar as line_items_admin_graphql_api_id,
            fulfillments_line_items.value:fulfillable_quantity::varchar as line_items_fulfillable_quantity,
            fulfillments_line_items.value:fulfillment_service::varchar as line_items_fulfillment_service,
            fulfillments_line_items.value:gift_card::varchar as line_items_gift_card,
            fulfillments_line_items.value:grams::varchar as line_items_grams,
            fulfillments_line_items.value:name::varchar as line_items_name,
            fulfillments_line_items.value:price::float as line_items_price,
            fulfillments_line_items.value:price_set as line_items_price_set,
            fulfillments_line_items.value:product_exists::varchar as line_items_product_exists,
            fulfillments_line_items.value:product_id::varchar as line_items_product_id,
            fulfillments_line_items.value:properties::varchar as line_items_properties,
            fulfillments_line_items.value:quantity::float as line_items_quantity,
            fulfillments_line_items.value:requires_shipping::varchar as line_items_requires_shipping,
            fulfillments_line_items.value:sku::varchar as line_items_sku,
            fulfillments_line_items.value:taxable::varchar as line_items_taxable,
            fulfillments_line_items.value:title::varchar as line_items_title,
            fulfillments_line_items.value:total_discount::numeric as line_items_total_discount,
            fulfillments_line_items.value:total_discount_set as line_items_total_discount_set
            fulfillments_line_items.value:variant_id::varchar as line_items_variant_id,
            fulfillments_line_items.value:variant_inventory_management::varchar as line_items_variant_inventory_management,
            fulfillments_line_items.value:variant_title::varchar as line_items_variant_title,
            fulfillments_line_items.value:tax_lines as line_items_tax_lines,
            fulfillments_line_items.value:discount_allocations as line_items_discount_allocations,
            fulfillments_line_items.value:pre_tax_price_set as line_items_pre_tax_price_set
            fulfillments_line_items.value:fulfillment_status::varchar as line_items_fulfillment_status,
            cast(fulfillments_line_items.value:pre_tax_price as numeric) as line_items_pre_tax_price,
            fulfillments_line_items.value:tax_code as line_items_tax_code,
            fulfillments_line_items.value:vendor::varchar as line_items_vendor,
            fulfillments.value:shipment_status as fulfillments_shipment_status,
        {% else %}
            coalesce(cast(fulfillments.id as string), 'N/A') as fulfillments_id,
            fulfillments.admin_graphql_api_id as fulfillments_admin_graphql_api_id,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.created_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_created_at,
            cast(fulfillments.location_id as string) as fulfillments_location_id,
            fulfillments.name as fulfillments_name,
            cast(fulfillments.order_id as string) as fulfillments_orders_id,
            receipt.testcase as receipt_testcase,
            receipt.authorization as receipt_authorization,
            receipt.gift_cards as receipt_gift_cards,
            fulfillments.service as fulfillments_service,
            fulfillments.status as fulfillments_status,
            fulfillments.tracking_company as fulfillments_tracking_company,
            fulfillments.tracking_number as fulfillments_tracking_number,
            fulfillments.tracking_numbers as fulfillments_tracking_numbers,
            fulfillments.tracking_url as fulfillments_tracking_url,
            fulfillments.tracking_urls as fulfillments_tracking_urls,
            cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.updated_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_updated_at,
            cast(fulfillments_line_items.id as string) as line_items_id,
            fulfillments_line_items.admin_graphql_api_id as line_items_admin_graphql_api_id,
            fulfillments_line_items.fulfillable_quantity as line_items_fulfillable_quantity,
            fulfillments_line_items.fulfillment_service as line_items_fulfillment_service,
            fulfillments_line_items.gift_card as line_items_gift_card,
            fulfillments_line_items.grams as line_items_grams,
            fulfillments_line_items.name as line_items_name,
            cast(fulfillments_line_items.price as numeric) line_items_price,
            fulfillments_line_items.price_set as line_items_price_set,
            fulfillments_line_items.product_exists as line_items_product_exists,
            fulfillments_line_items.product_id as line_items_product_id,
            fulfillments_line_items.properties as line_items_properties,
            fulfillments_line_items.quantity as line_items_quantity,
            fulfillments_line_items.requires_shipping as line_items_requires_shipping,
            fulfillments_line_items.sku as line_items_sku,
            fulfillments_line_items.taxable as line_items_taxable,
            fulfillments_line_items.title as line_items_title,
            cast(fulfillments_line_items.total_discount as numeric) line_items_total_discount,
            fulfillments_line_items.total_discount_set as line_items_total_discount_set,
            fulfillments_line_items.variant_id as line_items_variant_id,
            fulfillments_line_items.variant_inventory_management as line_items_variant_inventory_management,
            fulfillments_line_items.variant_title as line_items_variant_title,
            fulfillments_line_items.tax_lines as line_items_tax_lines,
            fulfillments_line_items.discount_allocations as line_items_discount_allocations,
            fulfillments_line_items.pre_tax_price_set as line_items_pre_tax_price_set,
            fulfillments_line_items.fulfillment_status as line_items_fulfillment_status,
            cast(fulfillments_line_items.pre_tax_price as numeric) as line_items_pre_tax_price,
            fulfillments_line_items.tax_code as line_items_tax_code,
            fulfillments_line_items.vendor as line_items_vendor,
            fulfillments.shipment_status as fulfillments_shipment_status,
        {% endif %}
        app_id,
        customer_locale,
        note,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        a.fulfillment_status,
        a.location_id,
        cancel_reason,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        user_id,
        device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
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
    {% if target.type == 'snowflake' %}
        , lateral flatten(input => parse_json(fulfillments.value:"line_items")) as fulfillments_line_items
    {% else %}
        left join unnest(fulfillments.line_items) as fulfillments_line_items
    {% endif %}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where a.{{ daton_batch_runtime() }} >= {{ max_loaded }}
    {% endif %}

    qualify dense_rank() over (partition by a.id order by a._daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}