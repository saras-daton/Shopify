{% if var('ShopifyFulfillmentOrders') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
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
    {{ set_table_name('%shopify%orders') }} and lower(table_name) not like '%shopify%fulfillment_orders'
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
        safe_cast(a.id as string) as order_id,
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        safe_cast(checkout_id as string) as checkout_id,
        checkout_token,
        confirmed,
        contact_email,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        currency,
        safe_cast(current_subtotal_price as numeric) as current_subtotal_price,
        safe_cast(current_total_discounts as numeric) as current_total_discounts,
        safe_cast(current_total_price as numeric) as current_total_price,
        safe_cast(current_total_tax as numeric) as current_total_tax,
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
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
        processing_method,
        reference,
        referring_site,
        source_identifier,
        source_name,
        safe_cast(subtotal_price as numeric) as subtotal_price,
        tags,
        taxes_included,
        test,
        token,
        safe_cast(total_discounts as numeric) as total_discounts,
        safe_cast(total_line_items_price as numeric) as total_line_items_price,
        safe_cast(total_outstanding as numeric) as total_outstanding,
        safe_cast(total_price as numeric) as total_price,
        safe_cast(total_price_usd as numeric) as total_price_usd,
        safe_cast(total_tax as numeric) as total_tax,
        safe_cast(total_tip_received as numeric) as total_tip_received,
        total_weight,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {% if target.type == 'snowflake' %}
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.value:created_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_created_at,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.value:updated_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_updated_at,
        {% else %}
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.created_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_created_at,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="fulfillments.updated_at") }} as {{ dbt.type_timestamp() }}) as fulfillments_updated_at,
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
        coalesce({{extract_nested_value("line_items","id","string")}},'N/A') as line_items_id,
        {{extract_nested_value("line_items","admin_graphql_api_id","string")}} as line_items_admin_graphql_api_id,
        {{extract_nested_value("line_items","fulfillable_quantity","numeric")}} as line_items_fulfillable_quantity,
        {{extract_nested_value("line_items","fulfillment_service","string")}} as line_items_fulfillment_service,
        {{extract_nested_value("line_items","gift_card","string")}} as line_items_gift_card,
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
        {{extract_nested_value("line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
        {{extract_nested_value("line_items","pre_tax_price","numeric")}} as line_items_pre_tax_price,
        {{extract_nested_value("line_items","tax_code","string")}} as line_items_tax_code,
        {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
        {{extract_nested_value("fulfillments","shipment_status","string")}} as fulfillments_shipment_status,
        safe_cast(app_id as string) app_id,
        customer_locale,
        note,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        a.fulfillment_status,
        safe_cast(a.location_id as string) location_id,
        cancel_reason,
        safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        safe_cast(user_id as string) user_id,
        safe_cast(device_id as string) device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            safe_cast(1 as decimal) as exchange_currency_rate,
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
        {{ multi_unnesting("fulfillments", "line_items") }}

        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{ daton_batch_runtime() }} >= {{ max_loaded }}
        {% endif %}

    qualify dense_rank() over (partition by a.id order by a._daton_batch_runtime desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}