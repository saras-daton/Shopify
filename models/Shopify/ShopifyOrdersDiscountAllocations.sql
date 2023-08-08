{% if var('ShopifyOrdersDiscountAllocations') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%shopify%orders')}} and lower(table_name) not like '%shopify%fulfillment_orders' and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        coalesce(cast(a.id as string),'N/A') as id, 
        a.admin_graphql_api_id,
        browser_ip,
        buyer_accepts_marketing,
        cart_token,
        cast(checkout_id as string) as checkout_id,
        checkout_token,
        client_details,
        confirmed,
        contact_email,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        currency,
        cast(current_subtotal_price as numeric) as current_subtotal_price,
        cast(current_total_discounts as numeric) as current_total_discounts,
        cast(current_total_price as numeric) as current_total_price,
        cast(current_total_tax as numeric) as current_total_tax,
        discount_codes,
        coalesce(email,'N/A') as email,
        estimated_taxes,
        financial_status,
        gateway,
        landing_site,
        landing_site_ref,
        a.name,
        note_attributes,
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
        a.tax_lines,
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
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        {% if target.type =='snowflake' %}
        line_items.value:id::varchar as line_items_id,
        line_items.value:admin_graphql_api_id::varchar as line_items_admin_graphql_api_id,
        line_items.value:fulfillable_quantity::varchar as line_items_fulfillable_quantity,
        line_items.value:fulfillment_service::varchar as line_items_fulfillment_service,
        line_items.value:gift_card::varchar as line_items_gift_card,
        line_items.value:grams::varchar as line_items_grams, 
        line_items.value:name::varchar as line_items_name,
        line_items.value:price::FLOAT as line_items_price,
        line_items.value:price_set as line_items_price_set,
        line_items.value:product_exists::varchar as line_items_product_exists,
        line_items.value:product_id::varchar as line_items_product_id,
        line_items.value:properties::varchar as line_items_properties,
        line_items.value:quantity::FLOAT as line_items_quantity,
        line_items.value:requires_shipping::varchar as line_items_requires_shipping,
        line_items.value:sku::varchar as line_items_sku,
        line_items.value:taxable::varchar as line_items_taxable,
        line_items.value:title::varchar as line_items_title,
        line_items.value:total_discount::numeric as line_items_total_discount,
        line_items.value:total_discount_set as line_items_total_discount_set,
        line_items.value:variant_id::varchar as line_items_variant_id,
        line_items.value:variant_inventory_management::varchar as line_items_variant_inventory_management,
        line_items.value:variant_title::varchar as line_items_variant_title,
        line_items.value:tax_lines as line_items_tax_lines,
        cast(discount_allocations.value:amount as numeric) as discount_allocations_amount,
        cast(shop_money.value:amount as numeric) as shop_money_amount,
        shop_money.value:currency_code as shop_money_currency_code,
        cast(presentment_money.value:amount as numeric) as presentment_money_amount,
        presentment_money.value:currency_code as presentment_money_currency_code,
        discount_allocations.value:discount_application_index::varchar as discount_application_index,
        line_items.value:pre_tax_price_set as line_items_pre_tax_price_set,
        line_items.value:pre_tax_price as line_items_pre_tax_price,
        line_items.value:tax_code as line_items_tax_code,
        line_items.value:vendor::varchar as vendor,
        line_items.value:fulfillment_status::varchar as line_items_fulfillment_status,
        {% else %}
        line_items.id as line_items_id,
        line_items.admin_graphql_api_id as line_items_admin_graphql_api_id,
        line_items.fulfillable_quantity as line_items_fulfillable_quantity,
        line_items.fulfillment_service as line_items_fulfillment_service,
        line_items.gift_card as line_items_gift_card,
        line_items.grams as line_items_grams, 
        line_items.name as line_items_name,
        cast(line_items.price as numeric) line_items_price,
        line_items.price_set as line_items_price_set,
        line_items.product_exists as line_items_product_exists,
        line_items.product_id as line_items_product_id,
        line_items.properties as line_items_properties,
        line_items.quantity as line_items_quantity,
        line_items.requires_shipping as line_items_requires_shipping,
        line_items.sku as line_items_sku,
        line_items.taxable as line_items_taxable,
        line_items.title as line_items_title,
        cast(line_items.total_discount as numeric) line_items_total_discount,
        line_items.total_discount_set as line_items_total_discount_set,
        line_items.variant_id as line_items_variant_id,
        line_items.variant_inventory_management as line_items_variant_inventory_management,
        line_items.variant_title as line_items_variant_title,
        line_items.tax_lines as line_items_tax_lines,
        cast(discount_allocations.amount as numeric) as discount_allocations_amount,
        cast(shop_money.amount as numeric) as shop_money_amount,
        shop_money.currency_code as shop_money_currency_code,
        cast(presentment_money.amount as numeric) as presentment_money_amount,
        presentment_money.currency_code as presentment_money_currency_code,
        discount_allocations.discount_application_index as discount_application_index,
        line_items.pre_tax_price_set as line_items_pre_tax_price_set,
        line_items.pre_tax_price as line_items_pre_tax_price,
        line_items.tax_code as line_items_tax_code,
        line_items.vendor as vendor,
        line_items.fulfillment_status as line_items_fulfillment_status,
        {% endif %}
        cast(app_id as string) as app_id,
        customer_locale,
        note,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
        a.fulfillment_status,
        cast(location_id as string) as location_id,
        cancel_reason,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
        cast(user_id as string) as user_id,
        cast(device_id as string) as device_id,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        from {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
            {% endif %}
                {{unnesting("line_items")}}
                {{multi_unnesting("line_items","discount_allocations")}}
                {{multi_unnesting("discount_allocations","amount_set")}}
                {{multi_unnesting("amount_set","shop_money")}}
                {{multi_unnesting("amount_set","presentment_money")}}
            {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        qualify dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %}
