{% if var('ShopifyOrdersLineItems') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if var('currency_conversion_flag') %}
    -- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
    {%- set max_loaded_query -%}
        select coalesce(max(_daton_batch_runtime) - 2592000000, 0) from {{ this }}
    {%- endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
        {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {%- else %}
        {% set max_loaded = 0 %}
    {%- endif -%}
{% endif %}

{% set table_name_query %}
    {{ set_table_name('%shopify%orders') }} and lower(table_name) not like '%shopify%fulfillment_orders' 
    and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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
    '{{brand}}' as brand,
    '{{store}}' as store,
    coalesce(cast(a.id as string), 'N/A') as order_id, 
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
    coalesce(email, 'N/A') as email,
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
    a.phone,
    presentment_currency,
    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} 
        as {{ dbt.type_timestamp() }}) as processed_at,
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
    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} 
        as {{ dbt.type_timestamp() }}) as updated_at,
    {% if target.type == 'snowflake' %}
    billing_address.value:first_name::varchar as billing_address_first_name,
    billing_address.value:address1::varchar as billing_address_address1,
    billing_address.value:phone::varchar as billing_address_phone,
    billing_address.value:city::varchar as billing_address_city,
    billing_address.value:zip::varchar as billing_address_zip,
    billing_address.value:province::varchar as billing_address_province,
    billing_address.value:country::varchar as billing_address_country,
    billing_address.value:last_name::varchar as billing_address_last_name,
    billing_address.value:address2::varchar as billing_address_address2,
    billing_address.value:latitude::varchar as billing_address_latitude,
    billing_address.value:longitude::varchar as billing_address_longitude,
    billing_address.value:name::varchar as billing_address_name,
    billing_address.value:country_code::varchar as billing_address_country_code,
    billing_address.value:province_code::varchar as billing_address_province_code,
    billing_address.value:longitude_bn as billing_address_longitude_bn,
    billing_address.value:latitude_bn as billing_address_latitude_bn,
    billing_address.value:company::varchar as billing_address_company,
    {% else %}
    billing_address.first_name as billing_address_first_name,
    billing_address.address1 as billing_address_address1,
    billing_address.phone as billing_address_phone,
    billing_address.city as billing_address_city,
    billing_address.zip as billing_address_zip,
    billing_address.province as billing_address_province,
    billing_address.country as billing_address_country,
    billing_address.last_name as billing_address_last_name,
    billing_address.address2 as billing_address_address2,
    billing_address.latitude as billing_address_latitude,
    billing_address.longitude as billing_address_longitude,
    billing_address.name as billing_address_name,
    billing_address.country_code as billing_address_country_code,
    billing_address.province_code as billing_address_province_code,
    billing_address.longitude_bn as billing_address_longitude_bn,
    billing_address.latitude_bn as billing_address_latitude_bn,
    billing_address.company as billing_address_company,
    {% endif %}

    {% if target.type == 'snowflake' %}
    line_items.value:id::string as line_items_id,
    line_items.value:admin_graphql_api_id::varchar as line_items_admin_graphql_api_id,
    line_items.value:fulfillable_quantity::varchar as line_items_fulfillable_quantity,
    line_items.value:fulfillment_service::varchar as line_items_fulfillment_service,
    line_items.value:gift_card::varchar as line_items_gift_card,
    line_items.value:grams::varchar as line_items_grams, 
    line_items.value:name::varchar as line_items_name,
    line_items.value:price::float as line_items_price,
    line_items.value:price_set as line_items_price_set,
    line_items.value:product_exists::varchar as line_items_product_exists,
    line_items.value:product_id::string as line_items_product_id,
    line_items.value:properties::varchar as line_items_properties,
    line_items.value:quantity::float as line_items_quantity,
    line_items.value:requires_shipping::varchar as line_items_requires_shipping,
    line_items.value:sku::varchar as line_items_sku,
    line_items.value:taxable::varchar as line_items_taxable,
    line_items.value:title::varchar as line_items_title,
    line_items.value:tax_lines as line_items_tax_lines,
    line_items.value:discount_allocations as line_items_discount_allocations,
    line_items.value:pre_tax_price_set as line_items_pre_tax_price_set,
    line_items.value:total_discount::numeric as line_items_total_discount,
    line_items.value:total_discount_set as line_items_total_discount_set,
    line_items.value:variant_id::string as line_items_variant_id,
    line_items.value:variant_inventory_management::varchar as line_items_variant_inventory_management,
    line_items.value:variant_title::varchar as line_items_variant_title,
    cast(line_items.value:pre_tax_price as numeric) as line_items_pre_tax_price,
    line_items.value:tax_code as line_items_tax_code,
    line_items.value:vendor::varchar as line_items_vendor,
    line_items.value:fulfillment_status::varchar as line_items_fulfillment_status,
    {% else %}
    cast(line_items.id as string) as line_items_id,
    line_items.admin_graphql_api_id as line_items_admin_graphql_api_id,
    line_items.fulfillable_quantity as line_items_fulfillable_quantity,
    line_items.fulfillment_service as line_items_fulfillment_service,
    line_items.gift_card as line_items_gift_card,
    line_items.grams as line_items_grams, 
    line_items.name as line_items_name,
    cast(line_items.price as numeric) line_items_price,
    line_items.price_set as line_items_price_set,
    line_items.product_exists as line_items_product_exists,
    cast(line_items.product_id as string) as line_items_product_id,
    line_items.properties as line_items_properties,
    line_items.quantity as line_items_quantity,
    line_items.requires_shipping as line_items_requires_shipping,
    line_items.sku as line_items_sku,
    line_items.taxable as line_items_taxable,
    line_items.title as line_items_title,
    line_items.tax_lines as line_items_tax_lines,
    line_items.discount_allocations as line_items_discount_allocations,
    line_items.pre_tax_price_set as line_items_pre_tax_price_set,
    cast(line_items.total_discount as numeric) line_items_total_discount,
    line_items.total_discount_set as line_items_total_discount_set,
    cast(line_items.variant_id as string) as line_items_variant_id,
    line_items.variant_inventory_management as line_items_variant_inventory_management,
    line_items.variant_title as line_items_variant_title,
    cast(line_items.pre_tax_price as numeric) as line_items_pre_tax_price,
    line_items.tax_code as line_items_tax_code,
    line_items.vendor as line_items_vendor,
    line_items.fulfillment_status as line_items_fulfillment_status,
    {% endif %}

    {% if target.type == 'snowflake' %}
    shipping_address.value:first_name::varchar as shipping_address_first_name,
    shipping_address.value:address1::varchar as shipping_address_address1,
    shipping_address.value:phone::varchar as shipping_address_phone,
    shipping_address.value:city::varchar as shipping_address_city,
    shipping_address.value:zip::varchar as shipping_address_zip,
    shipping_address.value:province::varchar as shipping_address_province,
    shipping_address.value:country::varchar as shipping_address_country,
    shipping_address.value:last_name::varchar as shipping_address_last_name,
    shipping_address.value:address2::varchar as shipping_address_address2,
    shipping_address.value:latitude::varchar as shipping_address_latitude,
    shipping_address.value:longitude::varchar as shipping_address_longitude,
    shipping_address.value:name::varchar as shipping_address_name,
    shipping_address.value:country_code::varchar as shipping_address_country_code,
    shipping_address.value:province_code::varchar as shipping_address_province_code,
    shipping_address.value:latitude_bn as shipping_address_latitude_bn,
    shipping_address.value:longitude_bn as shipping_address_longitude_bn,
    shipping_address.value:company::varchar as shipping_address_company,
    {% else %}
    shipping_address.first_name as shipping_address_first_name,
    shipping_address.address1 as shipping_address_address1,
    shipping_address.phone as shipping_address_phone,
    shipping_address.city as shipping_address_city,
    shipping_address.zip as shipping_address_zip,
    shipping_address.province as shipping_address_province,
    shipping_address.country as shipping_address_country,
    shipping_address.last_name as shipping_address_last_name,
    shipping_address.address2 as shipping_address_address2,
    shipping_address.latitude as shipping_address_latitude,
    shipping_address.longitude as shipping_address_longitude,
    shipping_address.name as shipping_address_name,
    shipping_address.country_code as shipping_address_country_code,
    shipping_address.province_code as shipping_address_province_code,
    shipping_address.latitude_bn as shipping_address_latitude_bn,
    shipping_address.longitude_bn as shipping_address_longitude_bn,
    shipping_address.company as shipping_address_company,
    {% endif %}

    cast(app_id as string) as app_id,
    customer_locale,
    note,
    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} 
        as {{ dbt.type_timestamp() }}) as closed_at,
    a.fulfillment_status,
    cast(location_id as string) as location_id,
    cancel_reason,
    cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} 
        as {{ dbt.type_timestamp() }}) as cancelled_at,
    cast(user_id as string) as user_id,
    cast(device_id as string) as device_id,
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
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id,
from {{ i }} a
{% if var('currency_conversion_flag') %}
    left join {{ ref('ExchangeRates') }} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
{% endif %}
{{ unnesting("line_items") }}
{{ unnesting("billing_address") }}
{{ unnesting("shipping_address") }}
{% if is_incremental() %}
    {# /* -- this filter will only be applied on an incremental run */ #}
    where a.{{ daton_batch_runtime() }} >= {{ max_loaded }}
{% endif %}

qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %}