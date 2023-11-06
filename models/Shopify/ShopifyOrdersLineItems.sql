{% if var('ShopifyOrdersLineItems') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if var('currency_conversion_flag') %}
    -- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_orders_line_items_tbl_ptrn'),
exclude=var('shopify_orders_line_items_exclude_tbl_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
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
    coalesce(safe_cast(a.id as string), 'N/A') as order_id, 
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
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.processed_at") }} as {{ dbt.type_timestamp() }}) as processed_at,
    processing_method,
    reference,
    referring_site,
    source_identifier,
    source_name,
    tags,
    taxes_included,
    test,
    token,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
    {{extract_nested_value("billing_address","first_name","string")}} as billing_address_first_name,
    {{extract_nested_value("billing_address","address1","string")}} as billing_address_address1,
    {{extract_nested_value("billing_address","phone","string")}} as billing_address_phone,
    {{extract_nested_value("billing_address","city","string")}} as billing_address_city,
    {{extract_nested_value("billing_address","zip","string")}} as billing_address_zip,
    {{extract_nested_value("billing_address","province","string")}} as billing_address_province,
    {{extract_nested_value("billing_address","country","string")}} as billing_address_country,
    {{extract_nested_value("billing_address","last_name","string")}} as billing_address_last_name,
    {{extract_nested_value("billing_address","address2","string")}} as billing_address_address2,
    {{extract_nested_value("billing_address","latitude","numeric")}} as billing_address_latitude,
    {{extract_nested_value("billing_address","longitude","numeric")}} as billing_address_longitude,
    {{extract_nested_value("billing_address","name","string")}} as billing_address_name,
    {{extract_nested_value("billing_address","country_code","string")}} as billing_address_country_code,
    {{extract_nested_value("billing_address","province_code","string")}} as billing_address_province_code,
    {{extract_nested_value("billing_address","longitude_bn","bignumeric")}} as billing_address_longitude_bn,
    {{extract_nested_value("billing_address","latitude_bn","bignumeric")}} as billing_address_latitude_bn,
    {{extract_nested_value("billing_address","company","string")}} as billing_address_company,
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
    {{extract_nested_value("line_items","pre_tax_price","numeric")}} as line_items_pre_tax_price,
    {{extract_nested_value("line_items","tax_code","string")}} as line_items_tax_code,
    {{extract_nested_value("line_items","vendor","string")}} as line_items_vendor,
    {{extract_nested_value("line_items","fulfillment_status","string")}} as line_items_fulfillment_status,
    {{extract_nested_value("shipping_address","first_name","string")}} as shipping_address_first_name,
    {{extract_nested_value("shipping_address","address1","string")}} as shipping_address_address1,
    {{extract_nested_value("shipping_address","phone","string")}} as shipping_address_phone,
    {{extract_nested_value("shipping_address","city","string")}} as shipping_address_city,
    {{extract_nested_value("shipping_address","zip","string")}} as shipping_address_zip,
    {{extract_nested_value("shipping_address","province","string")}} as shipping_address_province,
    {{extract_nested_value("shipping_address","country","string")}} as shipping_address_country,
    {{extract_nested_value("shipping_address","last_name","string")}} as shipping_address_last_name,
    {{extract_nested_value("shipping_address","address2","string")}} as shipping_address_address2,
    {{extract_nested_value("shipping_address","latitude","numeric")}} as shipping_address_latitude,
    {{extract_nested_value("shipping_address","longitude","numeric")}} as shipping_address_longitude,
    {{extract_nested_value("shipping_address","name","string")}} as shipping_address_name,
    {{extract_nested_value("shipping_address","country_code","string")}} as shipping_address_country_code,
    {{extract_nested_value("shipping_address","province_code","string")}} as shipping_address_province_code,
    {{extract_nested_value("shipping_address","latitude_bn","bignumeric")}} as shipping_address_latitude_bn,
    {{extract_nested_value("shipping_address","longitude_bn","bignumeric")}} as shipping_address_longitude_bn,
    {{extract_nested_value("shipping_address","company","string")}} as shipping_address_company,
    safe_cast(app_id as string) as app_id,
    customer_locale,
    note,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} 
        as {{ dbt.type_timestamp() }}) as closed_at,
    a.fulfillment_status,
    safe_cast(location_id as string) as location_id,
    cancel_reason,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cancelled_at") }} 
        as {{ dbt.type_timestamp() }}) as cancelled_at,
    safe_cast(user_id as string) as user_id,
    safe_cast(device_id as string) as device_id,
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
    {{ unnesting("line_items") }}
    {{ unnesting("billing_address") }}
    {{ unnesting("shipping_address") }}
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where a.{{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_line_items_lookback') }},0) from {{ this }})
    {% endif %}

qualify dense_rank() over (partition by a.id order by a.{{ daton_batch_runtime() }} desc) = 1
{% if not loop.last %} union all {% endif %}
{% endfor %}