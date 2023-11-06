{% if var('ShopifyOrdersDiscountApplications') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% if var('currency_conversion_flag') %}
    -- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_orders_discount_applications_tbl_ptrn'),
exclude=var('shopify_orders_discount_applications_exclude_tbl_ptrn'),
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

    select *, row_number() over (partition by order_id order by _daton_batch_runtime desc) _seq_id
    from (
        select 
            '{{ brand }}' as brand,
            '{{ store }}' as store,
            safe_cast(a.id as string) as order_id, 
            admin_graphql_api_id,
            browser_ip,
            buyer_accepts_marketing,
            cart_token,
            safe_cast(checkout_id as string) as checkout_id,
            checkout_token,
            confirmed,
            contact_email,
            safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
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
            name,
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
            cast(subtotal_price as numeric) as subtotal_price,
            tags,
            taxes_included,
            test,
            token,
            cast(total_discounts as numeric) as total_discounts,
            cast(total_outstanding as numeric) as total_outstanding,
            cast(total_price as numeric) as total_price,
            cast(total_price_usd as numeric) as total_price_usd,
            cast(total_tax as numeric) as total_tax,
            cast(total_tip_received as numeric) as total_tip_received,
            total_weight,
            cast(total_line_items_price as numeric) as total_line_items_price,
            safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
            {{extract_nested_value("discount_applications","target_type","string")}} as discount_applications_target_type,
            {{extract_nested_value("discount_applications","type","string")}} as discount_applications_type,
            {{extract_nested_value("discount_applications","value","string")}} as discount_applications_value,
            {{extract_nested_value("discount_applications","value_type","string")}} as discount_applications_value_type,
            {{extract_nested_value("discount_applications","allocation_method","string")}} as discount_applications_allocation_method,
            {{extract_nested_value("discount_applications","target_selection","string")}} as discount_applications_target_selection,
            {{extract_nested_value("discount_applications","code","string")}} as discount_applications_code,
            {{extract_nested_value("discount_applications","title","string")}} as discount_applications_title,
            {{extract_nested_value("discount_applications","description","string")}} as discount_applications_description,
            safe_cast(app_id as string) as app_id,
            customer_locale,
            note,
            safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as closed_at,
            fulfillment_status,
            safe_cast(location_id as string) as location_id,
            cancel_reason,
            safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="closed_at") }} as {{ dbt.type_timestamp() }}) as cancelled_at,
            safe_cast(user_id as string) as user_id,
            safe_cast(device_id as string) as device_id,
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.currency else c.from_currency_code end as exchange_currency_code,
            {% else %}
                safe_cast(1 as decimal) as exchange_currency_rate,
                a.currency as exchange_currency_code,
            {% endif %}
            a.{{daton_user_id()}} as _daton_user_id,
            a.{{daton_batch_runtime()}} as _daton_batch_runtime,
            a.{{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}} a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.created_at) = c.date and a.currency = c.to_currency_code
        {% endif %}
        {{ unnesting("discount_applications") }}
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_orders_discount_applications_lookback') }},0) from {{ this }})
        {% endif %}
    
    qualify dense_rank() over (partition by a.id order by a.{{daton_batch_runtime()}} desc) = 1
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}