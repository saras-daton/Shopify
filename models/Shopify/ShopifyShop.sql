{% if var('ShopifyShop') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
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
{{set_table_name('%shopify%shop')}} and lower(table_name) not like '%googleanalytics%' and lower(table_name) not like 'v1%'
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(id as string) id,
        name,
        email,
        domain,
        province,
        country,
        address1,
        zip,
        city,
        phone,
        latitude,
        longitude,
        primary_locale,
        address2,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        country_code,
        country_name,
        currency,
        customer_email,
        timezone,
        iana_timezone,
        shop_owner,
        money_format,
        money_with_currency_format,
        weight_unit,
        province_code,
        county_taxes,
        plan_display_name,
        plan_name,
        has_discounts,
        has_gift_cards,
        myshopify_domain,
        money_in_emails_format,
        money_with_currency_in_emails_format,
        eligible_for_payments,
        requires_extra_payments_agreement,
        password_enabled,
        has_storefront,
        finances,
        primary_location_id,
        cookie_consent_level,
        visitor_tracking_consent_preference,
        checkout_api_supported,
        multi_location_enabled,
        setup_required,
        pre_launch_enabled,
        enabled_presentment_currencies,
        transactional_sms_disabled,
        marketing_sms_consent_enabled_at_checkout,
        taxes_included,
        auto_configure_tax_inclusivity,
        tax_shipping,
        google_apps_domain,
        google_apps_login_enabled,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        qualify
        dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) = 1
        

    {% if not loop.last %} union all {% endif %}
{% endfor %}
