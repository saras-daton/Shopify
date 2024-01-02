{% if var('ShopifyShop') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_shop_tbl_ptrn','%shopify%shop','shopify_shop_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
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
        {{timezone_conversion("created_at")}} as created_at,
        {{timezone_conversion("updated_at")}} as updated_at,
        country_code,
        country_name,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'a.currency') }},
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
        cast(primary_location_id as string) as primary_location_id,
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
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from  {{i}} a
                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(a.updated_at) = c.date and a.currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_shop_lookback') }},0) from {{ this }})
                {% endif %}
        qualify dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) = 1
        

    {% if not loop.last %} union all {% endif %}
{% endfor %}
