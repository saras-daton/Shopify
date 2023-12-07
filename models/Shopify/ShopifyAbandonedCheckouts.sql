{% if var('ShopifyAbandonedCheckouts') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("shopify_abandoned_checkouts_tbl_ptrn","shopify_abandoned_checkouts_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        cast(id as string) as id,
        token,
        cart_token,
        email,
        buyer_accepts_marketing,
        {{timezone_conversion("created_at")}} as created_at,
        {{timezone_conversion("updated_at")}} as updated_at,
        taxes_included,
        total_weight,
        currency,
        customer_locale,
        name,
        abandoned_checkout_url,
        source_name,
        presentment_currency,
        buyer_accepts_sms_marketing,
        cast(total_discounts as numeric) as total_discounts,
        cast(total_line_items_price as numeric) as total_line_items_price,
        cast(total_price as numeric) as total_price,
        cast(total_tax as numeric) as total_tax,
        cast(subtotal_price as numeric) as subtotal_price,
        landing_site,
        referring_site,
        gateway,
        total_duties,
        cast(user_id as string) as user_id,
        cast(location_id as string) as location_id,
        source_identifier,
        cast(device_id as string) as device_id,
        {{timezone_conversion("completed_at")}} as completed_at,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from  {{i}} a
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_abandoned_checkouts_lookback') }},0) from {{ this }})
            {% endif %}
    qualify row_number() over (partition by a.id order by {{daton_batch_runtime()}} desc)= 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
