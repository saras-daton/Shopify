{% if var('ShopifyAbandonedCheckouts') %}
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
    {{set_table_name('%shopify%abandoned_checkouts')}} and lower(table_name) not like '%googleanalytics%'
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
        '{{brand}}' as brand,
        '{{store}}' as store,
        cast(id as string) as id,
        token,
        cart_token,
        email,
        buyer_accepts_marketing,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(created_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as created_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(updated_at as timestamp)") }} as {{ dbt.type_timestamp() }}) as updated_at,
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
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="completed_at") }} as {{ dbt.type_timestamp() }}) as completed_at,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            currency as exchange_currency_code, 
        {% endif %} 
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
    from  {{i}} a
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    qualify row_number() over (partition by a.id order by {{daton_batch_runtime()}} desc)= 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
