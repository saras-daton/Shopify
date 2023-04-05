{% if var('ShopifyCustomersAddresses') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%shopify%customers')}}    
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

    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        tags,
        a.phone,
        orders_count,
        total_spent,
        multipass_identifier,
        accepts_marketing,
        {% if target.type =='snowflake' %}
        addresses.VALUE:province_code::VARCHAR as addresses_province_code,
        addresses.VALUE:name::VARCHAR as addresses_name,
        addresses.VALUE:default::VARCHAR as addresses_default,
        addresses.VALUE:customer_id::VARCHAR as addresses_customer_id,
        addresses.VALUE:first_name::VARCHAR as addresses_first_name,
        addresses.VALUE:last_name::VARCHAR as addresses_last_name,
        addresses.VALUE:company::VARCHAR as addresses_company,
        addresses.VALUE:country_name::VARCHAR as addresses_country_name,
        COALESCE(addresses.VALUE:id::VARCHAR,'') as addresses_id,
        addresses.VALUE:address1::VARCHAR as addresses_address1,
        addresses.VALUE:country_code::VARCHAR as addresses_country_code,
        addresses.VALUE:address2::VARCHAR as addresses_address2,
        addresses.VALUE:zip::VARCHAR as addresses_zip,
        addresses.VALUE:city::VARCHAR as addresses_city,
        addresses.VALUE:phone::VARCHAR as addresses_phone,
        addresses.VALUE:province::VARCHAR as addresses_province,
        addresses.VALUE:country::VARCHAR as addresses_country,
        verified_email,
        a.state,
        CAST(a.created_at as timestamp) customers_created_at,
        default_address.VALUE:province_code::VARCHAR as default_address_province_code,
        default_address.VALUE:name::VARCHAR as default_address_name,
        default_address.VALUE:default::VARCHAR as default_address_default,
        default_address.VALUE:customer_id::VARCHAR as default_address_customer_id,
        default_address.VALUE:first_name::VARCHAR as default_address_first_name,
        default_address.VALUE:last_name::VARCHAR as default_address_last_name,
        default_address.VALUE:company::VARCHAR as default_address_company,
        default_address.VALUE:country_name::VARCHAR as default_address_country_name,
        default_address.VALUE:id::VARCHAR as default_address_id,
        default_address.VALUE:address1::VARCHAR as default_address_address1,
        default_address.VALUE:country_code::VARCHAR as default_address_country_code,
        default_address.VALUE:address2::VARCHAR as default_address_address2,
        default_address.VALUE:zip::VARCHAR as default_address_zip,
        default_address.VALUE:city::VARCHAR as default_address_city,
        default_address.VALUE:phone::VARCHAR as default_address_phone,
        default_address.VALUE:province::VARCHAR as default_address_province,
        default_address.VALUE:country::VARCHAR as default_address_country,
        {% else %}
        addresses.province_code as addresses_province_code,
        addresses.name as addresses_name,
        addresses.default as addresses_default,
        addresses.customer_id as addresses_customer_id,
        addresses.first_name as addresses_first_name,
        addresses.last_name as addresses_last_name,
        addresses.company as addresses_company,
        addresses.country_name as addresses_country_name,
        COALESCE(cast(addresses.id as string),'') as addresses_id,
        addresses.address1 as addresses_address1,
        addresses.country_code as addresses_country_code,
        addresses.address2 as addresses_address2,
        addresses.zip as addresses_zip,
        addresses.city as addresses_city,
        addresses.phone as addresses_phone,
        addresses.province as addresses_province,
        addresses.country as addresses_country,
        verified_email,
        state,
        CAST(a.created_at as timestamp) customers_created_at,
        default_address.province_code as default_address_province_code,
        default_address.name as default_address_name,
        default_address.default as default_address_default,
        default_address.customer_id as default_address_customer_id,
        default_address.first_name as default_address_first_name,
        default_address.last_name as default_address_last_name,
        default_address.company as default_address_company,
        default_address.country_name as default_address_country_name,
        default_address.id as default_address_id,
        default_address.address1 as default_address_address1,
        default_address.country_code as default_address_country_code,
        default_address.address2 as default_address_address2,
        default_address.zip as default_address_zip,
        default_address.city as default_address_city,
        default_address.phone as default_address_phone,
        default_address.province as default_address_province,
        default_address.country as default_address_country,
        {% endif %}
        note,
        email,
        a.first_name,
        CAST(a.updated_at as {{ dbt.type_timestamp() }}) as updated_at,
        last_order_id,
        COALESCE(cast(a.id as string),'') as customers_id,
        a.last_name,
        tax_exempt,
        last_order_name,
        a.admin_graphql_api_id,
        metafields,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by {{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {{unnesting("addresses")}} 
                {{unnesting("default_address")}} 
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
