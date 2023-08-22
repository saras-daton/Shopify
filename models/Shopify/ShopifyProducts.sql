{% if var('ShopifyProducts') %}
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
{{set_table_name('%shopify%products')}} 
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
    safe_cast(a.id as string) id,
    a.title,
    body_html,
    vendor,
    product_type,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
    handle,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="a.updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
    safe_cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="published_at") }} as {{ dbt.type_timestamp() }}) as published_at,
    published_scope,
    tags,
    a.admin_graphql_api_id,
    {{extract_nested_value("variants","id","string")}} as variants_id,
    {{extract_nested_value("variants","product_id","string")}} as variants_product_id,
    {{extract_nested_value("variants","title","string")}} as variants_title,
    {{extract_nested_value("variants","price","numeric")}} as variants_price,
    {{extract_nested_value("variants","sku","string")}} as variants_sku,
    {{extract_nested_value("variants","position","numeric")}} as variants_position,
    {{extract_nested_value("variants","inventory_policy","string")}} as variants_inventory_policy,
    {{extract_nested_value("variants","fulfillment_service","string")}} as variants_fulfillment_service,
    {{extract_nested_value("variants","inventory_management","string")}} as variants_inventory_management,
    {{extract_nested_value("variants","option1","string")}} as variants_option1,
    {{extract_nested_value("variants","created_at","timestamp")}} as variants_created_at,
    {{extract_nested_value("variants","updated_at","timestamp")}} as variants_updated_at,
    {{extract_nested_value("variants","taxable","boolean")}} as variants_taxable,
    {{extract_nested_value("variants","barcode","string")}} as variants_barcode,
    {{extract_nested_value("variants","grams","numeric")}} as variants_grams,
    {{extract_nested_value("variants","image_id","string")}} as variants_image_id,
    {{extract_nested_value("variants","weight","numeric")}} as variants_weight,
    {{extract_nested_value("variants","weight_unit","string")}} as variants_weight_unit,
    {{extract_nested_value("variants","inventory_item_id","string")}} as variants_inventory_item_id,
    {{extract_nested_value("variants","inventory_quantity","numeric")}} as variants_inventory_quantity,
    {{extract_nested_value("variants","old_inventory_quantity","numeric")}} as variants_old_inventory_quantity,
    {{extract_nested_value("price","amount","numeric")}} as variants_presentment_prices_price_amount,
    {{extract_nested_value("price","currency_code","string")}} as variants_presentment_prices_price_currency_code,
    template_suffix,
    status,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
            {{unnesting("variants")}}
            {{multi_unnesting('variants','presentment_prices')}}
            {{multi_unnesting('presentment_prices','price')}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}} 
            {% endif %}
    qualify dense_rank() over (partition by a.id order by _daton_batch_runtime desc) = 1
        
    {% if not loop.last %} union all {% endif %}
{% endfor %}
