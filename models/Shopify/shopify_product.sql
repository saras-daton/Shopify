{% if var('shopify_product') %}
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

with unnested_products as(
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

SELECT * 
FROM (
    select 
    '{{brand}}' as brand,
    '{{store}}' as store,
    vendor,
    tags,
    body_html,
    a.title,
    cast(a.created_at as {{ dbt.type_timestamp() }}) created_at,
    handle,
    a.id,
    image,
    template_suffix,
    published_at,
    {% if target.type =='snowflake' %}
    variants.VALUE:old_inventory_quantity::numeric as old_inventory_quantity,
    variants.VALUE:inventory_item_id::VARCHAR as inventory_item_id,
    variants.VALUE:fulfillment_service::VARCHAR as fulfillment_service,
    variants.VALUE:compare_at_price::VARCHAR as compare_at_price,
    variants.VALUE:taxable::VARCHAR as taxable,
    variants.VALUE:weight::numeric as weight,
    variants.VALUE:price::numeric as price,
    variants.VALUE:inventory_management::VARCHAR as inventory_management,
    variants.VALUE:grams::VARCHAR as grams,
    variants.VALUE:title::VARCHAR as variants_title,
    variants.VALUE:created_at::timestamp as variants_created_at,
    coalesce(variants.VALUE:product_id::VARCHAR,'') as product_id,
    coalesce(variants.VALUE:sku::VARCHAR,'') as sku,
    variants.VALUE:id::VARCHAR as variant_id,
    variants.VALUE:option2::VARCHAR as option2,
    variants.VALUE:requires_shipping::VARCHAR as requires_shipping,
    variants.VALUE:inventory_policy::VARCHAR as inventory_policy,
    variants.VALUE:option3::VARCHAR as option3,
    variants.VALUE:updated_at::timestamp as variants_updated_at,
    variants.VALUE:position::VARCHAR as position,
    variants.VALUE:option1::VARCHAR as option1,
    variants.VALUE:barcode::VARCHAR as barcode,
    variants.VALUE:image_id::VARCHAR as image_id,
    variants.VALUE:inventory_quantity::numeric as inventory_quantity,
    variants.VALUE:weight_unit::VARCHAR as weight_unit,
    {% else %}
    variants.old_inventory_quantity,
    variants.inventory_item_id,
    variants.fulfillment_service,
    variants.compare_at_price,
    variants.taxable,
    variants.weight,
    variants.price,
    variants.inventory_management,
    variants.grams,
    variants.title as variants_title,
    cast(date(variants.created_at) as timestamp) variants_created_at,
    coalesce(cast(variants.product_id as string),'') as product_id,
    coalesce(cast(variants.sku as string),'') as sku,
    variants.id as variant_id,
    variants.option2,
    variants.requires_shipping,
    variants.inventory_policy,
    variants.option3,
    cast(date(variants.updated_at) as timestamp) variants_updated_at,
    variants.position,
    variants.option1,
    variants.barcode,
    variants.image_id,
    variants.inventory_quantity,
    variants.weight_unit,
    {% endif %}
    images,
    published_scope,
    options,
    cast(date(a.updated_at) as timestamp) updated_at,
    product_type,
    status,
    metafields,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from {{i}} a
            {{unnesting("variants")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}} and sku is not null
            {% endif %}

        )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
ROW_NUMBER() OVER (PARTITION BY sku, product_id order by _daton_batch_runtime desc) row_num
from unnested_products 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num = 1
