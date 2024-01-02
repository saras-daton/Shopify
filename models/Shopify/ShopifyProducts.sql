{% if var('ShopifyProducts') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_products_tbl_ptrn','%shopify%products','shopify_products_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}


    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    cast(a.id as string) id,
    a.title,
    body_html,
    vendor,
    product_type,
    {{timezone_conversion("a.created_at")}} as created_at,
    handle,
    {{timezone_conversion("a.updated_at")}} as updated_at,
    {{timezone_conversion("published_at")}} as published_at,
    published_scope,
    tags,
    a.admin_graphql_api_id,
    {{extract_nested_value("variants","id","string")}} as variant_id,
    {{extract_nested_value("variants","product_id","string")}} as variants_product_id,
    {{extract_nested_value("variants","title","string")}} as variant_title,
    {{extract_nested_value("variants","price","numeric")}} as variants_price,
    {{extract_nested_value("variants","sku","string")}} as variants_sku,
    {{extract_nested_value("variants","position","string")}} as variants_position,
    {{extract_nested_value("variants","inventory_policy","string")}} as variants_inventory_policy,
    {{extract_nested_value("variants","fulfillment_service","string")}} as variants_fulfillment_service,
    {{extract_nested_value("variants","inventory_management","string")}} as variant_inventory_management,
    {{extract_nested_value("variants","option1","string")}} as variants_option1,
    {{extract_nested_value("variants","created_at","string")}} as variants_created_at,
    {{extract_nested_value("variants","updated_at","string")}} as variants_updated_at,
    {{extract_nested_value("variants","taxable","string")}} as variants_taxable,
    {{extract_nested_value("variants","barcode","string")}} as variants_barcode,
    {{extract_nested_value("variants","grams","string")}} as variants_grams,
    {{extract_nested_value("variants","image_id","string")}} as variants_image_id,
    {{extract_nested_value("variants","weight","numeric")}} as variants_weight,
    {{extract_nested_value("variants","weight_unit","string")}} as variants_weight_unit,
    {{extract_nested_value("variants","inventory_item_id","string")}} as variants_inventory_item_id,
    {{extract_nested_value("variants","inventory_quantity","numeric")}} as variants_inventory_quantity,
    {{extract_nested_value("variants","old_inventory_quantity","numeric")}} as variants_old_inventory_quantity,
    {{extract_nested_value("price","amount","string")}} as price_amount,
    {{extract_nested_value("price","currency_code","string")}} as price_currency_code,
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
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_products_lookback') }},0) from {{ this }})
            {% endif %}
    qualify dense_rank() over (partition by a.id order by _daton_batch_runtime desc) = 1
        
    {% if not loop.last %} union all {% endif %}
{% endfor %}
