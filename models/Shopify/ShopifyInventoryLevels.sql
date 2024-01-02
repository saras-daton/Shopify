{% if var('ShopifyInventoryLevels') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_inventory_levels_tbl_ptrn','%shopify%inventory_levels','shopify_inventory_levels_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

    select 
    {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
    {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
    cast(inventory_item_id as string) as inventory_item_id,
    cast(location_id as string) as location_id,
    available as inventory_levels_available,
    {{timezone_conversion("updated_at")}} as updated_at,
    admin_graphql_api_id,
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    from  {{i}} a
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_inventory_levels_lookback') }},0) from {{ this }})
            {% endif %}  
    qualify dense_rank() over (partition by inventory_item_id order by {{daton_batch_runtime()}}, {{daton_batch_id()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
