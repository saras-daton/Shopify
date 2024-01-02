{% if var('ShopifyCarrierServices') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name('shopify_carrier_services_tbl_ptrn','%shopify%carrier_services','shopify_carrier_services_exclude_tbl_ptrn','') %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
            {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
            {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
            cast(id as string) as id,
            name,
            active,
            service_discovery,
            carrier_service_type,
            admin_graphql_api_id, 
            {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_carrier_services_lookback') }},0) from {{ this }})
                {% endif %}
        qualify dense_rank() over (partition by a.id order by {{daton_batch_runtime()}} desc) = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}
