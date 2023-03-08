{% if var('spy_inventory') %}
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

with unnested_inventory_levels as (
{% set table_name_query %}
{{set_table_name('%shopify%inventory%')}}    
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
    {% if target.type =='snowflake' %}
    INVENTORY_LEVELS.VALUE:inventory_item_id::VARCHAR as inventory_item_id,
    INVENTORY_LEVELS.VALUE:location_id::VARCHAR as location_id,
    INVENTORY_LEVELS.VALUE:available,
    INVENTORY_LEVELS.VALUE:updated_at::timestamp as inventory_levels_updated_at,
    INVENTORY_LEVELS.VALUE:admin_graphql_api_id::VARCHAR as inventory_levels_admin_graphql_api_id,
    INVENTORY_ITEM.VALUE:id,
    INVENTORY_ITEM.VALUE:SKU,
    INVENTORY_ITEM.VALUE:created_at::{{ dbt.type_timestamp() }} as created_at,
    INVENTORY_ITEM.VALUE:updated_at::timestamp as inventory_item_updated_at,
    INVENTORY_ITEM.VALUE:requires_shipping,
    INVENTORY_ITEM.VALUE:cost,
    INVENTORY_ITEM.VALUE:country_code_of_origin,
    INVENTORY_ITEM.VALUE:province_code_of_origin,
    INVENTORY_ITEM.VALUE:harmonized_system_code,
    INVENTORY_ITEM.VALUE:tracked,
    INVENTORY_ITEM.VALUE:country_harmonized_system_codes,
    INVENTORY_ITEM.VALUE:admin_graphql_api_id::VARCHAR as inventory_item_admin_graphql_api_id,
    {% else %}
    inventory_levels.inventory_item_id as inventory_item_id,
    inventory_levels.location_id as location_id,
    inventory_levels.available as available,
    CAST(inventory_levels.updated_at as timestamp) inventory_levels_updated_at,
    inventory_levels.admin_graphql_api_id as inventory_levels_admin_graphql_api_id,
    inventory_item.id as id,
    inventory_item.SKU as SKU,
    CAST(inventory_item.created_at as {{ dbt.type_timestamp() }}) as created_at,
    CAST(inventory_item.updated_at as timestamp) as inventory_item_updated_at,
    inventory_item.requires_shipping as requires_shipping,
    inventory_item.cost as cost,
    inventory_item.country_code_of_origin as country_code_of_origin,
    inventory_item.province_code_of_origin as province_code_of_origin,
    inventory_item.harmonized_system_code as harmonized_system_code,
    inventory_item.tracked as tracked,
    inventory_item.country_harmonized_system_codes as country_harmonized_system_codes,
    inventory_item.admin_graphql_api_id as inventory_item_admin_graphql_api_id,
    {% endif %}
    {{daton_user_id()}} as _daton_user_id,
    {{daton_batch_runtime()}} as _daton_batch_runtime,
    {{daton_batch_id()}} as _daton_batch_id,
    current_timestamp() as _last_updated,
    '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    FROM  {{i}} a
            {{unnesting("INVENTORY_LEVELS")}} 
            {{multi_unnesting("INVENTORY_LEVELS","INVENTORY_ITEM")}} 
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
        )  
    {% if not loop.last %} union all {% endif %}
{% endfor %}
),

dedup as (
select *,
DENSE_RANK() OVER (PARTITION BY inventory_item_id order by _daton_batch_runtime desc) row_num
from unnested_inventory_levels 
)

select * {{exclude()}} (row_num)
from dedup 
where row_num =1
