{% if var('ShopifyPriceRules') %}
    {{ config(enabled=True) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('shopify_price_rules_tbl_ptrn'),
exclude=var('shopify_price_rules_exclude_tbl_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
            {% set hr = 0 %}
    {% endif %}

    select 
        '{{ brand }}' as brand,
        '{{ store }}' as store,
        cast(id as string) id,
        value_type,
        value,
        customer_selection,
        target_type,
        target_selection,
        allocation_method,
        once_per_customer,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="starts_at") }} as {{ dbt.type_timestamp() }}) as starts_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="ends_at") }} as {{ dbt.type_timestamp() }}) as ends_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="created_at") }} as {{ dbt.type_timestamp() }}) as created_at,
        cast({{ dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="updated_at") }} as {{ dbt.type_timestamp() }}) as updated_at,
        title,
        admin_graphql_api_id,
        usage_limit,
        entitled_collection_ids,
        customer_segment_prerequisite_ids,
        prerequisite_customer_ids,
        prerequisite_collection_ids,
        entitled_product_ids,
        {{ daton_user_id() }} as _daton_user_id,
        {{ daton_batch_runtime() }} as _daton_batch_runtime,
        {{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    from {{ i }} a
    {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
        where {{ daton_batch_runtime() }} >= (select coalesce(max(_daton_batch_runtime) - {{var('shopify_price_rules_lookback') }},0) from {{ this }})
    {% endif %}
    qualify dense_rank() over (partition by a.id order by {{ daton_batch_runtime() }} desc) = 1
    {% if not loop.last %} union all {% endif %}
{% endfor %} 