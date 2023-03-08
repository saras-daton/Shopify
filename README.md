# Shopify Data Unification

This dbt package is for the Shopify data unification Ingested by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challanges with raw data are:
- Array/Nested Array columns which makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Shopify
- Seperate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

By doing Data Unification the above challenges can be overcomed and simplifies Data Analytics. 
As part of Data Unification, the following funtions are performed:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Shopify 
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Installation & Configuration

## Installation Instructions

If you haven't already, you will need to create a packages.yml file in your DBT project. Include this in your `packages.yml` file

```yaml
packages:
  - package: saras-daton/shopify
    version: v1.0.0
```

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_shopify). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  shopify:
    +schema: custom_schema_extension
```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the datetime columns from local timezone to given timezone, please mark the timezone_conversion_flag f as True in the dbt_project.yml file, by default, it is False
Additionally, you need to provide offset hours for each raw table

Example:
```yaml
vars:
timezone_conversion_flag: True
  raw_table_timezone_offset_hours: {
    "Shopify.Raw.Brand_UK_Shopify_orders":-7,
    "Shopify.Raw.Brand_UK_Shopify_products":-7
  }
```

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
Shopify_Customers: False
```

## Models

This package contains models from the Shopify API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Customer | [Shopify_Customers](models/Shopify/Shopify_Customers.sql)  | A list orders along with the customer details |
|Inventory | [Shopify_Inventory](models/Shopify/Shopify_Inventory.sql)  | A detailed report which gives details about inventory levels |
|Orders | [Shopify_Orders_addresses](models/Shopify/Shopify_Orders_addresses.sql)  | A list of billing and shipping addresses |
|Orders | [Shopify_Orders_customer](models/Shopify/Shopify_Orders_customer.sql)| A report of orders at customer level |
|Orders | [Shopify_Orders_fulfillment_orders](models/Shopify/Shopify_Orders_fulfillment_orders.sql)| A report of orders with fulfillment details, destinations and assigned locations. |
|Orders | [Shopify_Orders_fulfillments](models/Shopify/Shopify_Orders_fulfillments.sql)| A report of orders with fulfillment details, destinations and assigned locations at product level.|
|Orders | [Shopify_Orders_line_items_tax_lines](models/Shopify/Shopify_Orders_line_items_tax_lines.sql)| A list of orders with  product level taxes. |
|Orders | [Shopify_Orders_line_items](models/Shopify/Shopify_Orders_line_items.sql)| A list of orders at product level |
|Orders | [Shopify_Orders_refund_lines](models/Shopify/Shopify_Orders_refund_lines.sql)| A list of refunded orders which includes refund & order level revenue. |
|Orders | [Shopify_Orders_refunds_line_items](models/Shopify/Shopify_Orders_refunds_line_items.sql)| A list of refunded orders which includes refund & product level revenue. |
|Orders | [Shopify_Orders_refunds_tax_lines](models/Shopify/Shopify_Orders_refunds_tax_lines.sql)| A list of taxes associated with the refunded item. |
|Orders | [Shopify_Orders_transactions](models/Shopify/Shopify_Orders_transactions.sql)| A list of order transactions |
|Orders | [Shopify_Orders](models/Shopify/Shopify_Orders.sql)| A list of orders |
|Product | [Shopify_Product](models/Shopify/Shopify_Product.sql)| A list of product summary, manufacturer & dimensions |
|Product | [Shopify_Refunds_transactions](models/Shopify/Shopify_Refunds_transactions.sql)| A list of refund transactions |
|Returns | [Shopify_transactions](models/Shopify/Shopify_transactions.sql)| A report of transactions with transactions fees, sources and status. |




### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: shopify_customers
    description: A list orders along with the customer details
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['customers_id','addresses_id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['customers_id']

  - name: shopify_inventory	
    description: A detailed report which gives details about inventory levels
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['inventory_item_id','location_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['inventory_item_id']

  - name: shopify_orders_addresses
    description: A list of billing and shipping addresses
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','email']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','email']

  - name: shopify_orders_customer
    description: A report of orders at customer level
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','customer_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders_discount_allocation
    description: A report of orders with discount allocations.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id','line_items_properties_name','discount_application_index']
      partition_by: { 'field': 'created_at', 'data_type': 'date' }
      cluster_by: ['order_id','line_items_id']

  - name: shopify_orders_fulfillment_orders
    description: A report of orders with fulfillment details, destinations and assigned locations.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','fulfillment_orders_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id']

  - name: shopify_orders_fulfillments
    description: A report of orders with fulfillment details, destinations and assigned locations at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','fulfillments_id','fulfillments_line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders_line_items_tax_lines
    description: A list of orders with  product level taxes.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id','line_items_tax_lines_title','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id','line_items_tax_lines_title'] 

  - name: shopify_orders_line_items
    description: A list of orders at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id']

  - name: shopify_orders_refund_lines
    description: A list of refunded orders which includes refund & order level revenue.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','refund_line_items_line_item_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders_refunds_line_items
    description: A list of refunded orders which includes refund & product level revenue.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','refund_line_items_id','transactions_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders_refunds_tax_lines
    description: A list of taxes associated with the refunded item.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','refund_line_items_id','line_items_tax_lines_title','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders_shipping_lines
    description: A list of orders.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders_transactions
    description: A list of order transactions.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','transactions_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_orders
    description: A list of orders.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_product
    description: A list of product summary, manufacturer & dimensions
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['sku','product_id','variant_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['sku']

  - name: shopify_refunds_transactions
    description: A list of refund transactions.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','transactions_id','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: shopify_transactions
    description: A report of transactions with transactions fees, sources and status.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id', 'type', 'source_id', 'source_order_id']
      partition_by: { 'field': 'processed_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id', 'type', 'source_id', 'source_order_id']
```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
