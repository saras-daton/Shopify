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

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is False. Additionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table for which you want timezone converison to be taken into account.

Example:
```yaml
vars:
timezone_conversion_flag: True
  raw_table_timezone_offset_hours: {
    "Shopify.Raw.Brand_UK_Shopify_orders":-7,
    "Shopify.Raw.Brand_UK_Shopify_products":-7
  }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

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
|Customer | [ShopifyCustomers](models/Shopify/ShopifyCustomers.sql)  | A detailed report which gives infomration about Customers |
|Addresses | [ShopifyCustomersAddresses](models/Shopify/ShopifyCustomersAddresses.sql)  | A detailed report which gives infomration about the addresses of each customer |
|Inventory | [ShopifyInventory](models/Shopify/ShopifyInventory.sql)  | A detailed report which gives infomration about inventory levels |
|Orders | [ShopifyOrdersAddresses](models/Shopify/ShopifyOrdersAddresses.sql)  | A list of billing and shipping addresses |
|Orders | [ShopifyOrdersCustomer](models/Shopify/ShopifyOrdersCustomer.sql)| A report of orders at customer level |
|Orders | [ShopifyOrdersLineItemsDiscounts](models/Shopify/ShopifyOrdersLineItemsDiscounts.sql)| A report of orders with discount allocations |
|Orders | [ShopifyOrdersFulfillmentOrders](models/Shopify/ShopifyOrdersFulfillmentOrders.sql)| A report of orders with fulfillment details, destinations and assigned locations. |
|Orders | [ShopifyOrdersFulfillments](models/Shopify/ShopifyOrdersFulfillments.sql)| A report of orders with fulfillment details, destinations and assigned locations at product level.|
|Orders | [ShopifyOrdersLineItemsTaxLines](models/Shopify/ShopifyOrdersLineItemsTaxLines.sql)| A list of orders with  product level taxes. |
|Orders | [ShopifyOrdersLineItems](models/Shopify/ShopifyOrdersLineItems.sql)| A list of orders at product level |
|Orders | [ShopifyOrdersRefundLines](models/Shopify/ShopifyOrdersRefundLines.sql)| A list of refunded orders which includes refund & order level revenue. |
|Orders | [ShopifyOrdersRefundsLineItems](models/Shopify/ShopifyOrdersRefundsLineItems.sql)| A list of refunded orders which includes refund & product level revenue. |
|Orders | [ShopifyOrdersRefundsTaxLines](models/Shopify/ShopifyOrdersRefundsTaxLines.sql)| A list of taxes associated with the refunded item. |
|Orders | [ShopifyOrdersShippingLines](models/Shopify/ShopifyOrdersShippingLines.sql)| A list of orders with shipping details |
|Orders | [ShopifyOrdersTransactions](models/Shopify/ShopifyOrdersTransactions.sql)| A list of order transactions |
|Orders | [ShopifyOrders](models/Shopify/ShopifyOrders.sql)| A list of orders |
|Product | [ShopifyProduct](models/Shopify/ShopifyProduct.sql)| A list of product summary, manufacturer & dimensions |
|Refunds | [ShopifyRefundsTransactions](models/Shopify/ShopifyRefundsTransactions.sql)| A list of refund transactions |
|Transactions | [ShopifyTransactions](models/Shopify/ShopifyTransactions.sql)| A report of transactions with transactions fees, sources and status. |
|Countries | [ShopifyCountries](models/Shopify/ShopifyCountries.sql)| A list of countries. |
|Events | [ShopifyEvents](models/Shopify/ShopifyEvents.sql)| A list of events. |
|Shops | [ShopifyShop](models/Shopify/ShopifyShop.sql)| Shop is a shopping destination and delivery tracking app that can be used  to track packages, discover new stores and products, make purchases using Shop Pay , and engage with your brand. |
|Checkouts | [ShopifyCheckouts](models/Shopify/ShopifyCheckouts.sql)| Checkout are used to enter their shipping information and payment details before placing the order. |
|Transactions | [ShopifyTenderTransactions](models/Shopify/ShopifyTenderTransactions.sql)| Tender transaction created trigger starts a workflow when a monetary action such as a payment or refund takes place. |
|Policies | [ShopifyPolicies](models/Shopify/ShopifyPolicies.sql)| List of policies for your Shopify store like Refund policy, Privacy policy, Terms of service, Shipping policy, Legal notice. |
|Collections | [ShopifySmartCollections](models/Shopify/ShopifySmartCollections.sql)| An automated collection uses selection conditions to automatically include matching products. |
|Collections | [ShopifyCollects](models/Shopify/ShopifyCollects.sql)| A list of collections. |
|Locations | [ShopifyLocations](models/Shopify/ShopifyLocations.sql)| Locations can be retail stores, warehouses, popups, dropshippers, or any other place where you manage or stock inventory. |
|Price Rules | [ShopifyPriceRules](models/Shopify/ShopifyPriceRules.sql)| A list of rules to set pricing. |
|Carrier Services | [ShopifyCarrierServices](models/Shopify/ShopifyCarrierServices.sql)| A list of carrier services. |
|Payouts | [ShopifyPayouts](models/Shopify/ShopifyPayouts.sql)| lists all of your payouts and their current status. |




### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: ShopifyAbandonedCheckouts
    description: After a customer adds products to a cart, they use your checkout to enter their shipping information and payment details before placing the order.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyBalanceTransactions
    description: A list of order transactions.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyCarrierServices
    description: A list of carrier services.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      cluster_by: ['id']

  - name: ShopifyCollects
    description: A list of collects.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyCountries
    description: A list of countries.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','provinces_id']
      cluster_by: ['provinces_id']

  - name: ShopifyCustomCollections
    description: An automated collection uses selection conditions to automatically include matching products.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyCustomerAddress
    description: A list of all the customer addresses and related fields
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['customers_id','id']
      cluster_by: ['customers_id']

  - name: ShopifyCustomers
    description: A list orders along with the customer details
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['customers_id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['customers_id']

  - name: ShopifyDisputes
    description: A list orders along with the customer details
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','order_id']
      partition_by: { 'field': 'initiated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id','order_id']

  - name: ShopifyEvents
    description: A list of events.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyFulfillmentEvents
    description: A list of events.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','fulfillment_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyFulfillmentOrders
    description: A report of orders with fulfillment details, destinations and assigned locations.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','order_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id']

  - name: ShopifyGiftCards
    description: A report of gift cards.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyInventoryItems
    description: A detailed report which gives details about inventory levels
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','inventory_item_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['inventory_item_id']

  - name: ShopifyInventoryLevels
    description: A detailed report which gives details about inventory levels
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['inventory_item_id','location_id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['inventory_item_id']

  - name: ShopifyLocations
    description: Locations can be retail stores, warehouses, popups, dropshippers, or any other place where you manage or stock inventory.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyOrders
    description: A list of orders.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: ShopifyOrdersAddresses
    description: A list of billing and shipping addresses
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','email']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','email']

  - name: ShopifyOrdersCustomer
    description: A report of orders at customer level
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','customer_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: ShopifyOrdersDiscountAllocations
    description: A list of orders at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id','discount_application_index']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id','discount_application_index']

  - name: ShopifyOrdersDiscountApplications
    description: A list of order and line item discounts with their coupon codes.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','discount_applications_target_type','discount_applications_type','discount_applications_value_type','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: ShopifyOrdersFulfillments
    description: A report of orders with fulfillment details, destinations and assigned locations at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','fulfillments_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']

  - name: ShopifyOrdersLineItems
    description: A list of orders at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id']

  - name: ShopifyOrdersLineItemsTaxLines
    description: A list of orders with  product level taxes.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id','tax_lines_title','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id','tax_lines_title'] 

  - name: ShopifyOrdersShippingLines
    description: A list of orders.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','shipping_lines_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','shipping_lines_id']

  - name: ShopifyPayouts
    description: lists all of your payouts and their current status.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'date', 'data_type': 'date' }
      cluster_by: ['id']

  - name: ShopifyPolicies
    description: List of policies for your Shopify store like Refund policy, Privacy policy, Terms of service, Shipping policy, Legal notice.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['title']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['title']

  - name: ShopifyPriceRules
    description: A list of rules to set pricing.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyProductMetafields
    description: A report of product metadata
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyProducts
    description: A list of product summary, manufacturer & dimensions
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['sku','product_id','variant_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['sku']

  - name: ShopifyRefundLineItemsTax
    description: A list of taxes associated with the refunded item.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','refund_line_items_id','tax_lines_title','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']

  - name: ShopifyRefundsLineItems
    description: A list of refunded orders which includes refund & product level revenue.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','refund_line_items_id','line_item_variant_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']

  - name: ShopifyRefundsRefundLineItems
    description: A list of refunded orders which includes refund & product level revenue.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','refund_line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']

  - name: ShopifyRefundsTransactions
    description: A list of refund transactions.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','transactions_id','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']

  - name: ShopifyShop
    description: Shop is a shopping destination and delivery tracking app that can be used  to track packages, discover new stores and products, make purchases using Shop Pay , and engage with your brand.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifySmartCollections
    description: An automated collection uses selection conditions to automatically include matching products.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyTenderTransactions
    description: Tender transaction created trigger starts a workflow when a monetary action such as a payment or refund takes place.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'processed_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

  - name: ShopifyTransactions
    description: A report of transactions with transactions fees, sources and status.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']

```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
