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


## DBT Tests

The tests property defines assertions about a column, table, or view. The property contains a list of generic tests, referenced by name, which can include the four built-in generic tests available in dbt. For example, you can add tests that ensure a column contains no duplicates and zero null values. Any arguments or configurations passed to those tests should be nested below the test name.

| **Tests**  | **Description** |
| ---------------| ------------------------------------------- |
| [Not Null Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no null values present in a column |
| [Uniqueness Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no duplicate values present in a field |
| [Data Recency Test] | This is used to check for issues with data refresh within {{ x }} days |
| [Accepted Value Test](https://docs.getdbt.com/reference/resource-properties/tests#accepted_values)  | This test validates that all of the values in a column are present in a supplied list of values. If any values other than those provided in the list are present, then the test will fail |
| [Aggregation Test](https://github.com/calogica/dbt-expectations/blob/0.8.5/macros/schema_tests/table_shape/expect_table_aggregation_to_equal_other_table.sql)  | Used to check and validate the integrity of the data with the reference table from which the table was created |

### Table Name: ShopifyAbandonedCheckouts

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| email | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes (1 day) |  |  |  |

### Table Name: ShopifyBalanceTransactions

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| payout_id | Yes |  |  |  |
| payout_status | Yes |  | Yes ['in_transit', 'pending', 'paid'] |  |
| processed_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyCarrierServices

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| id | Yes |  | Yes |
| active | Yes | Yes ['true', 'false'] |  |

### Table Name: ShopifyCollects

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| collection_id | Yes |  |  |  |
| product_id | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyCountries

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| id | Yes |  | Yes |
| name | Yes |  |  |
| code | Yes |  |  |
| provinces_id | Yes |  | Yes |

### Table Name: ShopifyCustomCollections

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| handle | Yes |  |  |  |
| title | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| published_at | Yes |  |  |  |

### Table Name: ShopifyCustomerAddress

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| id | Yes |  | Yes |
| customer_id | Yes |  | Yes |
| country | Yes |  |  |

### Table Name: ShopifyCustomers

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| customers_id | Yes |  |  |  |
| email | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyDisputes

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| order_id | Yes |  |  | Yes |
| type | Yes |  |  |  |
| status | Yes |  |  |  |
| initiated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyEvents

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| subject_type | Yes |  |  |  |
| verb | Yes |  |  |  |
| created_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyFulfillmentEvents

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| fulfillment_id | Yes |  |  | Yes |
| status | Yes |  | Yes ['status', 'in_transit', 'confirmed', 'out_for_delivery', 'label_printed', 'delivered', 'label_purchased', 'attempted_delivery', 'failure'] |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| order_id | Yes |  |  |  |

### Table Name: ShopifyFulfillmentOrders

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| order_id | Yes |  |  | Yes |
| request_status | Yes |  | Yes ['unsubmitted', 'submitted'] |  |
| status | Yes |  | Yes ['open', 'closed'] |  |
| destination_email | Yes |  |  |  |
| line_items_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyGiftCards

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyInventoryItems

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| sku | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| requires_shipping | Yes |  | Yes ['true', 'false'] |  |
| tracked | Yes |  | Yes ['true', 'false'] |  |
| inventory_item_id | Yes |  |  |  |

### Table Name: ShopifyInventoryLevels

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| inventory_item_id | Yes |  |  | Yes |
| location_id | Yes |  |  | Yes |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyLocations

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| id | Yes |  | Yes |
| zip | Yes |  | Yes |
| country | Yes |  |  |

### Table Name: ShopifyOrders

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyOrdersAddresses

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  | Yes |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyOrdersCustomer

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  | Yes |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| customer_id | Yes |  |  |  |
| customer_created_at | Yes |  |  |  |
| customer_email | Yes |  |  |  |
| customer_updated_at | Yes |  |  |  |

### Table Name: ShopifyOrdersDiscountAllocations

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| line_items_id | Yes |  |  | Yes |
| discount_application_index | Yes |  |  | Yes |

### Table Name: ShopifyOrdersDiscountApplications

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| discount_applications_target_type | Yes |  |  | Yes |
| discount_applications_type | Yes |  |  | Yes |
| discount_applications_value_type | Yes |  |  | Yes |
| _seq_id | Yes |  |  | Yes |

### Table Name: ShopifyOrdersFulfillments

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| fulfillments_id | Yes |  |  | Yes |
| line_items_id | Yes |  |  | Yes |

### Table Name: ShopifyOrdersLineItems

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| line_items_id | Yes |  |  | Yes |
| line_items_variant_id | Yes |  |  |  |

### Table Name: ShopifyOrdersLineItemsTaxLines

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| line_items_id | Yes |  |  | Yes |
| tax_lines_title | Yes |  |  | Yes |
| _seq_id | Yes |  |  | Yes |

### Table Name: ShopifyOrdersShippingLines

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| order_id | Yes |  |  | Yes |
| created_at | Yes |  |  |  |
| email | Yes |  |  |  |
| order_number | Yes |  |  |  |
| processed_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| shipping_lines_id | Yes |  |  | Yes |

### Table Name: ShopifyPayouts

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| status | Yes |  | Yes ['in_transit', 'paid'] |  |
| date | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyPolicies

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| created_at | Yes |  |  |
| updated_at | Yes |  |  |
| title | Yes |  | Yes |

### Table Name: ShopifyPriceRules

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| target_type | Yes |  |  |  |
| starts_at | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| title | Yes |  |  |  |

### Table Name: ShopifyProductMetafields

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| owner_id | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyProducts

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| vendor | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| variant_id | Yes |  |  | Yes |
| variants_sku | Yes |  |  | Yes |

### Table Name: ShopifyRefundLineItemsTax

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| refund_id | Yes |  | Yes |
| order_id | Yes |  |  |
| created_at | Yes |  |  |
| user_id | Yes |  |  |
| processed_at | Yes |  |  |
| refund_line_items_id | Yes |  | Yes |
| line_item_id | Yes |  |  |
| tax_lines_title | Yes |  | Yes |
| _seq_id | Yes |  | Yes |

### Table Name: ShopifyRefundsLineItems

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| refund_id | Yes |  | Yes |
| order_id | Yes |  |  |
| created_at | Yes |  |  |
| user_id | Yes |  |  |
| processed_at | Yes |  |  |
| refund_line_items_id | Yes |  | Yes |
| line_item_id | Yes |  |  |

### Table Name: ShopifyRefundsRefundLineItems

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| refund_id | Yes |  | Yes |
| order_id | Yes |  |  |
| created_at | Yes |  |  |
| user_id | Yes |  |  |
| processed_at | Yes |  |  |
| refund_line_items_id | Yes |  | Yes |

### Table Name: ShopifyRefundsTransactions

| **Columns**  | **Not Null Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- |
| brand | Yes | Yes |  |
| store | Yes |  |  |
| refund_id | Yes |  | Yes |
| order_id | Yes |  |  |
| created_at | Yes |  |  |
| user_id | Yes |  |  |
| processed_at | Yes |  |  |
| transactions_id | Yes |  | Yes |
| _seq_id | Yes |  | Yes |

### Table Name: ShopifyShop

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| email | Yes |  |  |  |
| created_at | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifySmartCollections

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| updated_at | Yes | Yes (1 day) |  |  |

### Table Name: ShopifyTenderTransactions

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| order_id | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| payment_method | Yes |  |  |  |

### Table Name: ShopifyTransactions

| **Columns**  | **Not Null Test** | **Data Recency Test** | **Accepted Value Test** | **Uniqueness Test** |
| --------------- | --------------- | --------------- | --------------- | --------------- |
| brand | Yes |  | Yes |  |
| store | Yes |  |  |  |
| id | Yes |  |  | Yes |
| order_id | Yes |  |  |  |
| updated_at | Yes | Yes (1 day) |  |  |
| processed_at | Yes |  |  |  |
| payment_id | Yes |  |  |  |
| user_id | Yes |  |  |  |


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
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyBalanceTransactions
    description: A list of order transactions.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'processed_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: payout_id
        tests:
          - not_null
      - name: payout_status
        tests:
          - not_null
          - accepted_values:
              values: ["in_transit","pending","paid"]
      - name: processed_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyCarrierServices
    description: A list of carrier services.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: active
        tests:
          - not_null
          - accepted_values:
              values: ['true','false']
              quote: false
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyCollects
    description: A list of collects.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: collection_id
        tests:
          - not_null
      - name: product_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyCountries
    description: A list of countries.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','provinces_id']
      cluster_by: ['provinces_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: name
        tests:
          - not_null
      - name: code
        tests:
          - not_null
      - name: provinces_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id
            - provinces_id

  - name: ShopifyCustomCollections
    description: An automated collection uses selection conditions to automatically include matching products.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: handle
        tests:
          - not_null
      - name: title
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyCustomerAddress
    description: A list of all the customer addresses and related fields
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['customer_id','id']
      cluster_by: ['customer_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: customer_id
        tests:
          - not_null
      - name: country
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - customer_id
            - id

  - name: ShopifyCustomers
    description: A list orders along with the customer details
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['customers_id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['customers_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: customers_id
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - customers_id

  - name: ShopifyDisputes
    description: A list orders along with the customer details
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','order_id']
      partition_by: { 'field': 'initiated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id','order_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: type
        tests:
          - not_null
      - name: status
        tests:
          - not_null
      - name: initiated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id
            - order_id

  - name: ShopifyEvents
    description: A list of events.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: subject_type
        tests:
          - not_null
      - name: verb
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyFulfillmentEvents
    description: A list of events.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','fulfillment_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: ["status", "in_transit", "confirmed", "out_for_delivery", "label_printed", "delivered", "label_purchased", "attempted_delivery", "failure"]
      - name: fulfillment_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: order_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id
            - fulfillment_id

  - name: ShopifyFulfillmentOrders
    description: A report of orders with fulfillment details, destinations and assigned locations.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','order_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: request_status
        tests:
          - not_null
          - accepted_values:
              values: ["unsubmitted", "submitted"]
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: ["open", "closed"]
      - name: destination_email
        tests:
          - not_null
      - name: line_items_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id
            - order_id
            - line_items_id

  - name: ShopifyGiftCards
    description: A report of gift cards.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyInventoryItems
    description: A detailed report which gives details about inventory levels
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id','sku']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id','sku']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: sku
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: requires_shipping
        tests:
          - not_null
          - accepted_values:
              values: ['true','false']
              quote: false
      - name: tracked
        tests:
          - not_null
          - accepted_values:
              values: ['true','false']
              quote: false
      - name: inventory_item_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id
            - sku

  - name: ShopifyInventoryLevels
    description: A detailed report which gives details about inventory levels
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['inventory_item_id','location_id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['inventory_item_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: inventory_item_id
        tests:
          - not_null
      - name: location_id
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - inventory_item_id
            - location_id

  - name: ShopifyLocations
    description: Locations can be retail stores, warehouses, popups, dropshippers, or any other place where you manage or stock inventory.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: zip
        tests:
          - not_null
      - name: country
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: legacy
        tests:
          - not_null
          - accepted_values:
              values: ['true','false']
              quote: false
      - name: active
        tests:
          - not_null
          - accepted_values:
              values: ['true','false']
              quote: false
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyOrders
    description: A list of orders.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id

  - name: ShopifyOrdersAddresses
    description: A list of billing and shipping addresses
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','email']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','email']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - email

  - name: ShopifyOrdersCustomer
    description: A report of orders at customer level
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','customer_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: customer_id
        tests:
          - not_null
      - name: customer_created_at
        tests:
          - not_null
      - name: customer_email
        tests:
          - not_null
      - name: customer_updated_at
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - customer_id

  - name: ShopifyOrdersDiscountAllocations
    description: A list of orders at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id','discount_application_index']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id','discount_application_index']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: line_items_id
        tests:
          - not_null
      - name: discount_application_index
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - line_items_id
            - discount_application_index

  - name: ShopifyOrdersDiscountApplications
    description: A list of order and line item discounts with their coupon codes.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','discount_applications_target_type','discount_applications_type','discount_applications_value_type','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: discount_applications_target_type
        tests:
          - not_null
      - name: discount_applications_type
        tests:
          - not_null
      - name: discount_applications_value_type
        tests:
          - not_null
      - name: _seq_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - discount_applications_target_type
            - discount_applications_type
            - discount_applications_value_type
            - _seq_id

  - name: ShopifyOrdersFulfillments
    description: A report of orders with fulfillment details, destinations and assigned locations at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','fulfillments_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: fulfillments_id
        tests:
          - not_null
      - name: line_items_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - fulfillments_id
            - line_items_id

  - name: ShopifyOrdersLineItems
    description: A list of orders at product level.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: line_items_id
        tests:
          - not_null
      - name: line_items_variant_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - line_items_id

  - name: ShopifyOrdersLineItemsTaxLines
    description: A list of orders with  product level taxes.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','line_items_id','tax_lines_title','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','line_items_id','tax_lines_title'] 
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: line_items_id
        tests:
          - not_null
      - name: tax_lines_title
        tests:
          - not_null
      - name: _seq_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - line_items_id
            - tax_lines_title
            - _seq_id

  - name: ShopifyOrdersShippingLines
    description: A list of orders.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['order_id','shipping_lines_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['order_id','shipping_lines_id'] 
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: order_number
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: shipping_lines_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - shipping_lines_id

  - name: ShopifyPayouts
    description: lists all of your payouts and their current status.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'date', 'data_type': 'date' }
      cluster_by: ['id'] 
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: date
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: ["in_transit", "paid"]
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyPolicies
    description: List of policies for your Shopify store like Refund policy, Privacy policy, Terms of service, Shipping policy, Legal notice.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['title']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['title']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
      - name: title
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - title

  - name: ShopifyPriceRules
    description: A list of rules to set pricing.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: target_type
        tests:
          - not_null
      - name: starts_at
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: title
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyProductMetafields
    description: A report of product metadata
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: owner_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyProducts
    description: A list of product summary, manufacturer & dimensions
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['variants_sku','id','variant_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['variants_sku']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: vendor
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: variant_id
        tests:
          - not_null
      - name: variants_sku
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id
            - variant_id
            - variants_sku

  - name: ShopifyRefundLineItemsTax
    description: A list of taxes associated with the refunded item.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','refund_line_items_id','tax_lines_title','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: refund_id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: user_id
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: refund_line_items_id
        tests:
          - not_null
      - name: line_item_id
        tests:
          - not_null
      - name: tax_lines_title
        tests:
          - not_null
      - name: _seq_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - refund_id
            - refund_line_items_id
            - tax_lines_title
            - _seq_id

  - name: ShopifyRefundsLineItems
    description: A list of refunded orders which includes refund & product level revenue.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','refund_line_items_id','line_item_variant_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: refund_id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: user_id
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: refund_line_items_id
        tests:
          - not_null
      - name: line_item_id
        tests:
          - not_null
      - name: line_item_variant_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - refund_id
            - refund_line_items_id
            - line_item_variant_id

  - name: ShopifyRefundsRefundLineItems
    description: A list of refunded orders which includes refund & product level revenue.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','refund_line_items_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: refund_id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: user_id
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: refund_line_items_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - refund_id
            - refund_line_items_id

  - name: ShopifyRefundsTransactions
    description: A list of refund transactions.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['refund_id','transactions_id','_seq_id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['refund_id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: refund_id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: user_id
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
      - name: transactions_id
        tests:
          - not_null
      - name: _seq_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - refund_id
            - transactions_id
            - _seq_id

  - name: ShopifyShop
    description: Shop is a shopping destination and delivery tracking app that can be used  to track packages, discover new stores and products, make purchases using Shop Pay , and engage with your brand.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: email
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifySmartCollections
    description: An automated collection uses selection conditions to automatically include matching products.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'updated_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: updated_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyTenderTransactions
    description: Tender transaction created trigger starts a workflow when a monetary action such as a payment or refund takes place.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'processed_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: payment_method
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

  - name: ShopifyTransactions
    description: A report of transactions with transactions fees, sources and status.
    config:
      materialized: incremental
      incremental_strategy: merge
      unique_key: ['id']
      partition_by: { 'field': 'created_at', 'data_type': 'timestamp', 'granularity': 'day' }
      cluster_by: ['id']
    columns:
      - name: brand
        tests:
          - not_null
          - accepted_values:
              values: ["B"]
      - name: store
        tests:
          - not_null
      - name: id
        tests:
          - not_null
      - name: order_id
        tests:
          - not_null
      - name: created_at
        tests:
          - not_null
          - dbt_expectations.expect_row_values_to_have_recent_data:
              datepart: day
              interval: 1
      - name: processed_at
        tests:
          - not_null
      - name: payment_id
        tests:
          - not_null
      - name: user_id
        tests:
          - not_null
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - id

```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
