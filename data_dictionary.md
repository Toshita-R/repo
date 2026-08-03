# Data Dictionary — Gold Layer

Catalog `ecom_dev`, schema `gold`. All objects are materialized views managed by the `ecom-gold-dev` pipeline.

**Conventions**
- `_sk` — surrogate key, SHA-256 of the natural key
- `dim_seller` and `dim_product` return the **current** SCD2 version only (`__END_AT IS NULL`)
- Facts store `customer_sk` directly; `bridge_customer_order` resolves `customer_id` to it

---

## Dimensions

### dim_customer
**Grain:** one row per person (`customer_unique_id`) · **Rows:** 96,096 · **Classification:** PII

| Column | Type | Description |
|---|---|---|
| customer_sk | STRING | SHA-256 of customer_unique_id. Primary key. |
| customer_unique_id | STRING | Person-level identifier. **PII** — masked in `v_dim_customer_secure`. |
| customer_city | STRING | Normalised city name. Suffixes like " sp" and "/state" stripped. |
| customer_state | STRING | Two-letter Brazilian state code. |
| zip_code_prefix | INT | First 5 digits of postal code. **PII** — quasi-identifier. |
| customer_latitude | DOUBLE | ZIP centroid. NULL for 269 customers with no geo match. **PII**. |
| customer_longitude | DOUBLE | ZIP centroid. NULL for the same 269. **PII**. |
| has_geo | BOOLEAN | FALSE where the ZIP prefix has no geolocation record. |
| lifetime_order_count | BIGINT | Orders placed by this person across the full period. |
| is_repeat_customer | BOOLEAN | TRUE where lifetime_order_count > 1. 2,997 people. |
| customer_segment | STRING | `frequent` (3+), `returning` (2), `one_time` (1). |

> **Design note.** Keyed on `customer_unique_id`, not `customer_id`. The source `customer_id` is regenerated per order; using it would produce 99,441 rows and report a 0% repeat-purchase rate. Join facts to this dimension through `bridge_customer_order`.

---

### dim_seller
**Grain:** one row per current seller · **Rows:** 3,095

| Column | Type | Description |
|---|---|---|
| seller_sk | STRING | SHA-256 of seller_id. Primary key. |
| seller_id | STRING | Natural key from source. |
| seller_city | STRING | City of operation. |
| seller_state | STRING | Two-letter state code. |
| zip_code_prefix | INT | First 5 digits of postal code. |
| seller_latitude | DOUBLE | ZIP centroid. NULL for 7 sellers with no geo match. |
| seller_longitude | DOUBLE | ZIP centroid. NULL for the same 7. |
| has_geo | BOOLEAN | FALSE where no geolocation record exists. |
| valid_from | TIMESTAMP | `__START_AT` from SCD2 history. When this version became current. |

> **Design note.** `sv_sellers_history` holds 3,135 rows across all versions — 40 sellers relocated in batch 2 and have two versions each. This dimension filters `__END_AT IS NULL`. Omitting that filter would double every fact row joining to those sellers.

---

### dim_product
**Grain:** one row per current product · **Rows:** 32,951

| Column | Type | Description |
|---|---|---|
| product_sk | STRING | SHA-256 of product_id. Primary key. |
| product_id | STRING | Natural key from source. |
| category_pt | STRING | Portuguese category. `unknown` for 610 products with no source category. |
| category_en | STRING | English category. Falls back to Portuguese for 2 untranslated categories. |
| name_length | INT | Characters in the product name. Source column is misspelled `product_name_lenght`. |
| description_length | INT | Characters in the description. Source misspelled `product_description_lenght`. |
| photos_qty | INT | Number of listing photos. |
| weight_g | DOUBLE | Weight in grams. |
| length_cm / height_cm / width_cm | DOUBLE | Package dimensions. |
| volume_cm3 | DOUBLE | length × height × width. NULL where any dimension is NULL. |
| size_bucket | STRING | `small` <500g, `medium` <2kg, `large` <10kg, `oversized` ≥10kg, `unknown`. |
| valid_from | TIMESTAMP | `__START_AT` from SCD2 history. |

> **Design note.** `pc_gamer` and `portateis_cozinha_e_preparadores_de_alimentos` have no English translation. A LEFT join with `COALESCE(en, pt, 'unknown')` preserves them; an inner join would have dropped 13 products including the 60 recategorised in batch 2.

---

### dim_date
**Grain:** one row per calendar day · **Rows:** 852 · **Range:** 2016-09-01 to 2018-12-31

| Column | Type | Description |
|---|---|---|
| date_key | DATE | Primary key. |
| year, quarter, month, day | INT | Calendar parts. |
| week_of_year | INT | ISO week number. |
| day_of_week | INT | 1 = Sunday through 7 = Saturday. |
| month_name, day_name | STRING | Full English names. |
| year_month | STRING | `yyyy-MM`, for monthly grouping. |
| is_weekend | BOOLEAN | TRUE for Saturday and Sunday. |

Generated, not sourced. Range extends past the last order date so future dates resolve.

---

### dim_order_status
**Grain:** one row per status · **Rows:** 8

| Column | Type | Description |
|---|---|---|
| order_status | STRING | Primary key. Lowercase status value. |
| status_order | INT | Lifecycle position, 1–8. Use for ordering in reports. |
| is_terminal | BOOLEAN | TRUE for `delivered`, `canceled`, `unavailable`. |
| status_description | STRING | Human-readable meaning. |

---

### bridge_customer_order
**Grain:** one row per `customer_id` · **Rows:** 99,441

| Column | Type | Description |
|---|---|---|
| customer_id | STRING | Per-order customer key, as carried on source orders. |
| customer_unique_id | STRING | Person-level key. |
| customer_sk | STRING | SHA-256 of customer_unique_id. FK to dim_customer. |

Resolves the many-to-one relationship between order-level and person-level customer keys. Required for any join from facts to `dim_customer`.

---

## Facts

### fct_order_items
**Grain:** one row per order line (`order_id` + `order_item_id`) · **Rows:** 112,650

| Column | Type | Description |
|---|---|---|
| order_item_sk | STRING | SHA-256 of order_id‖order_item_id. Primary key. |
| order_id | STRING | Degenerate dimension. |
| order_item_id | INT | Line sequence within the order. |
| customer_sk | STRING | FK to dim_customer, via bridge. |
| seller_sk | STRING | FK to dim_seller. |
| product_sk | STRING | FK to dim_product. |
| order_date_key | DATE | FK to dim_date. |
| order_status | STRING | FK to dim_order_status. |
| customer_id / seller_id / product_id | STRING | Natural keys, retained for traceability. |
| order_purchase_timestamp | TIMESTAMP | When the order was placed. |
| order_approved_at | TIMESTAMP | Payment approval. |
| order_delivered_customer_date | TIMESTAMP | Actual delivery. NULL if undelivered. |
| order_estimated_delivery_date | TIMESTAMP | Promised delivery date. |
| shipping_limit_date | TIMESTAMP | Seller's dispatch deadline. |
| price | DECIMAL(10,2) | Item price excluding freight. |
| freight_value | DECIMAL(10,2) | Shipping cost for this line. |
| gross_revenue | DECIMAL(10,2) | price + freight_value. **Total: R$ 15,843,553.24.** |
| freight_ratio | DOUBLE | freight ÷ price. NULL where price = 0. |
| delivery_days | INT | Purchase to delivery. NULL if undelivered. Mean 12.0. |
| delivery_delay_days | INT | Actual minus estimated. Negative = early. |
| is_late_delivery | BOOLEAN | TRUE late, FALSE on time, **NULL if undelivered**. |
| is_delivered | BOOLEAN | TRUE where order_status = 'delivered'. |

> **Design note on `is_late_delivery`.** Deliberately three-valued. Coalescing NULL to FALSE would count every in-transit order as on-time and overstate delivery performance. Filter `WHERE is_late_delivery IS NOT NULL` for delivery analysis.

> **Coverage.** 775 orders have no line items and are absent here by design. Use `sv_orders` for order-level counts that must include them.

---

### fct_payments
**Grain:** one row per instalment (`order_id` + `payment_sequential`) · **Rows:** 103,886

| Column | Type | Description |
|---|---|---|
| payment_sk | STRING | SHA-256 of order_id‖payment_sequential. Primary key. |
| order_id | STRING | Degenerate dimension. |
| payment_sequential | INT | Payment sequence within the order. |
| customer_sk | STRING | FK to dim_customer, via bridge. |
| order_date_key | DATE | FK to dim_date. |
| payment_type | STRING | `credit_card` 74%, `boleto` 19%, `voucher` 5.6%, `debit_card` 1.5%, `not_defined` 3 rows. |
| payment_installments | INT | Number of instalments agreed. |
| payment_value | DECIMAL(10,2) | Amount. **Total: R$ 16,008,872.12.** |
| is_installment | BOOLEAN | TRUE where payment_installments > 1. |
| is_suspect | BOOLEAN | TRUE for `not_defined` type or zero value. 12 rows. |
| has_order | BOOLEAN | FALSE for the 1 payment with no matching order. |
| order_status | STRING | Status of the parent order. |

> **Design note.** Suspect payments are flagged, not dropped. A zero-value payment usually means a fully voucher-covered order — a real business event. Dropping them would understate order counts.

---

### fct_reviews
**Grain:** one row per review-order pair (`review_id` + `order_id`) · **Rows:** 99,224 · **Classification:** PII

| Column | Type | Description |
|---|---|---|
| review_sk | STRING | SHA-256 of review_id‖order_id. Primary key. |
| review_id | STRING | **Not unique.** 724 reviews cover multiple orders. |
| order_id | STRING | Degenerate dimension. |
| customer_sk | STRING | FK to dim_customer, via bridge. |
| order_date_key | DATE | FK to dim_date. |
| review_score | INT | 1–5. Mean 4.09. |
| is_negative / is_positive | BOOLEAN | Score ≤2 / ≥4. |
| has_comment | BOOLEAN | TRUE where a free-text message exists. |
| review_creation_date | TIMESTAMP | Survey sent to customer. |
| review_answer_timestamp | TIMESTAMP | Customer submitted. |
| is_late_delivery | BOOLEAN | Delivery status of the reviewed order. NULL if undelivered. |
| order_status | STRING | Status of the parent order. |

> **Design note.** `review_id` alone is **not** the grain. 724 review IDs map to two distinct orders each, with identical scores and timestamps — one customer reviewing a basket split across sellers. Deduplicating on `review_id` would destroy 743 valid relationships.

> **Key finding.** Late deliveries average **2.57** stars against **4.29** for on-time — a 1.72-star penalty.

---

## Marts

### mart_delivery_performance
Delivered line items aggregated by `customer_state` × `year_month`.
Measures: `line_items`, `orders`, `gmv`, `avg_delivery_days`, `avg_delay_days`, `late_pct`, `avg_review_score`.

### mart_seller_scorecard
Sellers with 20+ line items.
Measures: `line_items`, `orders`, `distinct_products`, `gmv`, `avg_item_price`, `avg_delivery_days`, `late_pct`, `avg_review_score`.

### mart_monthly_revenue
One row per month, 24 rows.
Measures: `orders`, `customers`, `line_items`, `gmv`, `product_revenue`, `freight_revenue`, `repeat_customer_pct`, `avg_order_value`.

---

## Secure views

| View | Purpose |
|---|---|
| `v_dim_customer_secure` | `customer_unique_id` partially redacted; lat/lng rounded to ~11km for users outside `pii_readers`. |
| `v_fct_reviews_secure` | Free-text review content excluded. Scores and flags retained. |
| `v_dim_customer_regional` | Row-level filter by state. Demonstrates regional access scoping. |

---

## Reference: standard join pattern

```sql
SELECT
  c.customer_state,
  s.seller_state,
  p.category_en,
  d.year_month,
  COUNT(*)                      AS line_items,
  ROUND(SUM(f.gross_revenue),2) AS gmv,
  ROUND(AVG(f.delivery_days),1) AS avg_delivery_days
FROM ecom_dev.gold.fct_order_items f
LEFT JOIN ecom_dev.gold.dim_customer     c ON f.customer_sk    = c.customer_sk
LEFT JOIN ecom_dev.gold.dim_seller       s ON f.seller_sk      = s.seller_sk
LEFT JOIN ecom_dev.gold.dim_product      p ON f.product_sk     = p.product_sk
LEFT JOIN ecom_dev.gold.dim_date         d ON f.order_date_key = d.date_key
GROUP BY c.customer_state, s.seller_state, p.category_en, d.year_month;
```

LEFT joins throughout. Referential integrity is verified (zero orphan keys), but LEFT is defensive against future dimension refresh timing.
