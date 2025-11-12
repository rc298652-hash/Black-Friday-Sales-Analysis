/*
  Black Friday Sales Analysis
  
   - This file includes safe backups.
*/

CREATE TABLE IF NOT EXISTS customers_backup AS SELECT * FROM customers;
CREATE TABLE IF NOT EXISTS purchases_backup AS SELECT * FROM purchases;

-- =========================================================
DROP TABLE IF EXISTS `new_data_project`;

CREATE TABLE `new_data_project` AS
SELECT
  CASE WHEN TRIM(`User_ID`) = '' THEN NULL ELSE CAST(TRIM(`User_ID`) AS UNSIGNED) END AS user_id,
  NULLIF(TRIM(`Product_ID`), '') AS product_id,
  CASE WHEN TRIM(`Gender`) = '' THEN NULL ELSE UPPER(TRIM(`Gender`)) END AS gender,
  NULLIF(TRIM(`Age`), '') AS age_group,
  CASE WHEN TRIM(`Occupation`) = '' THEN NULL ELSE CAST(TRIM(`Occupation`) AS UNSIGNED) END AS occupation,
  NULLIF(TRIM(`City_Category`), '') AS city_category,
  CASE
    WHEN TRIM(`Stay_In_Current_City_Years`) = '' THEN NULL
    WHEN TRIM(`Stay_In_Current_City_Years`) = '4+' THEN 4
    ELSE CAST(TRIM(`Stay_In_Current_City_Years`) AS UNSIGNED)
  END AS stay_years,
  CASE WHEN TRIM(`Marital_Status`) = '' THEN NULL ELSE CAST(TRIM(`Marital_Status`) AS UNSIGNED) END AS marital_status,
  CASE WHEN TRIM(`Product_Category_1`) = '' THEN NULL ELSE CAST(TRIM(`Product_Category_1`) AS UNSIGNED) END AS product_category_1,
  CASE WHEN TRIM(`Product_Category_2`) = '' THEN NULL ELSE CAST(TRIM(`Product_Category_2`) AS UNSIGNED) END AS product_category_2,
  CASE WHEN TRIM(`Product_Category_3`) = '' THEN NULL ELSE CAST(TRIM(`Product_Category_3`) AS UNSIGNED) END AS product_category_3,
  CASE WHEN TRIM(`Purchase`) = '' THEN NULL ELSE CAST(TRIM(`Purchase`) AS SIGNED) END AS purchase_amt
FROM `black friday data`;

-- Basic staging checks
SELECT COUNT(*) AS cleaned_rows FROM new_data_project;
SELECT COUNT(DISTINCT user_id) AS distinct_users, COUNT(DISTINCT product_id) AS distinct_products FROM new_data_project;
SELECT SUM(user_id IS NULL OR product_id IS NULL OR purchase_amt IS NULL) AS missing_required FROM new_data_project;

-- Check for inconsistent demographics per user (ideally returns zero rows)
SELECT user_id,
       COUNT(DISTINCT gender) AS gender_vals,
       COUNT(DISTINCT age_group) AS age_vals,
       COUNT(DISTINCT occupation) AS occ_vals,
       COUNT(DISTINCT city_category) AS city_vals,
       COUNT(DISTINCT stay_years) AS stay_vals,
       COUNT(DISTINCT marital_status) AS marital_vals
FROM new_data_project
GROUP BY user_id
HAVING (gender_vals > 1 OR age_vals > 1 OR occ_vals > 1 OR city_vals > 1 OR stay_vals > 1 OR marital_vals > 1)
LIMIT 200;

-- =========================================================
CREATE TABLE IF NOT EXISTS `customers` (
  `user_id` BIGINT NOT NULL,
  `gender` CHAR(1),
  `age_group` VARCHAR(16),
  `occupation` SMALLINT,
  `city_category` VARCHAR(2),
  `stay_years` TINYINT,
  `marital_status` TINYINT,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `products` (
  `product_id` VARCHAR(100) NOT NULL,
  `product_category_1` SMALLINT NOT NULL,
  `product_category_2` SMALLINT NULL,
  `product_category_3` SMALLINT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `purchases` (
  `purchase_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `user_id` BIGINT NOT NULL,
  `product_id` VARCHAR(100) NOT NULL,
  `purchase_amt` INT NOT NULL,
  `source_row_hash` VARCHAR(64) NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY `idx_purchases_user` (`user_id`),
  KEY `idx_purchases_product` (`product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data_quality_log` (
  `check_id` INT AUTO_INCREMENT PRIMARY KEY,
  `check_name` VARCHAR(100) NOT NULL,
  `check_sql` TEXT NOT NULL,
  `result_value` VARCHAR(200),
  `notes` TEXT,
  `checked_at` DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- Create deterministic customers table: pick most frequent value per user
DROP TABLE IF EXISTS customers_clean;

CREATE TABLE customers_clean AS
SELECT
  u.user_id,
  (SELECT gender FROM new_data_project d WHERE d.user_id = u.user_id GROUP BY gender ORDER BY COUNT(*) DESC LIMIT 1) AS gender,
  (SELECT age_group FROM new_data_project d WHERE d.user_id = u.user_id GROUP BY age_group ORDER BY COUNT(*) DESC LIMIT 1) AS age_group,
  (SELECT occupation FROM new_data_project d WHERE d.user_id = u.user_id GROUP BY occupation ORDER BY COUNT(*) DESC LIMIT 1) AS occupation,
  (SELECT city_category FROM new_data_project d WHERE d.user_id = u.user_id GROUP BY city_category ORDER BY COUNT(*) DESC LIMIT 1) AS city_category,
  (SELECT stay_years FROM new_data_project d WHERE d.user_id = u.user_id GROUP BY stay_years ORDER BY COUNT(*) DESC LIMIT 1) AS stay_years,
  (SELECT marital_status FROM new_data_project d WHERE d.user_id = u.user_id GROUP BY marital_status ORDER BY COUNT(*) DESC LIMIT 1) AS marital_status,
  NOW() AS created_at
FROM (SELECT DISTINCT user_id FROM new_data_project WHERE user_id IS NOT NULL) u;

-- Validation counts before swapping
SELECT COUNT(*) AS old_customer_count FROM customers;
SELECT COUNT(*) AS new_customer_count FROM customers_clean;

-- Swap: keep old table as backup (customers_old)
RENAME TABLE customers TO customers_old, customers_clean TO customers;

-- Products: refresh product dimension (truncate+reload is OK for this dataset)
TRUNCATE TABLE products;

INSERT INTO products (product_id, product_category_1, product_category_2, product_category_3)
SELECT DISTINCT
  product_id,
  product_category_1,
  product_category_2,
  product_category_3
FROM new_data_project
WHERE product_id IS NOT NULL;

SELECT COUNT(*) AS products_count FROM products;

-- =========================================================
SELECT source_row_hash, COUNT(*) AS duplicate_count
FROM purchases
WHERE source_row_hash IS NOT NULL
GROUP BY source_row_hash
HAVING duplicate_count > 1
LIMIT 50;

-- Ensure source_row_hash is the proper size
ALTER TABLE purchases
  MODIFY COLUMN source_row_hash VARCHAR(64) NULL;

-- Add unique index on hash to prevent duplicates on re-run
-- Note: This will fail if duplicates exist. If it fails, run the dedupe snippet below first.
ALTER TABLE purchases
  ADD UNIQUE KEY ux_purchases_source_row_hash (source_row_hash);

-- Idempotent insert (will not create duplicate rows if re-run)
INSERT INTO purchases (user_id, product_id, purchase_amt, source_row_hash, created_at)
SELECT
  user_id,
  product_id,
  purchase_amt,
  LOWER(MD5(CONCAT_WS('|',
       COALESCE(CAST(user_id AS CHAR),''), '|',
       COALESCE(product_id,''), '|',
       COALESCE(CAST(purchase_amt AS CHAR),'')))) AS source_row_hash,
  NOW() AS created_at
FROM new_data_project
WHERE user_id IS NOT NULL AND product_id IS NOT NULL AND purchase_amt IS NOT NULL
ON DUPLICATE KEY UPDATE
  purchase_amt = VALUES(purchase_amt);

-- Optional guard (MySQL may accept CHECK depending on version)
ALTER TABLE purchases ADD CONSTRAINT chk_purchase_amt_positive CHECK (purchase_amt > 0);

-- If ADD UNIQUE failed earlier, use this dedupe snippet BEFORE adding unique index:
/*
CREATE TABLE purchases_keep AS
SELECT * FROM purchases p
WHERE p.purchase_id IN (
   SELECT MIN(p2.purchase_id) FROM purchases p2 GROUP BY p2.source_row_hash
);

-- Verify counts
SELECT COUNT(*) AS orig_cnt FROM purchases;
SELECT COUNT(*) AS keep_cnt FROM purchases_keep;

-- Swap (backup original)
RENAME TABLE purchases TO purchases_old, purchases_keep TO purchases;
*/

-- =========================================================
INSERT INTO data_quality_log (check_name, check_sql, result_value, notes)
VALUES ('total_transactions', 'SELECT COUNT(*) FROM purchases', (SELECT CAST(COUNT(*) AS CHAR) FROM purchases), 'Total transactions loaded');

INSERT INTO data_quality_log (check_name, check_sql, result_value, notes)
VALUES ('total_revenue', 'SELECT SUM(purchase_amt) FROM purchases', (SELECT CAST(SUM(purchase_amt) AS CHAR) FROM purchases), 'Total revenue in purchases');

INSERT INTO data_quality_log (check_name, check_sql, result_value, notes)
VALUES ('distinct_products', 'SELECT COUNT(*) FROM products', (SELECT CAST(COUNT(*) AS CHAR) FROM products), 'Distinct products loaded');

-- Quick null checks on final facts
SELECT
  SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_users,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_products,
  SUM(CASE WHEN purchase_amt IS NULL THEN 1 ELSE 0 END) AS null_purchase_amt
FROM purchases;

-- =========================================================
-- 7. FINAL KPIs & SANITY CHECKS
-- =========================================================
SELECT COUNT(*) AS total_transactions, SUM(purchase_amt) AS total_revenue, ROUND(AVG(purchase_amt),2) AS avg_purchase FROM purchases;

-- Top categories and products
SELECT p.product_category_1 AS cat1, SUM(pr.purchase_amt) AS total_sales
FROM purchases pr
JOIN products p ON pr.product_id = p.product_id
GROUP BY p.product_category_1
ORDER BY total_sales DESC
LIMIT 10;

SELECT p.product_id, COALESCE(p.product_category_1,'Unknown') AS cat1, SUM(pr.purchase_amt) AS total_sales, COUNT(*) AS txns
FROM purchases pr
JOIN products p ON pr.product_id = p.product_id
GROUP BY p.product_id, p.product_category_1
ORDER BY total_sales DESC
LIMIT 10;

-- Lifetime top customers
SELECT p.user_id, c.age_group, c.city_category, SUM(p.purchase_amt) AS lifetime_spend, COUNT(*) AS txns
FROM purchases p
JOIN customers c ON p.user_id = c.user_id
GROUP BY p.user_id
ORDER BY lifetime_spend DESC
LIMIT 10;

-- Repeat purchase summary
SELECT
  SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) AS users_with_1_txn,
  SUM(CASE WHEN cnt = 2 THEN 1 ELSE 0 END) AS users_with_2_txns,
  SUM(CASE WHEN cnt >= 3 THEN 1 ELSE 0 END) AS users_with_3plus_txns,
  COUNT(*) AS total_users
FROM (
  SELECT user_id, COUNT(*) AS cnt FROM purchases GROUP BY user_id
) t;

-- Final referential integrity checks (should be zero)
SELECT COUNT(*) AS missing_customers FROM purchases WHERE user_id NOT IN (SELECT user_id FROM customers);
SELECT COUNT(*) AS missing_products  FROM purchases WHERE product_id NOT IN (SELECT product_id FROM products);

select * from new_data_project;
