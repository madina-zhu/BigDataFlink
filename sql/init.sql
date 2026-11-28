-- Модель данных «звезда» для лабораторной работы №3.
--
-- Ключи измерений — детерминированные MD5-хэши натуральных ключей
-- (вычисляются в Flink-джобе). Это даёт:
--   * отсутствие коллизий (в отличие от 32-битного hashCode);
--   * идемпотентность: повторная отправка тех же данных не создаёт дублей (upsert по PK).
--
-- FOREIGN KEY намеренно не объявлены: в streaming-режиме Flink пишет в 6 таблиц
-- параллельно, и строка факта может оказаться в БД раньше строки измерения.
-- Связь fact_sales -> dim_* логическая (проверяется запросами из sql/check.sql).

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id     VARCHAR(32) PRIMARY KEY,
  first_name      TEXT,
  last_name       TEXT,
  age             INT,
  email           TEXT,
  country         TEXT,
  postal_code     TEXT,
  pet_type        TEXT,
  pet_name        TEXT,
  pet_breed       TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
  seller_id       VARCHAR(32) PRIMARY KEY,
  first_name      TEXT,
  last_name       TEXT,
  email           TEXT,
  country         TEXT,
  postal_code     TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_id      VARCHAR(32) PRIMARY KEY,
  product_name    TEXT,
  product_category TEXT,
  product_price   NUMERIC(10,2),
  product_quantity INT,
  product_weight  NUMERIC(10,2),
  product_color   TEXT,
  product_size    TEXT,
  product_brand   TEXT,
  product_material TEXT,
  product_description TEXT,
  product_rating  NUMERIC(3,1),
  product_reviews INT,
  product_release_date DATE,
  product_expiry_date DATE,
  pet_category    TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
  store_id        VARCHAR(32) PRIMARY KEY,
  store_name      TEXT,
  store_location  TEXT,
  store_city      TEXT,
  store_state     TEXT,
  store_country   TEXT,
  store_phone     TEXT,
  store_email     TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
  supplier_id     VARCHAR(32) PRIMARY KEY,
  supplier_name   TEXT,
  supplier_contact TEXT,
  supplier_email  TEXT,
  supplier_phone  TEXT,
  supplier_address TEXT,
  supplier_city   TEXT,
  supplier_country TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
  sale_id         VARCHAR(32) PRIMARY KEY,
  customer_id     VARCHAR(32),
  seller_id       VARCHAR(32),
  product_id      VARCHAR(32),
  store_id        VARCHAR(32),
  supplier_id     VARCHAR(32),
  sale_date       DATE,
  sale_quantity   INT,
  sale_total_price NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_seller   ON fact_sales(seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_store    ON fact_sales(store_id);
CREATE INDEX IF NOT EXISTS idx_fact_supplier ON fact_sales(supplier_id);
