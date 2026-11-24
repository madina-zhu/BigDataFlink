CREATE TABLE dim_customer (
  customer_id     INT PRIMARY KEY,
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

CREATE TABLE dim_seller (
  seller_id       INT PRIMARY KEY,
  first_name      TEXT,
  last_name       TEXT,
  email           TEXT,
  country         TEXT,
  postal_code     TEXT
);

CREATE TABLE dim_product (
  product_id      INT PRIMARY KEY,
  product_name    TEXT,
  product_category TEXT,
  product_price   NUMERIC,
  product_quantity INT,
  product_weight  NUMERIC,
  product_color   TEXT,
  product_size    TEXT,
  product_brand   TEXT,
  product_material TEXT,
  product_description TEXT,
  product_rating  NUMERIC,
  product_reviews INT,
  product_release_date DATE,
  product_expiry_date DATE,
  pet_category    TEXT
);

CREATE TABLE dim_store (
  store_id        BIGINT PRIMARY KEY,
  store_name      TEXT,
  store_location  TEXT,
  store_city      TEXT,
  store_state     TEXT,
  store_country   TEXT,
  store_phone     TEXT,
  store_email     TEXT
);

CREATE TABLE dim_supplier (
  supplier_id     BIGINT PRIMARY KEY,
  supplier_name   TEXT,
  supplier_contact TEXT,
  supplier_email  TEXT,
  supplier_phone  TEXT,
  supplier_address TEXT,
  supplier_city   TEXT,
  supplier_country TEXT
);

CREATE TABLE fact_sales (
  sale_id         BIGINT PRIMARY KEY,
  customer_id     INT REFERENCES dim_customer(customer_id),
  seller_id       INT REFERENCES dim_seller(seller_id),
  product_id      INT REFERENCES dim_product(product_id),
  store_id        BIGINT REFERENCES dim_store(store_id),
  supplier_id     BIGINT REFERENCES dim_supplier(supplier_id),
  sale_date       DATE,
  sale_quantity   INT,
  sale_total_price NUMERIC
);