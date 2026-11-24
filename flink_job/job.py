from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    env.execute_sql("""
        CREATE TABLE kafka_sales (
            id STRING,
            customer_first_name STRING, customer_last_name STRING,
            customer_age STRING, customer_email STRING, customer_country STRING,
            customer_postal_code STRING, customer_pet_type STRING,
            customer_pet_name STRING, customer_pet_breed STRING,
            seller_first_name STRING, seller_last_name STRING, seller_email STRING,
            seller_country STRING, seller_postal_code STRING,
            product_name STRING, product_category STRING, product_price STRING,
            product_quantity STRING, sale_date STRING,
            sale_customer_id STRING, sale_seller_id STRING, sale_product_id STRING,
            sale_quantity STRING, sale_total_price STRING,
            store_name STRING, store_location STRING, store_city STRING,
            store_state STRING, store_country STRING, store_phone STRING,
            store_email STRING,
            pet_category STRING, product_weight STRING, product_color STRING,
            product_size STRING, product_brand STRING, product_material STRING,
            product_description STRING, product_rating STRING, product_reviews STRING,
            product_release_date STRING, product_expiry_date STRING,
            supplier_name STRING, supplier_contact STRING, supplier_email STRING,
            supplier_phone STRING, supplier_address STRING, supplier_city STRING,
            supplier_country STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales_topic',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-lab3',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    def jdbc(table_name, columns):
        env.execute_sql(f"""
            CREATE TABLE {table_name} (
                {columns}
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://postgres:5432/lab3',
                'table-name' = '{table_name}',
                'username' = 'lab',
                'password' = 'lab',
                'sink.buffer-flush.max-rows' = '500',
                'sink.buffer-flush.interval' = '2s'
            )
        """)

    jdbc("dim_customer", """
        customer_id INT, first_name STRING, last_name STRING, age INT,
        email STRING, country STRING, postal_code STRING,
        pet_type STRING, pet_name STRING, pet_breed STRING,
        PRIMARY KEY (customer_id) NOT ENFORCED
    """)

    jdbc("dim_seller", """
        seller_id INT, first_name STRING, last_name STRING,
        email STRING, country STRING, postal_code STRING,
        PRIMARY KEY (seller_id) NOT ENFORCED
    """)

    jdbc("dim_product", """
        product_id INT, product_name STRING, product_category STRING,
        product_price DECIMAL(10,2), product_quantity INT,
        product_weight DECIMAL(10,2), product_color STRING,
        product_size STRING, product_brand STRING, product_material STRING,
        product_description STRING, product_rating DECIMAL(3,1),
        product_reviews INT, product_release_date DATE,
        product_expiry_date DATE, pet_category STRING,
        PRIMARY KEY (product_id) NOT ENFORCED
    """)

    jdbc("dim_store", """
        store_id BIGINT, store_name STRING, store_location STRING,
        store_city STRING, store_state STRING, store_country STRING,
        store_phone STRING, store_email STRING,
        PRIMARY KEY (store_id) NOT ENFORCED
    """)

    jdbc("dim_supplier", """
        supplier_id BIGINT, supplier_name STRING, supplier_contact STRING,
        supplier_email STRING, supplier_phone STRING, supplier_address STRING,
        supplier_city STRING, supplier_country STRING,
        PRIMARY KEY (supplier_id) NOT ENFORCED
    """)

    jdbc("fact_sales", """
        sale_id BIGINT, customer_id INT, seller_id INT, product_id INT,
        store_id BIGINT, supplier_id BIGINT,
        sale_date DATE, sale_quantity INT, sale_total_price DECIMAL(10,2),
        PRIMARY KEY (sale_id) NOT ENFORCED
    """)

    stmt_set = env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO dim_customer
        SELECT
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(customer_email, ''), COALESCE(customer_first_name, ''), COALESCE(customer_last_name, '')))),
            customer_first_name, customer_last_name,
            CAST(customer_age AS INT), customer_email,
            customer_country, customer_postal_code,
            customer_pet_type, customer_pet_name, customer_pet_breed
        FROM kafka_sales
        WHERE customer_email IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_seller
        SELECT
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(seller_email, ''), COALESCE(seller_first_name, ''), COALESCE(seller_last_name, '')))),
            seller_first_name, seller_last_name,
            seller_email, seller_country, seller_postal_code
        FROM kafka_sales
        WHERE seller_email IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_product
        SELECT
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(product_name, ''), COALESCE(product_price, ''), COALESCE(supplier_name, ''), COALESCE(product_release_date, '')))),
            product_name, product_category,
            CAST(product_price AS DECIMAL(10,2)),
            CAST(product_quantity AS INT),
            CAST(product_weight AS DECIMAL(10,2)),
            product_color, product_size, product_brand,
            product_material, product_description,
            CAST(product_rating AS DECIMAL(3,1)),
            CAST(product_reviews AS INT),
            TO_DATE(product_release_date, 'M/d/yyyy'),
            TO_DATE(product_expiry_date, 'M/d/yyyy'),
            pet_category
        FROM kafka_sales
        WHERE product_name IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_store
        SELECT
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(store_name, ''), COALESCE(store_city, '')))) AS store_id,
            store_name, store_location, store_city, store_state,
            store_country, store_phone, store_email
        FROM kafka_sales
        WHERE store_name IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO dim_supplier
        SELECT
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(supplier_name, ''), COALESCE(supplier_city, '')))) AS supplier_id,
            supplier_name, supplier_contact, supplier_email,
            supplier_phone, supplier_address, supplier_city, supplier_country
        FROM kafka_sales
        WHERE supplier_name IS NOT NULL
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO fact_sales
        SELECT
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(id, ''), COALESCE(sale_date, ''), COALESCE(customer_email, ''), COALESCE(seller_email, ''), COALESCE(product_name, ''), COALESCE(sale_total_price, '')))),
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(customer_email, ''), COALESCE(customer_first_name, ''), COALESCE(customer_last_name, '')))),
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(seller_email, ''), COALESCE(seller_first_name, ''), COALESCE(seller_last_name, '')))),
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(product_name, ''), COALESCE(product_price, ''), COALESCE(supplier_name, ''), COALESCE(product_release_date, '')))),
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(store_name, ''), COALESCE(store_city, '')))),
            ABS(HASH_CODE(CONCAT_WS('|', COALESCE(supplier_name, ''), COALESCE(supplier_city, '')))),
            TO_DATE(sale_date, 'M/d/yyyy'),
            CAST(sale_quantity AS INT),
            CAST(sale_total_price AS DECIMAL(10,2))
        FROM kafka_sales
        WHERE sale_total_price IS NOT NULL
    """)

    stmt_set.execute()

if __name__ == "__main__":
    main()
