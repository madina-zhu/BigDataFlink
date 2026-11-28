"""Flink-джоба (PyFlink Table API + SQL): Kafka -> модель «звезда» -> PostgreSQL.

Запуск (внутри контейнера jobmanager):
    flink run -d -py /opt/flink/usrlib/job.py
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales_topic")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/lab3")
PG_USER = os.getenv("PG_USER", "lab")
PG_PASSWORD = os.getenv("PG_PASSWORD", "lab")

SOURCE_COLUMNS = [
    "id", "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed",
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code",
    "product_name", "product_category", "product_price", "product_quantity",
    "sale_date", "sale_customer_id", "sale_seller_id", "sale_product_id",
    "sale_quantity", "sale_total_price",
    "store_name", "store_location", "store_city", "store_state",
    "store_country", "store_phone", "store_email",
    "pet_category", "product_weight", "product_color", "product_size",
    "product_brand", "product_material", "product_description",
    "product_rating", "product_reviews", "product_release_date",
    "product_expiry_date",
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country",
]

# Детерминированные ключи измерений: MD5 от натурального ключа.
# CONCAT_WS пропускает NULL, поэтому ключ считается всегда.
CUSTOMER_KEY = "MD5(CONCAT_WS('|', customer_email, customer_first_name, customer_last_name))"
SELLER_KEY = "MD5(CONCAT_WS('|', seller_email, seller_first_name, seller_last_name))"
PRODUCT_KEY = (
    "MD5(CONCAT_WS('|', product_name, product_category, product_price, product_brand, "
    "product_color, product_size, product_release_date, product_expiry_date))"
)
STORE_KEY = "MD5(CONCAT_WS('|', store_name, store_location, store_city, store_state, store_country))"
SUPPLIER_KEY = "MD5(CONCAT_WS('|', supplier_name, supplier_address, supplier_city, supplier_country))"
# id в CSV повторяется в разных файлах (1..1000), поэтому в ключ факта входят и другие поля.
SALE_KEY = (
    "MD5(CONCAT_WS('|', id, sale_date, customer_email, seller_email, "
    "product_name, store_phone, sale_quantity, sale_total_price))"
)


def main():
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    conf = env.get_config()
    conf.set("parallelism.default", "1")
    conf.set("execution.checkpointing.interval", "10s")

    # ---------- Источник: Kafka (JSON, все поля читаем как STRING) ----------
    columns = ",\n            ".join(f"`{c}` STRING" for c in SOURCE_COLUMNS)
    env.execute_sql(f"""
        CREATE TABLE kafka_sales (
            {columns}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-lab3',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ---------- Приёмники: PostgreSQL (JDBC upsert по PRIMARY KEY) ----------
    def jdbc_sink(table_name, columns_ddl):
        env.execute_sql(f"""
            CREATE TABLE {table_name} (
                {columns_ddl}
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{PG_URL}',
                'table-name' = '{table_name}',
                'username' = '{PG_USER}',
                'password' = '{PG_PASSWORD}',
                'sink.buffer-flush.max-rows' = '500',
                'sink.buffer-flush.interval' = '2s'
            )
        """)

    jdbc_sink("dim_customer", """
        customer_id STRING, first_name STRING, last_name STRING, age INT,
        email STRING, country STRING, postal_code STRING,
        pet_type STRING, pet_name STRING, pet_breed STRING,
        PRIMARY KEY (customer_id) NOT ENFORCED
    """)
    jdbc_sink("dim_seller", """
        seller_id STRING, first_name STRING, last_name STRING,
        email STRING, country STRING, postal_code STRING,
        PRIMARY KEY (seller_id) NOT ENFORCED
    """)
    jdbc_sink("dim_product", """
        product_id STRING, product_name STRING, product_category STRING,
        product_price DECIMAL(10,2), product_quantity INT,
        product_weight DECIMAL(10,2), product_color STRING,
        product_size STRING, product_brand STRING, product_material STRING,
        product_description STRING, product_rating DECIMAL(3,1),
        product_reviews INT, product_release_date DATE,
        product_expiry_date DATE, pet_category STRING,
        PRIMARY KEY (product_id) NOT ENFORCED
    """)
    jdbc_sink("dim_store", """
        store_id STRING, store_name STRING, store_location STRING,
        store_city STRING, store_state STRING, store_country STRING,
        store_phone STRING, store_email STRING,
        PRIMARY KEY (store_id) NOT ENFORCED
    """)
    jdbc_sink("dim_supplier", """
        supplier_id STRING, supplier_name STRING, supplier_contact STRING,
        supplier_email STRING, supplier_phone STRING, supplier_address STRING,
        supplier_city STRING, supplier_country STRING,
        PRIMARY KEY (supplier_id) NOT ENFORCED
    """)
    jdbc_sink("fact_sales", """
        sale_id STRING, customer_id STRING, seller_id STRING, product_id STRING,
        store_id STRING, supplier_id STRING,
        sale_date DATE, sale_quantity INT, sale_total_price DECIMAL(10,2),
        PRIMARY KEY (sale_id) NOT ENFORCED
    """)

    # ---------- Трансформация ----------
    # Пустые строки из CSV (postal_code, store_state) превращаем в NULL.
    # Даты в CSV имеют формат M/d/yyyy (например 5/14/2021).
    stmt_set = env.create_statement_set()

    stmt_set.add_insert_sql(f"""
        INSERT INTO dim_customer
        SELECT
            {CUSTOMER_KEY},
            customer_first_name, customer_last_name,
            TRY_CAST(customer_age AS INT), customer_email, customer_country,
            NULLIF(customer_postal_code, ''),
            customer_pet_type, customer_pet_name, customer_pet_breed
        FROM kafka_sales
        WHERE customer_email IS NOT NULL
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO dim_seller
        SELECT
            {SELLER_KEY},
            seller_first_name, seller_last_name, seller_email, seller_country,
            NULLIF(seller_postal_code, '')
        FROM kafka_sales
        WHERE seller_email IS NOT NULL
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO dim_product
        SELECT
            {PRODUCT_KEY},
            product_name, product_category,
            TRY_CAST(product_price AS DECIMAL(10,2)),
            TRY_CAST(product_quantity AS INT),
            TRY_CAST(product_weight AS DECIMAL(10,2)),
            product_color, product_size, product_brand, product_material,
            product_description,
            TRY_CAST(product_rating AS DECIMAL(3,1)),
            TRY_CAST(product_reviews AS INT),
            TO_DATE(product_release_date, 'M/d/yyyy'),
            TO_DATE(product_expiry_date, 'M/d/yyyy'),
            pet_category
        FROM kafka_sales
        WHERE product_name IS NOT NULL
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO dim_store
        SELECT
            {STORE_KEY},
            store_name, store_location, store_city,
            NULLIF(store_state, ''),
            store_country, store_phone, store_email
        FROM kafka_sales
        WHERE store_name IS NOT NULL
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO dim_supplier
        SELECT
            {SUPPLIER_KEY},
            supplier_name, supplier_contact, supplier_email,
            supplier_phone, supplier_address, supplier_city, supplier_country
        FROM kafka_sales
        WHERE supplier_name IS NOT NULL
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO fact_sales
        SELECT
            {SALE_KEY},
            {CUSTOMER_KEY},
            {SELLER_KEY},
            {PRODUCT_KEY},
            {STORE_KEY},
            {SUPPLIER_KEY},
            TO_DATE(sale_date, 'M/d/yyyy'),
            TRY_CAST(sale_quantity AS INT),
            TRY_CAST(sale_total_price AS DECIMAL(10,2))
        FROM kafka_sales
        WHERE id IS NOT NULL
    """)

    # Streaming-джоба бесконечная: сабмитим и не ждём завершения.
    stmt_set.execute()


if __name__ == "__main__":
    main()
