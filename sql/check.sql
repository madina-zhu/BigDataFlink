-- Проверка результата: docker exec -i lab3-postgres psql -U lab -d lab3 < sql/check.sql

SELECT 'fact_sales'   AS tbl, COUNT(*) FROM fact_sales
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller',   COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store',    COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier;

-- «Осиротевшие» ссылки факта на измерения (везде должно быть 0)
SELECT
  COUNT(*) FILTER (WHERE c.customer_id IS NULL) AS no_customer,
  COUNT(*) FILTER (WHERE s.seller_id   IS NULL) AS no_seller,
  COUNT(*) FILTER (WHERE p.product_id  IS NULL) AS no_product,
  COUNT(*) FILTER (WHERE st.store_id   IS NULL) AS no_store,
  COUNT(*) FILTER (WHERE su.supplier_id IS NULL) AS no_supplier
FROM fact_sales f
LEFT JOIN dim_customer c  ON c.customer_id  = f.customer_id
LEFT JOIN dim_seller   s  ON s.seller_id    = f.seller_id
LEFT JOIN dim_product  p  ON p.product_id   = f.product_id
LEFT JOIN dim_store    st ON st.store_id    = f.store_id
LEFT JOIN dim_supplier su ON su.supplier_id = f.supplier_id;

-- Пример аналитического запроса по звезде
SELECT p.product_category, COUNT(*) AS sales, SUM(f.sale_total_price) AS revenue
FROM fact_sales f JOIN dim_product p ON p.product_id = f.product_id
GROUP BY p.product_category
ORDER BY revenue DESC;
