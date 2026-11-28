# Лабораторная работа №3 — Streaming processing с Apache Flink

## Описание

Реализация потоковой обработки данных с использованием Apache Flink.
Конвейер: **CSV → Kafka → Flink → PostgreSQL (модель «звезда»)**.

Каждая строка исходных CSV-файлов превращается в JSON-сообщение и
отправляется в Kafka-топик `sales_topic`. Flink-джоба читает этот топик
в режиме streaming, трансформирует данные в модель «звезда» и пишет
результат в PostgreSQL.

## Архитектура

```text
CSV files
    │
    ▼
┌──────────┐    write    ┌────────┐    read    ┌───────────────┐    write    ┌──────────────┐
│ producer │ ──────────► │ Kafka  │ ─────────► │ Flink (Table  │ ──────────► │  PostgreSQL  │
│ (Docker) │             │ topic  │            │   API + SQL)  │             │ (star schema)│
└──────────┘             └────────┘            └───────────────┘             └──────────────┘
```

Модель данных — «звезда»:

* `fact_sales` — центральная таблица фактов (продажи)
* `dim_customer` — измерение «клиенты»
* `dim_seller` — измерение «продавцы»
* `dim_product` — измерение «товары»
* `dim_store` — измерение «магазины»
* `dim_supplier` — измерение «поставщики»

## Структура репозитория

```text
.
├── README.md
├── Dockerfile                  # Образ Flink 1.18 с поддержкой PyFlink и коннекторами Kafka/JDBC
├── docker-compose.yml          # Postgres, Kafka, Zookeeper, Flink, Producer
├── mock_data/                  # 10 CSV-файлов с исходными данными
│   ├── mock_data_1.csv
│   ├── ...
│   └── mock_data_10.csv
├── sql/
│   ├── init.sql                # DDL: таблицы звезды в PostgreSQL
│   └── check.sql               # Запросы для проверки результата
├── producer/
│   ├── Dockerfile              # Образ Python-продюсера
│   └── producer.py             # CSV → JSON → Kafka
└── flink_job/
    └── job.py                  # Flink-джоба: Kafka → PostgreSQL
```

## Особенности реализации

* **Ключи измерений** — детерминированные `MD5`-хэши натуральных ключей, вычисляемые в Flink SQL
  (`dim_customer`: email + имя + фамилия; `dim_product`: название, категория, цена, бренд, цвет, размер, даты и т. д.).
  В исходных CSV поля `sale_*_id` повторяются в каждом файле (1…1000), поэтому использовать их как ключи нельзя.
* **Идемпотентность**: Flink пишет в PostgreSQL через JDBC upsert по `PRIMARY KEY`, а ключи детерминированы,
  поэтому повторная отправка данных (`docker compose restart producer`) не создаёт дублей.
* **FOREIGN KEY не объявлены**: в streaming-режиме джоба параллельно пишет в 6 таблиц, и факт может попасть в БД раньше
  измерения. Целостность проверяется запросом из `sql/check.sql` (после загрузки «осиротевших» ссылок нет).
* Пустые значения (`customer_postal_code`, `seller_postal_code`, `store_state`) сохраняются как `NULL`;
  даты формата `M/d/yyyy` преобразуются в `DATE`.

## Требования

* Docker Desktop ≥ 4.x (или Docker + Docker Compose v2)
* DBeaver (для просмотра данных) — по желанию

## Шаги запуска

### 1. Клонировать репозиторий

```bash
git clone <URL_РЕПОЗИТОРИЯ>
cd <РЕПОЗИТОРИЙ>
```

### 2. Поднять инфраструктуру и запустить producer

```bash
docker compose up -d --build
```

Контейнер `lab3-producer` автоматически дождётся готовности Kafka и отправит 10 000 сообщений в топик `sales_topic`. Проверить его работу можно командой:

```bash
docker logs lab3-producer
```

Ожидаемый вывод:
```text
[+] Sending mock_data/mock_data_1.csv
...
[✓] Sent 10000 messages to topic 'sales_topic'
```

*(При необходимости повторной отправки данных выполните `docker compose restart producer`).*

### 3. Запустить Flink-джобу

```bash
docker exec lab3-jobmanager flink run -d -py /opt/flink/usrlib/job.py
```

Команда выведет `Job has been submitted with JobID ...`. Ошибки (если будут) видны сразу в выводе команды и в Flink Web UI.

Проверить статус задачи можно в Flink Web UI (`http://localhost:8081`, раздел **Running Jobs**) или через терминал:

```bash
docker exec -it lab3-jobmanager flink list
```

### 4. Проверить результат в PostgreSQL

```bash
docker exec -it lab3-postgres psql -U lab -d lab3 -c \
  "SELECT COUNT(*) FROM fact_sales;"

docker exec -it lab3-postgres psql -U lab -d lab3 -c \
  "SELECT * FROM dim_customer LIMIT 5;"
```

Или одним скриптом (счётчики по всем таблицам, проверка ссылок факта на измерения, пример аналитики):

```bash
docker exec -i lab3-postgres psql -U lab -d lab3 < sql/check.sql
```

Ожидаемый результат (данные грузятся в течение нескольких секунд после старта джобы): в `fact_sales` — **10 000** строк,
в каждой из таблиц `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier` — по **10 000** строк
(в исходных данных все значения натуральных ключей уникальны); `no_*` в проверке ссылок равны 0.

## Подключение через DBeaver

| Параметр | Значение |
| :--- | :--- |
| **Host** | `localhost` |
| **Port** | `15432` |
| **Database** | `lab3` |
| **User** | `lab` |
| **Password** | `lab` |

## Используемые технологии

* **Apache Kafka** (Confluent Platform 7.5) — брокер сообщений
* **Apache Flink 1.18** — потоковая обработка (PyFlink Table API + SQL)
* **PostgreSQL 15** — хранилище данных (модель «звезда»)
* **Python 3.11** — контейнеризированный продюсер CSV → JSON

## Остановка

```bash
docker compose down            # остановить, сохранить данные
docker compose down -v         # остановить и удалить тома (данные БД)
```

## Автор

Жувангараева Мадина Ермуратовна, группа 205
