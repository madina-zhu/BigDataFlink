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
│   └── init.sql                # DDL: таблицы звезды в PostgreSQL
├── producer/
│   ├── Dockerfile              # Образ Python-продюсера
│   └── producer.py             # CSV → JSON → Kafka
└── flink_job/
    └── job.py                  # Flink-джоба: Kafka → PostgreSQL
```

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
docker exec -d lab3-jobmanager \
  flink run -py /opt/flink/usrlib/job.py
```

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

Ожидаемый результат: в `fact_sales` загружено **10 000** строк, таблицы `dim_*` заполнены.

## Подключение через DBeaver

| Параметр | Значение |
| :--- | :--- |
| **Host** | `localhost` |
| **Port** | `15432` |
| **Database** | `lab3` |
| **User** | `lab` |
| **Password** | `lab` |

## Используемые технологии

* **Apache Kafka 7.5** — брокер сообщений
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
