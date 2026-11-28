FROM flink:1.18.0

USER root

# Python для PyFlink (python3-dev нужен для сборки зависимостей на ARM/без готовых wheel)
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.18.0

# Коннекторы Kafka и JDBC + драйвер PostgreSQL
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar /opt/flink/lib/

USER flink
