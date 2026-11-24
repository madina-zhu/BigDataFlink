FROM flink:1.18.0

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.18.0

RUN python3 -c "import urllib.request as u; \
    base = 'https://repo1.' + 'maven.org/maven2'; \
    files = [ \
      ('/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar', 'flink-sql-connector-kafka-3.1.0-1.18.jar'), \
      ('/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar', 'flink-connector-jdbc-3.1.2-1.18.jar'), \
      ('/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar', 'postgresql-42.6.0.jar') \
    ]; \
    [u.urlretrieve(base + p, '/opt/flink/lib/' + name) for p, name in files]"

USER flink
