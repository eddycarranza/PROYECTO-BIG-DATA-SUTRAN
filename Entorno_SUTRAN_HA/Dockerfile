# Java y Python se mantienen en las versiones usadas por el laboratorio.
FROM eclipse-temurin:11-jdk-jammy AS java11
FROM python:3.11-bookworm AS runtimebase
COPY --from=java11 /opt/java/openjdk /opt/java/openjdk
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates procps tini && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java/openjdk HADOOP_HOME=/opt/hadoop SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/java/openjdk/bin:/opt/spark/bin:/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
RUN java -version && python --version
FROM runtimebase AS final
# Las descargas conservan caché y se verifican con SHA-512.
COPY download_verified.py /opt/download_verified.py
RUN --mount=type=cache,id=sutran-apache-archives,target=/downloads,sharing=locked \
    python /opt/download_verified.py https://archive.apache.org/dist/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz /downloads/hadoop-3.4.3.tgz e25be7e57b4d3c5bfe83895844321a21d6cf7331266d524d8983c27cf484e576c3d79b3b60d590f8cddecf16229d0e232de2491b1b61b362ee7d67072c7290e1 && \
    tar -xzf /downloads/hadoop-3.4.3.tgz -C /opt && mv /opt/hadoop-3.4.3 /opt/hadoop
RUN --mount=type=cache,id=sutran-apache-archives,target=/downloads,sharing=locked \
    python /opt/download_verified.py https://archive.apache.org/dist/spark/spark-3.5.9/spark-3.5.9-bin-without-hadoop.tgz /downloads/spark-3.5.9.tgz 0ceafdcc46dff83272edbbe5a9a52c77e2c2c55672f67deaa33751e52e5e3ae13e3fc2c5deddec88d172a488cf8baa6c85cc0d2e88333217fdefccb303beda75 && \
    tar -xzf /downloads/spark-3.5.9.tgz -C /opt && mv /opt/spark-3.5.9-bin-without-hadoop /opt/spark
COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt
RUN useradd -m -u 1000 -s /bin/bash hadoop && mkdir -p /data/name /data/data /data/yarn/local /data/yarn/logs /data/journal /workspace && chown -R hadoop:hadoop /opt/hadoop /opt/spark /data /workspace
COPY conf/ /opt/base-conf/
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod 755 /opt/entrypoint.sh
ENV PYSPARK_PYTHON=/usr/local/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
COPY role_config.py /opt/role_config.py
COPY wait_tcp.py /opt/wait_tcp.py
USER hadoop
WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
