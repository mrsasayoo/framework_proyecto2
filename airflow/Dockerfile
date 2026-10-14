# Proyecto 2 FYHBD | Persona 4
# Airflow 2.10.3 + Java + PySpark 3.5.3 + jars Iceberg/S3

FROM apache/airflow:2.10.3-python3.11

# -------------------------------------------------------------------
# Dependencias del sistema
# Java es necesario para PySpark.
# curl se utiliza para descargar los JARs.
# procps agrega utilidades como "ps", utilizadas por Spark.
# -------------------------------------------------------------------
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       curl \
       procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Dependencias Python
# Se instalan como el usuario airflow, como recomienda la imagen oficial.
# -------------------------------------------------------------------
USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

# -------------------------------------------------------------------
# JARs necesarios para:
# - Apache Iceberg
# - acceso AWS/S3 compatible con MinIO
#
# Se usan exactamente las mismas versiones que spark/Dockerfile
# y jupyter/Dockerfile para evitar incompatibilidades.
# -------------------------------------------------------------------
RUN set -eux; \
    J="$(python -c 'import os,pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    M=https://repo1.maven.org/maven2; \
    for u in \
      $M/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
      $M/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar \
      $M/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      $M/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ; do \
        curl -fsSL -o "$J/$(basename "$u")" "$u"; \
    done


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"