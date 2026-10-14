FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
# [Persona 2] + openjdk-17 para que el notebook de Silver pueda ser el driver de Spark
RUN apt-get update && apt-get install -y --no-install-recommends curl git openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
# [Persona 2] jars de Iceberg y S3 dentro de pyspark (mismas versiones que spark/Dockerfile)
RUN set -eux; \
    J="$(python -c 'import os,pyspark;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')"; \
    M=https://repo1.maven.org/maven2; \
    for u in \
      $M/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
      $M/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar \
      $M/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      $M/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ; do \
        curl -fsSL -o "$J/$(basename $u)" "$u"; \
    done
WORKDIR /workspace
EXPOSE 8888
