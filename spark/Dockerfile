# Spark 3.5.3 (master/worker) + jars de Iceberg y S3 ya incluidos (sin descargas en tiempo de ejecucion)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
ARG MVN=https://repo1.maven.org/maven2
ADD ${MVN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar /opt/spark/jars/
ADD ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar /opt/spark/jars/
ADD ${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD ${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/iceberg-*.jar /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar
USER spark
