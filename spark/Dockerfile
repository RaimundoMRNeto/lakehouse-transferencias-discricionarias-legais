# Note: the -alpine variants of eclipse-temurin are published for amd64 only,
# so they do not build on Apple Silicon. The -jammy variants are multi-arch.
FROM eclipse-temurin:11-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /spark

ENV SPARK_VERSION 3.4.3
ENV HADOOP_VERSION 3

ENV SPARK_ARCHIVE spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}"
RUN tar -xf $SPARK_ARCHIVE
RUN rm $SPARK_ARCHIVE

ENV SPARK_DIR "/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

# Baixar JARs necessários para S3A + Delta Lake
RUN wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

EXPOSE 10001
EXPOSE 10000

ENTRYPOINT java \
 -Duser.timezone=Etc/UTC \
 -Xmx512m \
 -cp "${SPARK_DIR}/conf:${SPARK_DIR}/jars/*" \
 org.apache.spark.deploy.SparkSubmit \
 --master spark://spark-master:7077 \
 --conf spark.executor.extraJavaOptions=-Duser.timezone=Etc/UTC \
 --conf spark.eventLog.enabled=false \
 --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
 --name "Thrift JDBC/ODBC Server" \
 --executor-memory 512m \
 --executor-cores 1 \
 --driver-memory 512m \
 --conf spark.cores.max=1 \
 --conf spark.executor.instances=1 \
 --conf spark.sql.catalogImplementation=hive \
 --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/metastore/metastore_db;create=true" \
 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
 --conf spark.hadoop.fs.s3a.access.key=minio \
 --conf spark.hadoop.fs.s3a.secret.key=minio123 \
 --conf spark.hadoop.fs.s3a.path.style.access=true \
 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
 --conf spark.sql.warehouse.dir=s3a://gold/warehouse \
 --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
 --conf spark.jars.packages=io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore \
 spark-internal


