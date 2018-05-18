#!/bin/sh

INDEX_CAT=$1
WORK_DATE=$2
INDUSTRY_CODE=$3
PARTITIONS=$4
EXPORT2REDIS=$5
EXPORT2HBASE=$6
METRICS_DEBUG=$7

export METRICS_CFG_DB_SERVER=59.212.226.51
export METRICS_CFG_DB_PORT=3306
export METRICS_CFG_DB_USER=root
export METRICS_CFG_DB_PASSWORD=123456
export METRICS_CFG_DB_NAME=user_profile

export REDIS_HOST_IP=59.212.226.49
export REDIS_HOST_PORT=6379

if [ -z "$INDEX_CAT" -o -z "$WORK_DATE" -o -z "$INDUSTRY_CODE" ]; then
	echo "Usage: $0 <INDEX_CAT> <WORK_DATE> <INDUSTRY_CODE>"
	exit -1
fi

if [ -z "$PARTITIONS" ]; then
	PARTITIONS=1024
fi

if [ -z "$EXPORT2REDIS" ]; then
	EXPORT2REDIS=true
fi

if [ -z "$EXPORT2HBASE" ]; then
	EXPORT2HBASE=true
fi

if [ -z "$METRICS_DEBUG" ]; then
	METRICS_DEBUG=true
fi

#INDUSTRY_CODE=1000		# 人口

if [ "$INDEX_CAT" != "1" ]; then
	echo "Invalid INDEX_CAT, please choose one from 1."
	exit -1
fi

DEPS="\
./lib/activation-1.1.jar,\
./lib/akka-actor_2.10-2.3.11.jar,\
./lib/akka-remote_2.10-2.3.11.jar,\
./lib/akka-slf4j_2.10-2.3.11.jar,\
./lib/antlr-2.7.7.jar,\
./lib/antlr-runtime-3.4.jar,\
./lib/aopalliance-1.0.jar,\
./lib/apache-log4j-extras-1.2.17.jar,\
./lib/asm-3.1.jar,\
./lib/avro-1.7.7.jar,\
./lib/avro-ipc-1.7.7.jar,\
./lib/avro-ipc-1.7.7-tests.jar,\
./lib/avro-mapred-1.7.7-hadoop2.jar,\
./lib/bonecp-0.8.0.RELEASE.jar,\
./lib/calcite-avatica-1.2.0-incubating.jar,\
./lib/calcite-core-1.2.0-incubating.jar,\
./lib/calcite-linq4j-1.2.0-incubating.jar,\
./lib/chill_2.10-0.5.0.jar,\
./lib/chill-java-0.5.0.jar,\
./lib/commons-beanutils-1.7.0.jar,\
./lib/commons-beanutils-core-1.8.0.jar,\
./lib/commons-cli-1.2.jar,\
./lib/commons-codec-1.10.jar,\
./lib/commons-collections-3.2.1.jar,\
./lib/commons-compiler-2.7.6.jar,\
./lib/commons-compress-1.4.1.jar,\
./lib/commons-configuration-1.6.jar,\
./lib/commons-daemon-1.0.13.jar,\
./lib/commons-dbcp-1.4.jar,\
./lib/commons-digester-1.8.jar,\
./lib/commons-el-1.0.jar,\
./lib/commons-httpclient-3.1.jar,\
./lib/commons-io-2.4.jar,\
./lib/commons-lang-2.6.jar,\
./lib/commons-lang3-3.3.2.jar,\
./lib/commons-logging-1.1.1.jar,\
./lib/commons-math-2.1.jar,\
./lib/commons-math3-3.4.1.jar,\
./lib/commons-net-2.2.jar,\
./lib/commons-pool-1.5.4.jar,\
./lib/commons-pool2-2.4.2.jar,\
./lib/compress-lzf-1.0.3.jar,\
./lib/config-1.2.1.jar,\
./lib/curator-client-2.4.0.jar,\
./lib/curator-framework-2.4.0.jar,\
./lib/curator-recipes-2.4.0.jar,\
./lib/datanucleus-api-jdo-3.2.6.jar,\
./lib/datanucleus-core-3.2.10.jar,\
./lib/datanucleus-rdbms-3.2.9.jar,\
./lib/derby-10.10.2.0.jar,\
./lib/eigenbase-properties-1.1.5.jar,\
./lib/fastjson-1.1.41.jar,\
./lib/findbugs-annotations-1.3.9-1.jar,\
./lib/gmbal-api-only-3.0.0-b023.jar,\
./lib/grizzly-framework-2.1.2.jar,\
./lib/grizzly-http-2.1.2.jar,\
./lib/grizzly-http-server-2.1.2.jar,\
./lib/grizzly-http-servlet-2.1.2.jar,\
./lib/grizzly-rcm-2.1.2.jar,\
./lib/guava-12.0.1.jar,\
./lib/guice-3.0.jar,\
./lib/guice-servlet-3.0.jar,\
./lib/hadoop-annotations-2.2.0.jar,\
./lib/hadoop-auth-2.2.0.jar,\
./lib/hadoop-client-2.2.0.jar,\
./lib/hadoop-common-2.2.0.jar,\
./lib/hadoop-hdfs-2.2.0.jar,\
./lib/hadoop-mapreduce-client-app-2.2.0.jar,\
./lib/hadoop-mapreduce-client-common-2.2.0.jar,\
./lib/hadoop-mapreduce-client-core-2.2.0.jar,\
./lib/hadoop-mapreduce-client-jobclient-2.2.0.jar,\
./lib/hadoop-mapreduce-client-shuffle-2.2.0.jar,\
./lib/hadoop-yarn-api-2.2.0.jar,\
./lib/hadoop-yarn-client-2.2.0.jar,\
./lib/hadoop-yarn-common-2.2.0.jar,\
./lib/hadoop-yarn-server-common-2.2.0.jar,\
./lib/hamcrest-core-1.3.jar,\
./lib/hbase-annotations-0.98.13-hadoop2.jar,\
./lib/hbase-client-0.98.13-hadoop2.jar,\
./lib/hbase-common-0.98.13-hadoop2.jar,\
./lib/hbase-common-0.98.13-hadoop2-tests.jar,\
./lib/hbase-hadoop2-compat-0.98.13-hadoop2.jar,\
./lib/hbase-hadoop-compat-0.98.13-hadoop2.jar,\
./lib/hbase-prefix-tree-0.98.13-hadoop2.jar,\
./lib/hbase-protocol-0.98.13-hadoop2.jar,\
./lib/hbase-server-0.98.13-hadoop2.jar,\
./lib/high-scale-lib-1.1.1.jar,\
./lib/hive-cli-1.2.1.spark.jar,\
./lib/hive-exec-1.2.1.spark.jar,\
./lib/hive-metastore-1.2.1.spark.jar,\
./lib/htrace-core-2.04.jar,\
./lib/httpclient-4.3.2.jar,\
./lib/httpcore-4.3.1.jar,\
./lib/ivy-2.4.0.jar,\
./lib/jackson-annotations-2.4.4.jar,\
./lib/jackson-core-2.4.4.jar,\
./lib/jackson-core-asl-1.8.8.jar,\
./lib/jackson-databind-2.4.4.jar,\
./lib/jackson-jaxrs-1.8.8.jar,\
./lib/jackson-mapper-asl-1.9.13.jar,\
./lib/jackson-module-scala_2.10-2.4.4.jar,\
./lib/jackson-xc-1.8.3.jar,\
./lib/jamon-runtime-2.3.1.jar,\
./lib/janino-2.7.8.jar,\
./lib/jasper-compiler-5.5.23.jar,\
./lib/jasper-runtime-5.5.23.jar,\
./lib/JavaEWAH-0.3.2.jar,\
./lib/javax.inject-1.jar,\
./lib/javax.servlet-3.0.0.v201112011016.jar,\
./lib/javax.servlet-3.1.jar,\
./lib/javax.servlet-api-3.0.1.jar,\
./lib/javolution-5.5.1.jar,\
./lib/jaxb-api-2.2.2.jar,\
./lib/jaxb-impl-2.2.3-1.jar,\
./lib/jcl-over-slf4j-1.7.10.jar,\
./lib/jcodings-1.0.8.jar,\
./lib/jdo-api-3.0.1.jar,\
./lib/jedis-2.8.1.jar,\
./lib/jersey-client-1.9.jar,\
./lib/jersey-core-1.9.jar,\
./lib/jersey-grizzly2-1.9.jar,\
./lib/jersey-guice-1.9.jar,\
./lib/jersey-json-1.9.jar,\
./lib/jersey-server-1.9.jar,\
./lib/jersey-test-framework-core-1.9.jar,\
./lib/jersey-test-framework-grizzly2-1.9.jar,\
./lib/jets3t-0.7.1.jar,\
./lib/jettison-1.1.jar,\
./lib/jetty-6.1.26.jar,\
./lib/jetty-sslengine-6.1.26.jar,\
./lib/jetty-util-6.1.26.jar,\
./lib/jline-2.12.jar,\
./lib/joda-time-2.9.3.jar,\
./lib/jodd-core-3.5.2.jar,\
./lib/joni-2.1.2.jar,\
./lib/jsch-0.1.42.jar,\
./lib/json-20090211.jar,\
./lib/json4s_2.9.2-3.1.0-sources.jar,\
./lib/json4s-ast_2.10-3.2.10.jar,\
./lib/json4s-core_2.10-3.2.10.jar,\
./lib/json4s-jackson_2.10-3.2.10.jar,\
./lib/jsp-2.1-6.1.14.jar,\
./lib/jsp-api-2.1-6.1.14.jar,\
./lib/jsr305-1.3.9.jar,\
./lib/jta-1.1.jar,\
./lib/jul-to-slf4j-1.7.10.jar,\
./lib/kryo-2.21.jar,\
./lib/leveldbjni-all-1.8.jar,\
./lib/libfb303-0.9.2.jar,\
./lib/libthrift-0.9.2.jar,\
./lib/lift-json-2.0.jar,\
./lib/log4j-1.2.17.jar,\
./lib/lz4-1.3.0.jar,\
./lib/management-api-3.0.0-b012.jar,\
./lib/mesos-0.21.1-shaded-protobuf.jar,\
./lib/metrics-core-2.2.0.jar,\
./lib/metrics-core-3.1.2.jar,\
./lib/metrics-graphite-3.1.2.jar,\
./lib/metrics-json-3.1.2.jar,\
./lib/metrics-jvm-3.1.2.jar,\
./lib/minlog-1.2.jar,\
./lib/mysql-connector-java-5.1.18.jar,\
./lib/netty-3.6.6.Final.jar,\
./lib/netty-all-4.0.29.Final.jar,\
./lib/objenesis-1.2.jar,\
./lib/opencsv-2.3.jar,\
./lib/oro-2.0.8.jar,\
./lib/paranamer-2.0.jar,\
./lib/parquet-column-1.7.0.jar,\
./lib/parquet-common-1.7.0.jar,\
./lib/parquet-encoding-1.7.0.jar,\
./lib/parquet-format-2.3.0-incubating.jar,\
./lib/parquet-generator-1.7.0.jar,\
./lib/parquet-hadoop-1.7.0.jar,\
./lib/parquet-hadoop-bundle-1.6.0.jar,\
./lib/parquet-jackson-1.7.0.jar,\
./lib/protobuf-java-2.5.0.jar,\
./lib/py4j-0.9.jar,\
./lib/pyrolite-4.9.jar,\
./lib/reflectasm-1.07-shaded.jar,\
./lib/RoaringBitmap-0.5.11.jar,\
./lib/scala-compiler-2.10.0.jar,\
./lib/scala-library-2.10.5.jar,\
./lib/scalap-2.10.0.jar,\
./lib/scala-reflect-2.10.4.jar,\
./lib/servlet-api-2.5-6.1.14.jar,\
./lib/slf4j-api-1.7.10.jar,\
./lib/slf4j-log4j12-1.7.10.jar,\
./lib/snappy-0.2.jar,\
./lib/snappy-java-1.1.2.6.jar,\
./lib/spark-catalyst_2.10-1.6.3.jar,\
./lib/spark-core_2.10-1.6.3.jar,\
./lib/spark-hive_2.10-1.6.3.jar,\
./lib/spark-launcher_2.10-1.6.3.jar,\
./lib/spark-network-common_2.10-1.6.3.jar,\
./lib/spark-network-shuffle_2.10-1.6.3.jar,\
./lib/spark-sql_2.10-1.6.3.jar,\
./lib/spark-unsafe_2.10-1.6.3.jar,\
./lib/ST4-4.0.4.jar,\
./lib/stax-api-1.0.1.jar,\
./lib/stream-2.7.0.jar,\
./lib/stringtemplate-3.2.1.jar,\
./lib/tachyon-client-0.8.2.jar,\
./lib/tachyon-underfs-hdfs-0.8.2.jar,\
./lib/tachyon-underfs-local-0.8.2.jar,\
./lib/tachyon-underfs-s3-0.8.2.jar,\
./lib/uncommons-maths-1.2.2a.jar,\
./lib/unused-1.0.0.jar,\
./lib/xbean-asm5-shaded-4.4.jar,\
./lib/xmlenc-0.52.jar,\
./lib/xz-1.0.jar,\
./lib/zookeeper-3.4.6.jar"

CURR_TIME=`date +%Y%m%d%H%M`
LOG_FILE=metrics_${INDUSTRY_CODE}_${CURR_TIME}.log

echo "Please wait ..."
echo

spark-submit \
	--class com.evergrande.bigdata.MetricsLoader \
	--master yarn \
	--num-executors 8 \
	--driver-memory 4g \
	--executor-memory 32g \
	--executor-cores 4 \
	--jars $DEPS \
	--conf spark.default.parallelism=$PARTITIONS \
	--conf spark.yarn.queue=etl \
	chestnut-1.0-SNAPSHOT.jar cluster $INDEX_CAT $WORK_DATE $INDUSTRY_CODE $PARTITIONS $EXPORT2REDIS $EXPORT2HBASE $METRICS_DEBUG 1>${LOG_FILE} 2>&1

export RET=$?

grep Excep ${LOG_FILE} > /dev/null
if [ $? -eq 1 ]; then
	echo "Job finished successfully."
	echo "Please refer to HBase table 'user_metrics_test/user_discrete_metrics_test' for the result."
	exit $RET
else
	echo "Job failed with exception, please check ${LOG_FILE} for more details."
	exit $RET
fi


