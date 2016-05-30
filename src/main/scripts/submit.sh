#!/bin/sh

WORK_DATE=$1
INDUSTRY_CODE=$2

if [ -z "$WORK_DATE" -o -z "$INDUSTRY_CODE" ]; then
	echo "Usage: $0 <WORK_DATE> <INDUSTRY_CODE>"
	exit -1
fi

#INDUSTRY_CODE=1100		# 房地产
#INDUSTRY_CODE=2000		# 酒店
#INDUSTRY_CODE=3110		# 足球

if [ "$INDUSTRY_CODE" != "1100" -a "$INDUSTRY_CODE" != "2000" -a "$INDUSTRY_CODE" != "3110" ]; then
	echo "Invalid INDUSTRY_CODE, please choose one from 1100, 2000 and 3110."
	exit -1
fi

DEPS="\
./lib/mysql-connector-java-5.1.38.jar,\
./lib/activation-1.1.jar,\
./lib/aopalliance-1.0.jar,\
./lib/asm-3.1.jar,\
./lib/avro-1.7.4.jar,\
./lib/commons-beanutils-1.7.0.jar,\
./lib/commons-beanutils-core-1.8.0.jar,\
./lib/commons-cli-1.2.jar,\
./lib/commons-codec-1.7.jar,\
./lib/commons-collections-3.2.1.jar,\
./lib/commons-compress-1.4.1.jar,\
./lib/commons-configuration-1.6.jar,\
./lib/commons-daemon-1.0.13.jar,\
./lib/commons-digester-1.8.jar,\
./lib/commons-el-1.0.jar,\
./lib/commons-httpclient-3.1.jar,\
./lib/commons-io-2.4.jar,\
./lib/commons-lang-2.6.jar,\
./lib/commons-logging-1.1.1.jar,\
./lib/commons-math-2.1.jar,\
./lib/commons-net-3.1.jar,\
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
./lib/htrace-core-2.04.jar,\
./lib/jackson-core-asl-1.8.8.jar,\
./lib/jackson-jaxrs-1.8.8.jar,\
./lib/jackson-mapper-asl-1.8.8.jar,\
./lib/jackson-xc-1.8.3.jar,\
./lib/jamon-runtime-2.3.1.jar,\
./lib/jasper-compiler-5.5.23.jar,\
./lib/jasper-runtime-5.5.23.jar,\
./lib/javax.inject-1.jar,\
./lib/javax.servlet-3.1.jar,\
./lib/javax.servlet-api-3.0.1.jar,\
./lib/jaxb-api-2.2.2.jar,\
./lib/jaxb-impl-2.2.3-1.jar,\
./lib/jcodings-1.0.8.jar,\
./lib/jdk.tools-1.7.jar,\
./lib/jersey-client-1.9.jar,\
./lib/jersey-core-1.9.jar,\
./lib/jersey-grizzly2-1.9.jar,\
./lib/jersey-guice-1.9.jar,\
./lib/jersey-json-1.9.jar,\
./lib/jersey-server-1.9.jar,\
./lib/jersey-test-framework-core-1.9.jar,\
./lib/jersey-test-framework-grizzly2-1.9.jar,\
./lib/jets3t-0.6.1.jar,\
./lib/jettison-1.1.jar,\
./lib/jetty-6.1.26.jar,\
./lib/jetty-sslengine-6.1.26.jar,\
./lib/jetty-util-6.1.26.jar,\
./lib/joni-2.1.2.jar,\
./lib/jsch-0.1.42.jar,\
./lib/jsp-2.1-6.1.14.jar,\
./lib/jsp-api-2.1-6.1.14.jar,\
./lib/jsr305-1.3.9.jar,\
./lib/junit-4.11.jar,\
./lib/log4j-1.2.17.jar,\
./lib/management-api-3.0.0-b012.jar,\
./lib/metrics-core-2.2.0.jar,\
./lib/netty-3.6.6.Final.jar,\
./lib/paranamer-2.3.jar,\
./lib/protobuf-java-2.5.0.jar,\
./lib/servlet-api-2.5-6.1.14.jar,\
./lib/slf4j-api-1.7.2.jar,\
./lib/slf4j-log4j12-1.6.1.jar,\
./lib/snappy-java-1.0.4.1.jar,\
./lib/xmlenc-0.52.jar,\
./lib/xz-1.0.jar,\
./lib/zookeeper-3.4.6.jar"

CURR_TIME=`date +%Y%m%d%H%M`
LOG_FILE=metrics_loading_${CURR_TIME}.log

echo "Please wait ..."
echo

spark-submit \
	--class com.hd.bigdata.WordCount \
	--master yarn \
	--num-executors 8 \
	--driver-memory 1g \
	--executor-memory 8g \
	--executor-cores 4 \
	--jars $DEPS \
	wordcount-1.0-SNAPSHOT.jar cluster $WORK_DATE $INDUSTRY_CODE 1>${LOG_FILE} 2>&1

grep Excep ${LOG_FILE} > /dev/null
if [ $? -eq 1 ]; then
	echo "Job finished successfully."
	echo "Please check table 'user_metrics_test/user_discrete_metrics_test' in HBase for the result."
else
	echo "Job failed with exception, please check ${LOG_FILE} for more details."
fi


