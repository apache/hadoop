#!/bin/bash

if [ -z $HADOOP_HOME ] 
then
  echo "Error HADOOP_HOME not defined"  ;
  exit 1;
fi

if [ -z $JAVA_HOME ] 
then
  echo "Error JAVA_HOME not defined"  ;
  exit 1;
fi

export CLASSPATH=$HADOOP_HOME/conf:$HADOOP_HOME/build/classes:$HADOOP_HOME/build:$HADOOP_HOME/build/test/classes:$HADOOP_HOME/hadoop-*.jar:$HADOOP_HOME/lib/commons-cli-2.0-SNAPSHOT.jar:$HADOOP_HOME/lib/commons-logging-1.0.4.jar:$HADOOP_HOME/lib/commons-logging-api-1.0.4.jar:$HADOOP_HOME/lib/jetty-5.1.4.jar:$HADOOP_HOME/lib/junit-3.8.1.jar:$HADOOP_HOME/lib/log4j-1.2.13.jar:$HADOOP_HOME/lib/lucene-core-1.9.1.jar:$HADOOP_HOME/lib/servlet-api.jar:$HADOOP_HOME/lib/jetty-ext/ant.jar:$HADOOP_HOME/lib/jetty-ext/commons-el.jar:$HADOOP_HOME/lib/jetty-ext/jasper-compiler.jar:$HADOOP_HOME/lib/jetty-ext/jasper-runtime.jar:$HADOOP_HOME/lib/jetty-ext/jsp-api.jar

mkdir -p logs;

export TIMES=2

#for dataLines in 1 10000 10000000 
for dataLines in 1 100
 do 

for maps in 1 18
	do 
	for reduces in 1 18
	do
$JAVA_HOME/bin/java -classpath $CLASSPATH:./classes org.apache.hadoop.benchmarks.mapred.MultiJobRunner -inputLines ${dataLines} -output /hadoop/mapred/MROutput -jar MRBenchmark.jar -times ${TIMES} -workDir /hadoop/mapred/work -maps ${maps} -reduces ${reduces} -inputType ascending  -ignoreOutput  2>&1 | tee logs/benchmark_${dataLines}_${maps}_${reduces}.log

	done
	done
	done

bin/report.sh
