#!/bin/bash

if [ -z $HADOOP_HOME ] 
then
  echo "Error HADOOP_HOME not defined"  ;
  exit 1;
fi

mkdir -p logs;

export TIMES=2

#for dataLines in 1 10000 10000000 
for dataLines in 1 100
 do 

for maps in 1 18
	do 
	for reduces in 1 18
	do
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/../contrib/smallJobsBenchmark/MRBenchmark.jar smallJobsBenchmark -inputLines ${dataLines} -output /hadoop/mapred/MROutput -jar $HADOOP_HOME/../contrib/smallJobsBenchmark/MRBenchmark.jar -times ${TIMES} -workDir /hadoop/mapred/work -maps ${maps} -reduces ${reduces} -inputType ascending  -ignoreOutput  2>&1 | tee logs/benchmark_${dataLines}_${maps}_${reduces}.log

	done
	done
	done

bin/report.sh
