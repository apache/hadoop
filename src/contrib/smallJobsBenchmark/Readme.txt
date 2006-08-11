SmallJobsBenchmark Readme : 

Building the benchmark. 
to build - 
$ cd smallJobsBenchmark
$ ant deploy

Running the benchmark
$ $HADOOP_HOME/bin/hadoop jar <path to MRBenchmark.jar> smallJobsBenchmark -inputLines <numLines> -maps <numMaps> -reduces <numReduces> -jar <path to MRBenchmark.jar>

For running the benchmark multiple times with different input sizes, maps, reduces following helper scripts can be used. 
$ cd build/contrib/smallJobsBenchmark
$ bin/run.sh

after successfully running the benchmark see logs/report.txt for consolidated output of all the runs. 

change this script to configure options. 

Configurable options are - 

-inputLines noOfLines 
  no of lines of input to generate. 

-inputType (ascending, descending, random)
  type of input to generate. 

-jar jarFilePath 
  Jar file containing Mapper and Reducer implementations in jar file. By default ant build creates MRBenchmark.jar file containing default Mapper and Reducer. 
  
-times numJobs 
No of times to run each MapReduce task, time is calculated as average of all runs. 

-workDir dfsPath 
DFS path to put output of MR tasks. 

-maps numMaps 
No of maps for wach task 

-reduces numReduces 
No of reduces for each task

-ignoreOutput
Doesn't copy the output back to local disk. Otherwise it creates the output back to a temp location on local disk. 
