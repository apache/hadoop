To run the examples, first compile them:

% mvn install -Pdist,native

and then copy the binaries to dfs:

% hdfs dfs -put target/native/examples/wordcount-simple /examples/bin/

create an input directory with text files:

% hdfs dfs -put my-data in-dir

and run the word count example:

% mapred pipes -conf src/main/native/examples/conf/word.xml \
                   -input in-dir -output out-dir
