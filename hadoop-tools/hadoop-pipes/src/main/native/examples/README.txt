To run the examples, first compile them:

% mvn install 

and then copy the binaries to dfs:

% hadoop fs -put target/native/wordcount-simple /examples/bin/

create an input directory with text files:

% hadoop fs -put my-data in-dir

and run the word count example:

% hadoop pipes -conf src/main/native/examples/conf/word.xml \
                   -input in-dir -output out-dir
