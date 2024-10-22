YARN (YET ANOTHER RESOURCE NEGOTIATOR or YARN Application Resource Negotiator)
------------------------------------------------------------------------------

Requirements
-------------
Java: JDK 1.6
Maven: Maven 3

Setup
-----
Install protobuf 2.5.0 (Download from http://code.google.com/p/protobuf/downloads/list)
 - install the protoc executable (configure, make, make install)
 - install the maven artifact (cd java; mvn install)


Quick Maven Tips
----------------
clean workspace: mvn clean
compile and test: mvn install
skip tests: mvn install -DskipTests
skip test execution but compile: mvn install -Dmaven.test.skip.exec=true
clean and test: mvn clean install
run selected test after compile: mvn test -Dtest=TestClassName (combined: mvn clean install -Dtest=TestClassName)
create runnable binaries after install: mvn assembly:assembly -Pnative (combined: mvn clean install assembly:assembly -Pnative)

Eclipse Projects
----------------
http://maven.apache.org/guides/mini/guide-ide-eclipse.html

1. Generate .project and .classpath files in all maven modules
mvn eclipse:eclipse
CAUTION: If the project structure has changed from your previous workspace, clean up all .project and .classpath files recursively. Then run:
mvn eclipse:eclipse

2. Import the projects in eclipse.

3. Set the environment variable M2_REPO to point to your .m2/repository location.

NetBeans Projects
-----------------

NetBeans has builtin support of maven projects. Just "Open Project..."
and everything is setup automatically. Verified with NetBeans 6.9.1.


Custom Hadoop Dependencies
--------------------------

By default Hadoop dependencies are specified in the top-level pom.xml
properties section. One can override them via -Dhadoop-common.version=...
on the command line. ~/.m2/settings.xml can also be used to specify
these properties in different profiles, which is useful for IDEs.

Modules
-------
YARN consists of multiple modules. The modules are listed below as per the directory structure:

hadoop-yarn-api - YARN's cross platform external interface

hadoop-yarn-common - Utilities which can be used by yarn clients and server

hadoop-yarn-server - Implementation of the hadoop-yarn-api
	hadoop-yarn-server-common - APIs shared between resourcemanager and nodemanager
	hadoop-yarn-server-nodemanager (TaskTracker replacement)
	hadoop-yarn-server-resourcemanager (JobTracker replacement)

Utilities for understanding the code
------------------------------------
Almost all of the yarn components as well as the mapreduce framework use
state-machines for all the data objects. To understand those central pieces of
the code, a visual representation of the state-machines helps much. You can first
convert the state-machines into graphviz(.gv) format by
running:
   mvn compile -Pvisualize
Then you can use the dot program for generating directed graphs and convert the above
.gv files to images. The graphviz package has the needed dot program and related
utilites.For e.g., to generate png files you can run:
   dot -Tpng NodeManager.gv > NodeManager.png
