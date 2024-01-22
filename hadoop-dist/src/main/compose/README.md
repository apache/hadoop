## Hadoop Docker

This directory contains Docker Compose definitions for running the current version of Hadoop in a multi-node docker environment.

This is meant for testing code changes locally and debugging.

The image used by the Docker setup is built as part of the maven lifecycle.

In order to start the docker environment you need to do the following 
* Build the project, which will also build the docker image
  * ```shell
    > mvn clean install -Dmaven.javadoc.skip=true -DskipTests -DskipShade -Pdist,src
    ```
* From the project root, navigate under the docker-compose dir under the generated dist directory
  * ```shell
    > cd hadoop-dist/target/hadoop-<current-version>/compose/hadoop
    ```
* Start the docker environment
  * ```shell
    > docker-compose up -d --scale datanode=3
    ```
* Connect to a container to execute commands
  * ```shell
    > docker exec -it hadoop_datanode_1 bash
    bash-4.2$ hdfs dfs -mkdir /test
    ```

### Config files

To add or remove properties from the `core-site.xml`, `hdfs-site.xml`, etc. files used in the docker environment,
simply edit the `config` file before starting the containers. The changes will be persisted in the docker environment.
