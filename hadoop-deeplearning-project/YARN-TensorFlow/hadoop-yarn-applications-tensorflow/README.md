TensorFlow on YARN
======================
TensorFlow on YARN is a YARN application to enable an easy way for end user to run TensorFlow scripts.

Note that current project is a prototype with limitation and is still under development

## Features
- [x] Launch a TensorFlow cluster with specified number of worker and PS server
- [x] Replace python layer with java bridge layer to start server
- [x] Generate ClusterSpec dynamically
- [x] RPC support for client to get ClusterSpec from AM
- [x] Signal handling for graceful shutdown
- [ ] Package TensorFlow runtime as a resource that can be distributed easily
- [ ] Fault tolerance
- [ ] Code refine and more tests

## Set up and run
1. Git clone ..
2. Compile [tensorflow-bridge](../tensorflow-bridge/README.md) and put libbridge.so to a place be aware to YARN application. For instance, JVM lib directory.
3. Compile TensorFlow on YARN

   ```sh
   cd <path_to_hadoop-yarn-application-tensorflow>
   mvn clean package -DskipTests
   ```
4. Run your Tensorflow script. Let's assume a "job.py"

   ```sh
   ./bin/yarn-tf -job job.py -numberworkers 4 -numberps 1 -jar <path_to_tensorflow-on-yarn-with-dependency_jar>
   ```

   Note that at present, the "job.py" should parse worker and PS server from parameters "ps" and "wk" populated by TensorFlow on YARN client in the form of comma seperated values.
