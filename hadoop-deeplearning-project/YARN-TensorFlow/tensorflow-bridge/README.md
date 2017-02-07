

## How to build native library
To build the native library, we assume that user already have Tensorflow installed on their servers and eventuall we'll build an independent .so file.

1. Down load this project to folder ${HDL_HOME}

2. Get protobuf 3.1
   
   `wget https://github.com/google/protobuf/archive/v3.1.0.tar.gz`
   
3. Build protobuf 3.1

   Unzip the protobuf source code and build out it's native library (No need to install)
   
   ```
   ./autogen.sh
   ./configure "CFLAGS=-fPIC" "CXXFLAGS=-fPIC"
   make
   ```
   
4. Enter into folder ${HDL_HOME}

4. Build `libridge.so`

   ```
   g++ -std=c++11 -o libbridge.so  {TENSORFLOW_HOME}/python/_pywrap_tensorflow.so -shared -O3 -mavx -fPIC 
   -I{JDK_HOME}/include -I{JDK_HOME}/include/linux/ -I{TENSORFLOW_HOME}/include/ -I/usr/lib64 -I./ -lpython2.7
   -Wl,--whole-archive ../{PROTOBUF3.1_HOME}/src/.libs/libprotobuf-lite.a -Wl,--no-whole-archive {HDL_HOME}/hadoop-deeplearning-project/tensorflow-bridge/src/main/native/org_tensorflow_bridge_TFServer.cpp
   {HDL_HOME}/hadoop-deeplearning-project/tensorflow-bridge/src/main/native/exception_jni.cc 
   ```
   
   Please note to build out libbridge.so correctly, you need to replace JDK_HOME with your own path. TENSORFLOW_HOME means tensorflow's installed folder which may be various.
   Here is an example path it may be: `/usr/lib/python2.7/site-packages/tensorflow`. Also, please make sure the python native library is also sepcified when building.
   
## How to build java library.
**Please note that hadoop requires protoc 2.5 while tensorflow-bridge needs protoc 3.1 which means you need to build this java library using a different environment. However, this java library has already been pulished out and the tenrflow on yarn project depends on that published artifact. So you don't need to compile this project and we'll fix this part in the future.**
 
Here are the main java API it exposed:
 
 ```
 package org.tensorflow.bridge;
 
 public class TFServer {
  public static ServerDef makeServerDef(ServerDef serverDef, String jobName,
    int taskIndex, String proto, ConfigProto config)

  public static ServerDef makeServerDef(ClusterSpec clusterSpec, String jobName,
    int taskIndex, String proto, ConfigProto config)

  public TFServer(ClusterSpec clusterSpec, String jobName, int taskIndex,
                  String proto, ConfigProto config) throws TFServerException

  public TFServer(Map<String,List<String>> clusterSpec, String jobName, int taskIndex)
    throws TFServerException

  public void start()

  public void join()

  public void stop()

  public String getTarget()

  public static TFServer createLocalServer()
}

 ```
