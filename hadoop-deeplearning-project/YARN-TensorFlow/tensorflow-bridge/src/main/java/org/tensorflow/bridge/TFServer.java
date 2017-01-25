/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tensorflow.bridge;

import org.tensorflow.distruntime.ServerDef;
import org.tensorflow.framework.ConfigProto;
import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TFServer {
  public ServerDef serverDef;
  private long nativeServer;

  static {
    System.loadLibrary("bridge"); // Load native library at runtime
  }

  public static ServerDef makeServerDef(ServerDef serverDef, String jobName,
    int taskIndex, String proto, ConfigProto config) {
    return ServerDef.newBuilder().mergeFrom(serverDef).setJobName(jobName)
      .setTaskIndex(taskIndex).setProtocol(proto).setDefaultSessionConfig(config).build();
  }

  public static ServerDef makeServerDef(ClusterSpec clusterSpec, String jobName,
    int taskIndex, String proto, ConfigProto config) {
    return ServerDef.newBuilder().setCluster(clusterSpec.as_cluster_def())
      .setJobName(jobName).setProtocol(proto).setTaskIndex(taskIndex)
      .setDefaultSessionConfig(config).build();
  }

  public TFServer(ClusterSpec clusterSpec, String jobName, int taskIndex,
                  String proto, ConfigProto config) throws TFServerException {
    this.serverDef = makeServerDef(clusterSpec, jobName, taskIndex, proto, config);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      serverDef.writeTo(baos);
      byte[] bytes = baos.toByteArray();
      baos.close();
      this.nativeServer = createServer(bytes);
    } catch (TFServerException e) {
      throw e;
    } catch (IOException e) {
      //
    }
  }

  public TFServer(Map<String,List<String>> clusterSpec, String jobName, int taskIndex)
    throws TFServerException {
    this(new ClusterSpec(clusterSpec), jobName, taskIndex,
      "grpc", ConfigProto.getDefaultInstance());
  }

  public void start() {
    this.startServer(this.nativeServer);
  }

  public void join() {
    this.join(this.nativeServer);
  }

  public void stop() {
    this.stop(this.nativeServer);
  }

  public String getTarget() {
    return target(this.nativeServer);
  }

  public static TFServer createLocalServer() {
    HashMap<String,List<String>> cluster = new HashMap<String,List<String>>();
    List<String> address_list = new ArrayList<String>();
    address_list.add("localhost:0");
    cluster.put("worker",address_list);
    ClusterSpec cluster_spec = new ClusterSpec(cluster);
    return new TFServer(cluster_spec, "worker", 0, "grpc", ConfigProto.getDefaultInstance());
  }

  private native long createServer(byte[] server_def);

  private native void startServer(long server);

  private native void join(long server);

  private native void stop(long server);

  private native String target(long server);
}
