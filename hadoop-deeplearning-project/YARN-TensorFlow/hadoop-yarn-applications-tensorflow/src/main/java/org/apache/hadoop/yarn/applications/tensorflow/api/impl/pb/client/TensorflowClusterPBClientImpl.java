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
package org.apache.hadoop.yarn.applications.tensorflow.api.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnTensorflowClusterProtos.GetClusterSpecRequestProto;
import org.apache.hadoop.yarn.applications.tensorflow.api.TensorflowCluster;
import org.apache.hadoop.yarn.applications.tensorflow.api.TensorflowClusterPB;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecRequest;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecResponse;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.impl.pb.GetClusterSpecRequestPBImpl;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.impl.pb.GetClusterSpecResponsePBImpl;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class TensorflowClusterPBClientImpl implements TensorflowCluster, Closeable {
  private TensorflowClusterPB proxy;

  public TensorflowClusterPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, TensorflowClusterPB.class, ProtobufRpcEngine.class);
    proxy = (TensorflowClusterPB)RPC.getProxy(
      TensorflowClusterPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if(this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public GetClusterSpecResponse getClusterSpec(GetClusterSpecRequest request) throws YarnException, IOException {
    GetClusterSpecRequestProto requestProto = ((GetClusterSpecRequestPBImpl)request).getProto();
    try {
      return new GetClusterSpecResponsePBImpl(proxy.getClusterSpec(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
