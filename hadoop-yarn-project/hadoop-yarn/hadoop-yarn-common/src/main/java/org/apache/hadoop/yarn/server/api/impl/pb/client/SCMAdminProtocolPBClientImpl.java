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

package org.apache.hadoop.yarn.server.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocol;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RunSharedCacheCleanerTaskRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RunSharedCacheCleanerTaskResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

import org.apache.hadoop.thirdparty.protobuf.ServiceException;

public class SCMAdminProtocolPBClientImpl implements SCMAdminProtocol,
    Closeable {

  private SCMAdminProtocolPB proxy;

  public SCMAdminProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, SCMAdminProtocolPB.class,
        ProtobufRpcEngine2.class);
    proxy = RPC.getProxy(SCMAdminProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public RunSharedCacheCleanerTaskResponse runCleanerTask(
      RunSharedCacheCleanerTaskRequest request) throws YarnException,
      IOException {
    YarnServiceProtos.RunSharedCacheCleanerTaskRequestProto requestProto =
        ((RunSharedCacheCleanerTaskRequestPBImpl) request).getProto();
    try {
      return new RunSharedCacheCleanerTaskResponsePBImpl(proxy.runCleanerTask(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
