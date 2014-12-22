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

package org.apache.hadoop.yarn.api.impl.pb.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.ClientSCMProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReleaseSharedCacheResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReleaseSharedCacheResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UseSharedCacheResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.UseSharedCacheResourceResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ReleaseSharedCacheResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UseSharedCacheResourceRequestProto;

import com.google.protobuf.ServiceException;

public class ClientSCMProtocolPBClientImpl implements ClientSCMProtocol,
    Closeable {

  private ClientSCMProtocolPB proxy;

  public ClientSCMProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, ClientSCMProtocolPB.class,
      ProtobufRpcEngine.class);
    proxy = RPC.getProxy(ClientSCMProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
      this.proxy = null;
    }
  }

  @Override
  public UseSharedCacheResourceResponse use(
      UseSharedCacheResourceRequest request) throws YarnException, IOException {
    UseSharedCacheResourceRequestProto requestProto =
        ((UseSharedCacheResourceRequestPBImpl) request).getProto();
    try {
      return new UseSharedCacheResourceResponsePBImpl(proxy.use(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public ReleaseSharedCacheResourceResponse release(
      ReleaseSharedCacheResourceRequest request) throws YarnException,
      IOException {
    ReleaseSharedCacheResourceRequestProto requestProto =
        ((ReleaseSharedCacheResourceRequestPBImpl) request).getProto();
    try {
      return new ReleaseSharedCacheResourceResponsePBImpl(proxy.release(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
