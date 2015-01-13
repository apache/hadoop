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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderCanUploadRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SCMUploaderNotifyRequestProto;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderCanUploadRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderCanUploadResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderNotifyRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.SCMUploaderNotifyResponsePBImpl;

import com.google.protobuf.ServiceException;

public class SCMUploaderProtocolPBClientImpl implements
    SCMUploaderProtocol, Closeable {

  private SCMUploaderProtocolPB proxy;

  public SCMUploaderProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, SCMUploaderProtocolPB.class,
      ProtobufRpcEngine.class);
    proxy =
        RPC.getProxy(SCMUploaderProtocolPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
      this.proxy = null;
    }
  }

  @Override
  public SCMUploaderNotifyResponse notify(SCMUploaderNotifyRequest request)
      throws YarnException, IOException {
    SCMUploaderNotifyRequestProto requestProto =
        ((SCMUploaderNotifyRequestPBImpl) request).getProto();
    try {
      return new SCMUploaderNotifyResponsePBImpl(proxy.notify(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }

  @Override
  public SCMUploaderCanUploadResponse canUpload(
      SCMUploaderCanUploadRequest request) throws YarnException, IOException {
    SCMUploaderCanUploadRequestProto requestProto =
        ((SCMUploaderCanUploadRequestPBImpl)request).getProto();
    try {
      return new SCMUploaderCanUploadResponsePBImpl(proxy.canUpload(null,
          requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
