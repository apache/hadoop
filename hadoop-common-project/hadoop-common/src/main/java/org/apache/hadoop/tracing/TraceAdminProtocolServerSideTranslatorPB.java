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
package org.apache.hadoop.tracing;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverRequestProto;
import org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverResponseProto;
import org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversRequestProto;
import org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto;
import org.apache.hadoop.tracing.TraceAdminPB.ConfigPair;
import org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverRequestProto;
import org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverResponseProto;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
public class TraceAdminProtocolServerSideTranslatorPB
    implements TraceAdminProtocolPB, Closeable  {
  private final TraceAdminProtocol server;

  public TraceAdminProtocolServerSideTranslatorPB(TraceAdminProtocol server) {
    this.server = server;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(server);
  }

  @Override
  public ListSpanReceiversResponseProto listSpanReceivers(
      RpcController controller, ListSpanReceiversRequestProto req)
          throws ServiceException {
    try {
      SpanReceiverInfo[] descs = server.listSpanReceivers();
      ListSpanReceiversResponseProto.Builder bld =
          ListSpanReceiversResponseProto.newBuilder();
      for (int i = 0; i < descs.length; ++i) {
        bld.addDescriptions(TraceAdminPB.SpanReceiverListInfo.newBuilder().
              setId(descs[i].getId()).
              setClassName(descs[i].getClassName()).build());
      }
      return bld.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AddSpanReceiverResponseProto addSpanReceiver(
      RpcController controller, AddSpanReceiverRequestProto req)
          throws ServiceException {
    try {
      SpanReceiverInfoBuilder factory =
        new SpanReceiverInfoBuilder(req.getClassName());
      for (ConfigPair config : req.getConfigList()) {
        factory.addConfigurationPair(config.getKey(), config.getValue());
      }
      long id = server.addSpanReceiver(factory.build());
      return AddSpanReceiverResponseProto.newBuilder().setId(id).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveSpanReceiverResponseProto removeSpanReceiver(
      RpcController controller,  RemoveSpanReceiverRequestProto req)
          throws ServiceException {
    try {
      server.removeSpanReceiver(req.getId());
      return RemoveSpanReceiverResponseProto.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return TraceAdminProtocol.versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
          long clientVersion, int clientMethodsHash) throws IOException {
    if (!protocol.equals(RPC.getProtocolName(TraceAdminProtocolPB.class))) {
      throw new IOException("Serverside implements " +
          RPC.getProtocolName(TraceAdminProtocolPB.class) +
          ". The following requested protocol is unknown: " + protocol);
    }
    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(TraceAdminProtocolPB.class),
        TraceAdminProtocolPB.class);
  }
}
