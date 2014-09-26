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
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverRequestProto;
import org.apache.hadoop.tracing.TraceAdminPB.AddSpanReceiverResponseProto;
import org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversRequestProto;
import org.apache.hadoop.tracing.TraceAdminPB.ListSpanReceiversResponseProto;
import org.apache.hadoop.tracing.TraceAdminPB.ConfigPair;
import org.apache.hadoop.tracing.TraceAdminPB.RemoveSpanReceiverRequestProto;
import org.apache.hadoop.tracing.TraceAdminPB.SpanReceiverListInfo;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
public class TraceAdminProtocolTranslatorPB implements
    TraceAdminProtocol, ProtocolTranslator, Closeable  {
  private final TraceAdminProtocolPB rpcProxy;

  public TraceAdminProtocolTranslatorPB(TraceAdminProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public SpanReceiverInfo[] listSpanReceivers() throws IOException {
    ArrayList<SpanReceiverInfo> infos = new ArrayList<SpanReceiverInfo>(1);
    try {
      ListSpanReceiversRequestProto req =
          ListSpanReceiversRequestProto.newBuilder().build();
      ListSpanReceiversResponseProto resp =
          rpcProxy.listSpanReceivers(null, req);
      for (SpanReceiverListInfo info : resp.getDescriptionsList()) {
        infos.add(new SpanReceiverInfo(info.getId(), info.getClassName()));
      }
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
    return infos.toArray(new SpanReceiverInfo[infos.size()]);
  }

  @Override
  public long addSpanReceiver(SpanReceiverInfo info) throws IOException {
    try {
      AddSpanReceiverRequestProto.Builder bld =
          AddSpanReceiverRequestProto.newBuilder();
      bld.setClassName(info.getClassName());
      for (ConfigurationPair configPair : info.configPairs) {
        ConfigPair tuple = ConfigPair.newBuilder().
            setKey(configPair.getKey()).
            setValue(configPair.getValue()).build();
        bld.addConfig(tuple);
      }
      AddSpanReceiverResponseProto resp =
          rpcProxy.addSpanReceiver(null, bld.build());
      return resp.getId();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void removeSpanReceiver(long spanReceiverId) throws IOException {
    try {
      RemoveSpanReceiverRequestProto req =
          RemoveSpanReceiverRequestProto.newBuilder()
            .setId(spanReceiverId).build();
      rpcProxy.removeSpanReceiver(null, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}
