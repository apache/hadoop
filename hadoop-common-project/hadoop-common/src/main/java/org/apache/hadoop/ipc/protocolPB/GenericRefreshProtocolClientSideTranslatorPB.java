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

package org.apache.hadoop.ipc.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.GenericRefreshProtocol;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshRequestProto;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GenericRefreshProtocolClientSideTranslatorPB implements
    ProtocolMetaInterface, GenericRefreshProtocol, Closeable {

  /** RpcController is not used and hence is set to null. */
  private final static RpcController NULL_CONTROLLER = null;
  private final GenericRefreshProtocolPB rpcProxy;

  public GenericRefreshProtocolClientSideTranslatorPB(
      GenericRefreshProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Collection<RefreshResponse> refresh(String identifier, String[] args) throws IOException {
    List<String> argList = Arrays.asList(args);

    try {
      GenericRefreshRequestProto request = GenericRefreshRequestProto.newBuilder()
        .setIdentifier(identifier)
        .addAllArgs(argList)
        .build();

      GenericRefreshResponseCollectionProto resp = rpcProxy.refresh(NULL_CONTROLLER, request);
      return unpack(resp);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  private Collection<RefreshResponse> unpack(GenericRefreshResponseCollectionProto collection) {
    List<GenericRefreshResponseProto> responseProtos = collection.getResponsesList();
    List<RefreshResponse> responses = new ArrayList<RefreshResponse>();

    for (GenericRefreshResponseProto rp : responseProtos) {
      RefreshResponse response = unpack(rp);
      responses.add(response);
    }

    return responses;
  }

  private RefreshResponse unpack(GenericRefreshResponseProto proto) {
    // The default values
    String message = null;
    String sender = null;
    int returnCode = -1;

    // ... that can be overridden by data from the protobuf
    if (proto.hasUserMessage()) {
      message = proto.getUserMessage();
    }
    if (proto.hasExitStatus()) {
      returnCode = proto.getExitStatus();
    }
    if (proto.hasSenderName()) {
      sender = proto.getSenderName();
    }

    // ... and put into a RefreshResponse
    RefreshResponse response = new RefreshResponse(returnCode, message);
    response.setSenderName(sender);

    return response;
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
      GenericRefreshProtocolPB.class,
      RPC.RpcKind.RPC_PROTOCOL_BUFFER,
      RPC.getProtocolVersion(GenericRefreshProtocolPB.class),
      methodName);
  }
}
