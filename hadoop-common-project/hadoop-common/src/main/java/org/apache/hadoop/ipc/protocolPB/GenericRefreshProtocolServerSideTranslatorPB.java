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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.ipc.GenericRefreshProtocol;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshRequestProto;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseProto;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshResponseCollectionProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GenericRefreshProtocolServerSideTranslatorPB implements
    GenericRefreshProtocolPB {

  private final GenericRefreshProtocol impl;

  public GenericRefreshProtocolServerSideTranslatorPB(
      GenericRefreshProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GenericRefreshResponseCollectionProto refresh(
      RpcController controller, GenericRefreshRequestProto request)
      throws ServiceException {
    try {
      List<String> argList = request.getArgsList();
      String[] args = argList.toArray(new String[argList.size()]);

      if (!request.hasIdentifier()) {
        throw new ServiceException("Request must contain identifier");
      }

      Collection<RefreshResponse> results = impl.refresh(request.getIdentifier(), args);

      return pack(results);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  // Convert a collection of RefreshResponse objects to a
  // RefreshResponseCollection proto
  private GenericRefreshResponseCollectionProto pack(
    Collection<RefreshResponse> responses) {
    GenericRefreshResponseCollectionProto.Builder b =
      GenericRefreshResponseCollectionProto.newBuilder();

    for (RefreshResponse response : responses) {
      GenericRefreshResponseProto.Builder respBuilder =
        GenericRefreshResponseProto.newBuilder();
      respBuilder.setExitStatus(response.getReturnCode());
      respBuilder.setUserMessage(response.getMessage());
      respBuilder.setSenderName(response.getSenderName());

      // Add to collection
      b.addResponses(respBuilder);
    }

    return b.build();
  }
}
