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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.FederatedNamespaceIds;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.hdfs.server.federation.router.RouterStateIdCache.UniqueCallID;

import java.io.IOException;

/*
 * Only router's state id is CLIENT mode, this class will be constructed.
 * */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class RouterClientSideStateIdContext implements AlignmentContext {

  RouterClientSideStateIdContext() {
  }

  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Fill header with federatedNamespaceIds updated by namenode. Then send back to client.
    UniqueCallID uid = new UniqueCallID(header.getClientId().toByteArray(), header.getCallId());
    FederatedNamespaceIds ids = RouterStateIdCache.get(uid);
    if (ids != null) {
      ids.setResponseHeaderState(header);
      RouterStateIdCache.remove(uid);
    }
  }

  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    throw new UnsupportedOperationException("Router server should not receive response state");
  }

  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    throw new UnsupportedOperationException("Router server should not update request state");
  }

  @Override
  public long receiveRequestState(RpcRequestHeaderProto header,long clientWaitTime,
                                  boolean isCoordinatedCall) throws IOException {
    // Receive request from client, cache the federatedNamespaceIds
    UniqueCallID uid = new UniqueCallID(header.getClientId().toByteArray(), header.getCallId());
    if (header.hasNameserviceStateIdsContext()) {
      // Only cache FederatedNamespaceIds which mode is PROXY or TRANSMISSION
      FederatedNamespaceIds ids = new FederatedNamespaceIds(RpcClientUtil.toStateIdMode(header));
      ids.updateStateUsingRequestHeader(header);
      RouterStateIdCache.put(uid, ids);
    } else {
      RouterStateIdCache.remove(uid);
    }
    return 0;
  }

  @Override
  public long getLastSeenStateId() {
    // In Router, getLastSeenStateId always larger than receiveRequestState.
    return Long.MAX_VALUE;
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String methodName) {
    return true;
  }
}
