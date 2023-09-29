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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RouterFederatedStateProto;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.util.ProtoUtil;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestRouterFederatedState {

  @Test
  public void testRpcRouterFederatedState() throws InvalidProtocolBufferException {
    byte[] uuid = ClientId.getClientId();
    Map<String, Long> expectedStateIds = new HashMap<String, Long>() {
      {
        put("namespace1", 11L);
        put("namespace2", 22L);
      }
    };

    AlignmentContext alignmentContext = new AlignmentContextWithRouterState(expectedStateIds);

    RpcHeaderProtos.RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
        0,
        RpcConstants.INVALID_RETRY_COUNT,
        uuid,
        alignmentContext);

    Map<String, Long> stateIdsFromHeader =
        RouterFederatedStateProto.parseFrom(
            header.getRouterFederatedState().toByteArray()
        ).getNamespaceStateIdsMap();

    assertEquals(expectedStateIds, stateIdsFromHeader);
  }

  private static class AlignmentContextWithRouterState implements AlignmentContext {

    private Map<String, Long> routerFederatedState;

    AlignmentContextWithRouterState(Map<String, Long> namespaceStates) {
      this.routerFederatedState = namespaceStates;
    }

    @Override
    public void updateRequestState(RpcHeaderProtos.RpcRequestHeaderProto.Builder header) {
      RouterFederatedStateProto fedState = RouterFederatedStateProto
          .newBuilder()
          .putAllNamespaceStateIds(routerFederatedState)
          .build();

      header.setRouterFederatedState(fedState.toByteString());
    }

    @Override
    public void updateResponseState(RpcHeaderProtos.RpcResponseHeaderProto.Builder header) {}

    @Override
    public void receiveResponseState(RpcHeaderProtos.RpcResponseHeaderProto header) {}

    @Override
    public long receiveRequestState(RpcHeaderProtos.RpcRequestHeaderProto header, long threshold) {
      return 0;
    }

    @Override
    public long getLastSeenStateId() {
      return 0;
    }

    @Override
    public boolean isCoordinatedCall(String protocolName, String method) {
      return false;
    }
  }
}