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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RouterFederatedStateProto;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;

/**
 * Global State Id context for the client.
 * <p>
 * This is the client side implementation responsible for receiving
 * state alignment info from server(s).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ClientGSIContext implements AlignmentContext {

  private final LongAccumulator lastSeenStateId;
  private ByteString routerFederatedState;

  public ClientGSIContext() {
    this(new LongAccumulator(Math::max, Long.MIN_VALUE));
  }

  public ClientGSIContext(LongAccumulator lastSeenStateId) {
    this.lastSeenStateId = lastSeenStateId;
    routerFederatedState = null;
  }

  @Override
  public long getLastSeenStateId() {
    return lastSeenStateId.get();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
    throw new UnsupportedOperationException(
        "Client should not be checking uncoordinated call");
  }

  /**
   * Client side implementation only receives state alignment info.
   * It does not provide state alignment info therefore this does nothing.
   */
  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * Client side implementation for receiving state alignment info
   * in responses.
   */
  @Override
  public synchronized void receiveResponseState(RpcResponseHeaderProto header) {
    if (header.hasRouterFederatedState()) {
      routerFederatedState = mergeRouterFederatedState(
          this.routerFederatedState, header.getRouterFederatedState());
    } else {
      lastSeenStateId.accumulate(header.getStateId());
    }
  }

  /**
   * Utility function to parse routerFederatedState field in RPC headers.
   */
  public static Map<String, Long> getRouterFederatedStateMap(ByteString byteString) {
    if (byteString != null) {
      try {
        RouterFederatedStateProto federatedState = RouterFederatedStateProto.parseFrom(byteString);
        return federatedState.getNamespaceStateIdsMap();
      } catch (InvalidProtocolBufferException e) {
        // Ignore this exception and will return an empty map
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Merge state1 and state2 to get the max value for each namespace.
   * @param state1 input ByteString.
   * @param state2 input ByteString.
   * @return one ByteString object which contains the max value of each namespace.
   */
  public static ByteString mergeRouterFederatedState(ByteString state1, ByteString state2) {
    Map<String, Long> mapping1 = new HashMap<>(getRouterFederatedStateMap(state1));
    Map<String, Long> mapping2 = getRouterFederatedStateMap(state2);
    mapping2.forEach((k, v) -> {
      long localValue = mapping1.getOrDefault(k, 0L);
      mapping1.put(k, Math.max(v, localValue));
    });
    RouterFederatedStateProto.Builder federatedBuilder = RouterFederatedStateProto.newBuilder();
    mapping1.forEach(federatedBuilder::putNamespaceStateIds);
    return federatedBuilder.build().toByteString();
  }

  /**
   * Client side implementation for providing state alignment info in requests.
   */
  @Override
  public synchronized void updateRequestState(RpcRequestHeaderProto.Builder header) {
    if (lastSeenStateId.get() != Long.MIN_VALUE) {
      header.setStateId(lastSeenStateId.get());
    }
    if (routerFederatedState != null) {
      header.setRouterFederatedState(routerFederatedState);
    }
  }

  /**
   * Client side implementation only provides state alignment info in requests.
   * Client does not receive RPC requests therefore this does nothing.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header, long threshold)
      throws IOException {
    // Do nothing.
    return 0;
  }

  @VisibleForTesting
  public ByteString getRouterFederatedState() {
    return this.routerFederatedState;
  }
}
