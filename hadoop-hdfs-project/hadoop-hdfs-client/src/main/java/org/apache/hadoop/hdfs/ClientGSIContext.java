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
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.NameServiceStateIdMode;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * Global State Id context for the client.
 * <p>
 * This is the client side implementation responsible for receiving
 * state alignment info from server(s).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ClientGSIContext implements AlignmentContext {

  public final static String DEFAULT_NS = "";
  private final FederatedNamespaceIds federatedNamespaceIds;
  private final String nsId;

  public ClientGSIContext(NameServiceStateIdMode mode) {
    this(mode, DEFAULT_NS);
  }

  public ClientGSIContext(NameServiceStateIdMode mode, String nsId) {
    this.federatedNamespaceIds = new FederatedNamespaceIds(mode);
    this.nsId = nsId;
  }


  @Override
  public long getLastSeenStateId() {
    return federatedNamespaceIds.getNamespaceId(nsId, true).get();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
    throw new UnsupportedOperationException("Client should not be checking uncoordinated call");
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
  public void receiveResponseState(RpcResponseHeaderProto header) {
    if (federatedNamespaceIds.isDisable()) {
      federatedNamespaceIds.updateNameserviceState(this.nsId, header.getStateId());
    } else {
      federatedNamespaceIds.updateStateUsingResponseHeader(header);
    }
  }

  /**
   * Client side implementation for providing state alignment info in requests.
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    if (federatedNamespaceIds.isDisable()) {
      header.setStateId(federatedNamespaceIds.getNamespaceId(this.nsId, true).get());
      header.clearNameserviceStateIdsContext();
    } else {
      federatedNamespaceIds.setRequestHeaderState(header);
    }
  }

  /**
   * Client side implementation only provides state alignment info in requests.
   * Client does not receive RPC requests therefore this does nothing.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header, long threshold,
      boolean isCoordinatedCall) throws IOException {
    // Do nothing.
    return 0;
  }
}
