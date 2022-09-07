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

import java.io.IOException;
import org.apache.hadoop.hdfs.NamespaceStateId;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;


public class PoolAlignmentContext implements AlignmentContext {
  private NamespaceStateId sharedGlobalStateId;
  private NamespaceStateId poolLocalStateId;

  PoolAlignmentContext(NamespaceStateId namespaceStateId) {
    sharedGlobalStateId = namespaceStateId;
    poolLocalStateId = new NamespaceStateId();
  }

  /**
   * Client side implementation only receives state alignment info.
   * It does not provide state alignment info therefore this does nothing.
   */
  @Override
  public void updateResponseState(RpcHeaderProtos.RpcResponseHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * Router update globally shared namespaceStateId value using response from
   * namenodes.
   */
  @Override
  public void receiveResponseState(RpcHeaderProtos.RpcResponseHeaderProto header) {
    sharedGlobalStateId.update(header.getStateId());
  }

  /**
   * Client side implementation for routers to provide state info in requests to
   * namenodes.
   */
  @Override
  public void updateRequestState(RpcHeaderProtos.RpcRequestHeaderProto.Builder header) {
    long maxStateId = Long.max(poolLocalStateId.get(), sharedGlobalStateId.get());
    header.setStateId(maxStateId);
  }

  /**
   * Client side implementation only provides state alignment info in requests.
   * Client does not receive RPC requests therefore this does nothing.
   */
  @Override
  public long receiveRequestState(RpcHeaderProtos.RpcRequestHeaderProto header, long threshold)
      throws IOException {
    // Do nothing.
    return 0;
  }

  @Override
  public long getLastSeenStateId() {
    return sharedGlobalStateId.get();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String method) {
    throw new UnsupportedOperationException(
        "Client should not be checking uncoordinated call");
  }

  public void advanceClientStateId(Long clientStateId) {
    poolLocalStateId.update(clientStateId);
  }
}
