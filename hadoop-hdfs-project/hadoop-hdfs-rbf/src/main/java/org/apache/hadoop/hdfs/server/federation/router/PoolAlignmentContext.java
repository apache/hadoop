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
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;


/**
 * An alignment context shared by all connections in a {@link ConnectionPool}.
 * There is a distinct connection pool for each [namespace,UGI] pairing.
 * <p>
 * {@link #sharedGlobalStateId} is a reference to a
 * shared {@link LongAccumulator} object in the {@link RouterStateIdContext}.
 * {@link #poolLocalStateId} is specific to each PoolAlignmentContext.
 * <p>
 * The shared {@link #sharedGlobalStateId} is updated only using
 * responses from NameNodes, so clients cannot poison it.
 * {@link #poolLocalStateId} is used to propagate client observed
 * state into NameNode requests. A misbehaving client can poison this but the effect is only
 * visible to other clients with the same UGI and accessing the same namespace.
 */
public class PoolAlignmentContext implements AlignmentContext {
  private LongAccumulator sharedGlobalStateId;
  private LongAccumulator poolLocalStateId;

  PoolAlignmentContext(RouterStateIdContext routerStateIdContext, String namespaceId) {
    sharedGlobalStateId = routerStateIdContext.getNamespaceStateId(namespaceId);
    poolLocalStateId = new LongAccumulator(Math::max, Long.MIN_VALUE);
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
   * Router updates a globally shared value using response from
   * namenodes.
   */
  @Override
  public void receiveResponseState(RpcHeaderProtos.RpcResponseHeaderProto header) {
    sharedGlobalStateId.accumulate(header.getStateId());
  }

  /**
   * Client side implementation for routers to provide state info in requests to
   * namenodes.
   */
  @Override
  public void updateRequestState(RpcHeaderProtos.RpcRequestHeaderProto.Builder header) {
    header.setStateId(poolLocalStateId.get());
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
    poolLocalStateId.accumulate(clientStateId);
  }

  @VisibleForTesting
  public long getPoolLocalStateId() {
    return this.poolLocalStateId.get();
  }
}
