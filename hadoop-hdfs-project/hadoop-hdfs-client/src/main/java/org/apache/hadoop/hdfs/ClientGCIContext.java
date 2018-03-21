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
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

import java.util.concurrent.atomic.LongAccumulator;

/**
 * This is the client side implementation responsible for receiving
 * state alignment info from server(s).
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
class ClientGCIContext implements AlignmentContext {

  private final DFSClient dfsClient;
  private final LongAccumulator lastSeenStateId =
      new LongAccumulator(Math::max, Long.MIN_VALUE);

  /**
   * Client side constructor.
   * @param dfsClient client side state receiver
   */
  ClientGCIContext(DFSClient dfsClient) {
    this.dfsClient = dfsClient;
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
   * Client side implementation for receiving state alignment info.
   */
  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    lastSeenStateId.accumulate(header.getStateId());
    dfsClient.lastSeenStateId = lastSeenStateId.get();
  }
}
