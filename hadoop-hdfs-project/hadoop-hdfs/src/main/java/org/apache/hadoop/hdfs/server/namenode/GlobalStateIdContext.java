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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * This is the server side implementation responsible for passing
 * state alignment info to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
class GlobalStateIdContext implements AlignmentContext {
  private final FSNamesystem namesystem;

  /**
   * Server side constructor.
   * @param namesystem server side state provider
   */
  GlobalStateIdContext(FSNamesystem namesystem) {
    this.namesystem = namesystem;
  }

  /**
   * Server side implementation for providing state alignment info in responses.
   */
  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    // Using getCorrectLastAppliedOrWrittenTxId will acquire the lock on
    // FSEditLog. This is needed so that ANN will return the correct state id
    // it currently has. But this may not be necessary for Observer, may want
    // revisit for optimization. Same goes to receiveRequestState.
    header.setStateId(getLastSeenStateId());
  }

  /**
   * Server side implementation only provides state alignment info.
   * It does not receive state alignment info therefore this does nothing.
   */
  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Do nothing.
  }

  /**
   * Server side implementation only receives state alignment info.
   * It does not build RPC requests therefore this does nothing.
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * Server side implementation for processing state alignment info in requests.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header) {
    long serverStateId =
        namesystem.getFSImage().getCorrectLastAppliedOrWrittenTxId();
    long clientStateId = header.getStateId();
    if (clientStateId > serverStateId &&
        HAServiceProtocol.HAServiceState.ACTIVE.equals(namesystem.getState())) {
      FSNamesystem.LOG.warn("A client sent stateId: " + clientStateId +
          ", but server state is: " + serverStateId);
    }
    return clientStateId;
  }

  @Override
  public long getLastSeenStateId() {
    return namesystem.getFSImage().getCorrectLastAppliedOrWrittenTxId();
  }
}
