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

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * This is the server side implementation responsible for passing
 * state alignment info to clients.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class GlobalStateIdContext implements AlignmentContext {
  /**
   * Estimated number of journal transactions a typical NameNode can execute
   * per second. The number is used to estimate how long a client's
   * RPC request will wait in the call queue before the Observer catches up
   * with its state id.
   */
  private static final long ESTIMATED_TRANSACTIONS_PER_SECOND = 10000L;

  /**
   * The client wait time on an RPC request is composed of
   * the server execution time plus the communication time.
   * This is an expected fraction of the total wait time spent on
   * server execution.
   */
  private static final float ESTIMATED_SERVER_TIME_MULTIPLIER = 0.8f;

  private final FSNamesystem namesystem;
  private final HashSet<String> coordinatedMethods;

  /**
   * Server side constructor.
   * @param namesystem server side state provider
   */
  GlobalStateIdContext(FSNamesystem namesystem) {
    this.namesystem = namesystem;
    this.coordinatedMethods = new HashSet<>();
    // For now, only ClientProtocol methods can be coordinated, so only checking
    // against ClientProtocol.
    for (Method method : ClientProtocol.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ReadOnly.class) &&
          method.getAnnotationsByType(ReadOnly.class)[0].isCoordinated()) {
        coordinatedMethods.add(method.getName());
      }
    }
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
   * Server-side implementation for processing state alignment info in
   * requests.
   * For Observer it compares the client and the server states and determines
   * if it makes sense to wait until the server catches up with the client
   * state. If not the server throws RetriableException so that the client
   * could retry the call according to the retry policy with another Observer
   * or the Active NameNode.
   *
   * @param header The RPC request header.
   * @param clientWaitTime time in milliseconds indicating how long client
   *    waits for the server response. It is used to verify if the client's
   *    state is too far ahead of the server's
   * @return the minimum of the state ids of the client or the server.
   * @throws RetriableException if Observer is too far behind.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header,
      long clientWaitTime) throws RetriableException {
    long serverStateId =
        namesystem.getFSImage().getCorrectLastAppliedOrWrittenTxId();
    long clientStateId = header.getStateId();
    if (clientStateId > serverStateId &&
        HAServiceState.ACTIVE.equals(namesystem.getState())) {
      FSNamesystem.LOG.warn("A client sent stateId: " + clientStateId +
          ", but server state is: " + serverStateId);
      return serverStateId;
    }
    if (HAServiceState.OBSERVER.equals(namesystem.getState()) &&
        clientStateId - serverStateId >
        ESTIMATED_TRANSACTIONS_PER_SECOND
            * TimeUnit.MILLISECONDS.toSeconds(clientWaitTime)
            * ESTIMATED_SERVER_TIME_MULTIPLIER) {
      throw new RetriableException(
          "Observer Node is too far behind: serverStateId = "
              + serverStateId + " clientStateId = " + clientStateId);
    }
    return clientStateId;
  }

  @Override
  public long getLastSeenStateId() {
    return namesystem.getFSImage().getCorrectLastAppliedOrWrittenTxId();
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String methodName) {
    return protocolName.equals(ClientProtocol.class.getCanonicalName())
        && coordinatedMethods.contains(methodName);
  }
}
