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
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

import java.io.IOException;
import java.util.concurrent.atomic.LongAccumulator;

import static org.apache.hadoop.ipc.RpcConstants.DISABLED_OBSERVER_READ_STATEID;

/**
 * Global State Id context for the client.
 * <p>
 * This is the client side implementation responsible for receiving
 * state alignment info from server(s).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ClientGSIContext implements AlignmentContext {

  private static Long STATEID_DEFAULT_VALUE = Long.MIN_VALUE;
  private final LongAccumulator lastSeenStateId =
      new LongAccumulator(Math::max, STATEID_DEFAULT_VALUE);
  private FederatedGSIContext federatedGSIContext = new FederatedGSIContext();

  public void disableObserverRead() {
    if (lastSeenStateId.get() > DISABLED_OBSERVER_READ_STATEID) {
      throw new IllegalStateException(
          "Can't disable observer read after communicate.");
    }
    lastSeenStateId.accumulate(DISABLED_OBSERVER_READ_STATEID);
  }

  @Override
  public long getLastSeenStateId() {
    return lastSeenStateId.get();
  }

  public void updateLastSeenStateID(Long stateId) {
    lastSeenStateId.accumulate(stateId);
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
  public void receiveResponseState(RpcResponseHeaderProto header) {
    if (lastSeenStateId.get() == DISABLED_OBSERVER_READ_STATEID) {
      //Observer read is disabled
      return;
    }
    federatedGSIContext.updateStateUsingResponseHeader(header);
    lastSeenStateId.accumulate(header.getStateId());
  }

  /**
   * Client side implementation for providing state alignment info in requests.
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    if (lastSeenStateId.longValue() != STATEID_DEFAULT_VALUE) {
      header.setStateId(lastSeenStateId.longValue());
    }
    federatedGSIContext.setRequestHeaderState(header);
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
}
