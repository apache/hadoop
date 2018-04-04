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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * This is the client side implementation responsible for receiving
 * state alignment info from server(s).
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
class ClientGCIContext implements AlignmentContext {

  private final AtomicLong lastSeenStateId = new AtomicLong(Long.MIN_VALUE);

  long getLastSeenStateId() {
    return lastSeenStateId.get();
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
   * Client side implementation for receiving state alignment info in responses.
   */
  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    updateMax(header.getStateId());
  }

  /**
   * Client side implementation for providing state alignment info in requests.
   */
  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    header.setStateId(lastSeenStateId.longValue());
  }

  /**
   * Client side implementation only provides state alignment info in requests.
   * Client does not receive RPC requests therefore this does nothing.
   */
  @Override
  public void receiveRequestState(RpcRequestHeaderProto header) {
    // Do nothing.
  }

  private void updateMax(long sample) {
    while (true) {
      long curMax = lastSeenStateId.get();
      if (curMax >= sample) {
        break;
      }

      boolean setSuccessful =
          lastSeenStateId.compareAndSet(curMax, sample);

      if (setSuccessful) {
        break;
      }
    }
  }
}
