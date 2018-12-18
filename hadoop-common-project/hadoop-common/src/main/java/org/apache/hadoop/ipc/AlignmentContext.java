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

package org.apache.hadoop.ipc;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * This interface intends to align the state between client and server
 * via RPC communication.
 *
 * This should be implemented separately on the client side and server side
 * and can be used to pass state information on RPC responses from server
 * to client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface AlignmentContext {

  /**
   * This is the intended server method call to implement to pass state info
   * during RPC response header construction.
   *
   * @param header The RPC response header builder.
   */
  void updateResponseState(RpcResponseHeaderProto.Builder header);

  /**
   * This is the intended client method call to implement to recieve state info
   * during RPC response processing.
   *
   * @param header The RPC response header.
   */
  void receiveResponseState(RpcResponseHeaderProto header);

  /**
   * This is the intended client method call to pull last seen state info
   * into RPC request processing.
   *
   * @param header The RPC request header builder.
   */
  void updateRequestState(RpcRequestHeaderProto.Builder header);

  /**
   * This is the intended server method call to implement to receive
   * client state info during RPC response header processing.
   *
   * @param header The RPC request header.
   * @param threshold a parameter to verify a condition when server
   *        should reject client request due to its state being too far
   *        misaligned with the client state.
   *        See implementation for more details.
   * @return state id required for the server to execute the call.
   * @throws IOException
   */
  long receiveRequestState(RpcRequestHeaderProto header, long threshold)
      throws IOException;

  /**
   * Returns the last seen state id of the alignment context instance.
   *
   * @return the value of the last seen state id.
   */
  long getLastSeenStateId();

  /**
   * Return true if this method call does need to be synced, false
   * otherwise. sync meaning server state needs to have caught up with
   * client state.
   *
   * @param protocolName the name of the protocol
   * @param method the method call to check
   * @return true if this method is async, false otherwise.
   */
  boolean isCoordinatedCall(String protocolName, String method);
}
