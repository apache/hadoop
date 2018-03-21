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
import org.apache.hadoop.ipc.AlignmentContext;
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
   * Server side implementation for providing state alignment info.
   */
  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    header.setStateId(namesystem.getLastWrittenTransactionId());
  }

  /**
   * Server side implementation only provides state alignment info.
   * It does not receive state alignment info therefore this does nothing.
   */
  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Do nothing.
  }
}
