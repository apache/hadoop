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

import java.lang.reflect.Method;
import java.util.HashSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;

/**
 * This is the router implementation responsible for passing
 * client state id to next level.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class RouterStateIdContext implements AlignmentContext {

  private final HashSet<String> coordinatedMethods;

  RouterStateIdContext() {
    this.coordinatedMethods = new HashSet<>();
    // For now, only ClientProtocol methods can be coordinated, so only checking
    // against ClientProtocol.
    for (Method method : ClientProtocol.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ReadOnly.class)
          && method.getAnnotationsByType(ReadOnly.class)[0].isCoordinated()) {
        coordinatedMethods.add(method.getName());
      }
    }
  }

  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
 // Do nothing.
  }

  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Do nothing.
  }

  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Do nothing.
  }

  @Override
  public long receiveRequestState(RpcRequestHeaderProto header,
      long clientWaitTime) throws RetriableException {
    long clientStateId = header.getStateId();
    return clientStateId;
  }

  @Override
  public long getLastSeenStateId() {
    return 0;
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String methodName) {
    return protocolName.equals(ClientProtocol.class.getCanonicalName())
        && coordinatedMethods.contains(methodName);
  }
}
