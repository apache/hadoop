/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.states.endpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.hdsl.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Task that returns version.
 */
public class VersionEndpointTask implements
    Callable<EndpointStateMachine.EndPointStates> {
  private final EndpointStateMachine rpcEndPoint;
  private final Configuration configuration;

  public VersionEndpointTask(EndpointStateMachine rpcEndPoint,
      Configuration conf) {
    this.rpcEndPoint = rpcEndPoint;
    this.configuration = conf;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {
    rpcEndPoint.lock();
    try{
      SCMVersionResponseProto versionResponse =
          rpcEndPoint.getEndPoint().getVersion(null);
      rpcEndPoint.setVersion(VersionResponse.getFromProtobuf(versionResponse));

      EndpointStateMachine.EndPointStates nextState =
          rpcEndPoint.getState().getNextState();
      rpcEndPoint.setState(nextState);
      rpcEndPoint.zeroMissedCount();
    } catch (IOException ex) {
      rpcEndPoint.logIfNeeded(ex);
    } finally {
      rpcEndPoint.unlock();
    }
    return rpcEndPoint.getState();
  }
}
