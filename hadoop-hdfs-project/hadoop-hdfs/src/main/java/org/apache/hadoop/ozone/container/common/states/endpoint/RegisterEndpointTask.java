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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerNodeIDProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Register a container with SCM.
 */
public class RegisterEndpointTask implements
    Callable<EndpointStateMachine.EndPointStates> {
  static final Logger LOG = LoggerFactory.getLogger(RegisterEndpointTask.class);

  private final EndpointStateMachine rpcEndPoint;
  private final Configuration conf;
  private Future<EndpointStateMachine.EndPointStates> result;
  private ContainerNodeIDProto containerNodeIDProto;

  /**
   * Creates a register endpoint task.
   *
   * @param rpcEndPoint - endpoint
   * @param conf        - conf
   */
  public RegisterEndpointTask(EndpointStateMachine rpcEndPoint,
      Configuration conf) {
    this.rpcEndPoint = rpcEndPoint;
    this.conf = conf;

  }

  /**
   * Get the ContainerNodeID Proto.
   *
   * @return ContainerNodeIDProto
   */
  public ContainerNodeIDProto getContainerNodeIDProto() {
    return containerNodeIDProto;
  }

  /**
   * Set the contiainerNodeID Proto.
   *
   * @param containerNodeIDProto
   */
  public void setContainerNodeIDProto(ContainerNodeIDProto
                                          containerNodeIDProto) {
    this.containerNodeIDProto = containerNodeIDProto;
  }

  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  @Override
  public EndpointStateMachine.EndPointStates call() throws Exception {

    if (getContainerNodeIDProto() == null) {
      LOG.error("Container ID proto cannot be null in RegisterEndpoint task, " +
          "shutting down the endpoint.");
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.SHUTDOWN);
      return EndpointStateMachine.EndPointStates.SHUTDOWN;
    }
    rpcEndPoint.lock();

    try {
      DatanodeID dnNodeID = DatanodeID.getFromProtoBuf(
          getContainerNodeIDProto().getDatanodeID());

      // TODO : Add responses to the command Queue.
      rpcEndPoint.getEndPoint().register(dnNodeID,
          conf.getStrings(OzoneConfigKeys.OZONE_SCM_NAMES));
      EndpointStateMachine.EndPointStates nextState =
          rpcEndPoint.getState().getNextState();
      rpcEndPoint.setState(nextState);
      rpcEndPoint.zeroMissedCount();
    } catch (IOException ex) {
      rpcEndPoint.logIfNeeded(ex,
          OzoneClientUtils.getScmHeartbeatInterval(conf));
    } finally {
      rpcEndPoint.unlock();
    }

    return rpcEndPoint.getState();
  }
}
