/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.states.endpoint;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneClientUtils;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerNodeIDProto;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Heartbeat class for SCMs.
 */
public class HeartbeatEndpointTask
    implements Callable<EndpointStateMachine.EndPointStates> {
  private final EndpointStateMachine rpcEndpoint;
  private final Configuration conf;
  private ContainerNodeIDProto containerNodeIDProto;

  /**
   * Constructs a SCM heart beat.
   *
   * @param conf Config.
   */
  public HeartbeatEndpointTask(EndpointStateMachine rpcEndpoint,
      Configuration conf) {
    this.rpcEndpoint = rpcEndpoint;
    this.conf = conf;
  }

  /**
   * Get the container Node ID proto.
   *
   * @return ContainerNodeIDProto
   */
  public ContainerNodeIDProto getContainerNodeIDProto() {
    return containerNodeIDProto;
  }

  /**
   * Set container node ID proto.
   *
   * @param containerNodeIDProto - the node id.
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
    rpcEndpoint.lock();
    try {
      Preconditions.checkState(this.containerNodeIDProto != null);
      DatanodeID datanodeID = DatanodeID.getFromProtoBuf(this
          .containerNodeIDProto.getDatanodeID());
      // TODO : Add the command to command processor queue.
      rpcEndpoint.getEndPoint().sendHeartbeat(datanodeID);
      rpcEndpoint.zeroMissedCount();
    } catch (IOException ex) {
      rpcEndpoint.logIfNeeded(ex,
          OzoneClientUtils.getScmHeartbeatInterval(this.conf));
    } finally {
      rpcEndpoint.unlock();
    }
    return rpcEndpoint.getState();
  }
}
