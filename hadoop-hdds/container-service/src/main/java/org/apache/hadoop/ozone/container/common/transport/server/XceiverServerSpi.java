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

package org.apache.hadoop.ozone.container.common.transport.server;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.io.IOException;

/** A server endpoint that acts as the communication layer for Ozone
 * containers. */
public interface XceiverServerSpi {
  /** Starts the server. */
  void start() throws IOException;

  /** Stops a running server. */
  void stop();

  /** Get server IPC port. */
  int getIPCPort();

  /**
   * Returns the Replication type supported by this end-point.
   * @return enum -- {Stand_Alone, Ratis, Chained}
   */
  HddsProtos.ReplicationType getServerType();

  /**
   * submits a containerRequest to be performed by the replication pipeline.
   * @param request ContainerCommandRequest
   */
  void submitRequest(ContainerCommandRequestProto request,
      HddsProtos.PipelineID pipelineID)
      throws IOException;
}
