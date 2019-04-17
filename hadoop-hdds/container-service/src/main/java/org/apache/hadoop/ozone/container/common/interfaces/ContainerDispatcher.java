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

package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;

import java.util.Set;

/**
 * Dispatcher acts as the bridge between the transport layer and
 * the actual container layer. This layer is capable of transforming
 * protobuf objects into corresponding class and issue the function call
 * into the lower layers.
 *
 * The reply from the request is dispatched to the client.
 */
public interface ContainerDispatcher {
  /**
   * Dispatches commands to container layer.
   * @param msg - Command Request
   * @param context - Context info related to ContainerStateMachine
   * @return Command Response
   */
  ContainerCommandResponseProto dispatch(ContainerCommandRequestProto msg,
      DispatcherContext context);

  /**
   * Validates whether the container command should be executed on the pipeline
   * or not. Will be invoked by the leader node in the Ratis pipeline
   * @param msg containerCommand
   * @throws StorageContainerException
   */
  void validateContainerCommand(
      ContainerCommandRequestProto msg) throws StorageContainerException;

  /**
   * Initialize the Dispatcher.
   */
  void init();

  /**
   * finds and builds the missing containers in case of a lost disk etc
   * in the ContainerSet.
   */
  void buildMissingContainerSet(Set<Long> createdContainers);

  /**
   * Shutdown Dispatcher services.
   */
  void shutdown();

  /**
   * Returns the handler for the specified containerType.
   * @param containerType
   * @return
   */
  Handler getHandler(ContainerProtos.ContainerType containerType);

  /**
   * If scmId is not set, this will set scmId, otherwise it is a no-op.
   * @param scmId
   */
  void setScmId(String scmId);
}
