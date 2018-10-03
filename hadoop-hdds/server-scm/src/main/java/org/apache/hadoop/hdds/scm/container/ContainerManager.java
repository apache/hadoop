/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.pipelines.PipelineSelector;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ContainerManager class contains the mapping from a name to a pipeline
 * mapping. This is used by SCM when allocating new locations and when
 * looking up a key.
 */
public interface ContainerManager extends Closeable {
  /**
   * Returns the ContainerInfo from the container ID.
   *
   * @param containerID - ID of container.
   * @return - ContainerInfo such as creation state and the pipeline.
   * @throws IOException
   */
  ContainerInfo getContainer(long containerID) throws IOException;

  /**
   * Returns the ContainerInfo from the container ID.
   *
   * @param containerID - ID of container.
   * @return - ContainerWithPipeline such as creation state and the pipeline.
   * @throws IOException
   */
  ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException;

  /**
   * Returns containers under certain conditions.
   * Search container IDs from start ID(exclusive),
   * The max size of the searching range cannot exceed the
   * value of count.
   *
   * @param startContainerID start containerID, >=0,
   * start searching at the head if 0.
   * @param count count must be >= 0
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big.
   *
   * @return a list of container.
   * @throws IOException
   */
  List<ContainerInfo> listContainer(long startContainerID, int count)
      throws IOException;

  /**
   * Allocates a new container for a given keyName and replication factor.
   *
   * @param replicationFactor - replication factor of the container.
   * @param owner
   * @return - ContainerWithPipeline.
   * @throws IOException
   */
  ContainerWithPipeline allocateContainer(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor replicationFactor, String owner)
      throws IOException;

  /**
   * Deletes a container from SCM.
   *
   * @param containerID - Container ID
   * @throws IOException
   */
  void deleteContainer(long containerID) throws IOException;

  /**
   * Update container state.
   * @param containerID - Container ID
   * @param event - container life cycle event
   * @return - new container state
   * @throws IOException
   */
  HddsProtos.LifeCycleState updateContainerState(long containerID,
      HddsProtos.LifeCycleEvent event) throws IOException;

  /**
   * Returns the container State Manager.
   * @return ContainerStateManager
   */
  ContainerStateManager getStateManager();

  /**
   * Process container report from Datanode.
   *
   * @param reports Container report
   */
  void processContainerReports(DatanodeDetails datanodeDetails,
      ContainerReportsProto reports, boolean isRegisterCall)
      throws IOException;

  /**
   * Update deleteTransactionId according to deleteTransactionMap.
   *
   * @param deleteTransactionMap Maps the containerId to latest delete
   *                             transaction id for the container.
   * @throws IOException
   */
  void updateDeleteTransactionId(Map<Long, Long> deleteTransactionMap)
      throws IOException;

  /**
   * Returns the ContainerWithPipeline.
   * @return NodeManager
   */
  ContainerWithPipeline getMatchingContainerWithPipeline(long size,
      String owner, ReplicationType type, ReplicationFactor factor,
      LifeCycleState state) throws IOException;

  PipelineSelector getPipelineSelector();
}
