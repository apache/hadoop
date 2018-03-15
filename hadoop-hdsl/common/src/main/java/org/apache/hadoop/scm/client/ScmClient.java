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
package org.apache.hadoop.scm.client;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerData;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

/**
 * The interface to call into underlying container layer.
 *
 * Written as interface to allow easy testing: implement a mock container layer
 * for standalone testing of CBlock API without actually calling into remote
 * containers. Actual container layer can simply re-implement this.
 *
 * NOTE this is temporarily needed class. When SCM containers are full-fledged,
 * this interface will likely be removed.
 */
@InterfaceStability.Unstable
public interface ScmClient {
  /**
   * Creates a Container on SCM and returns the pipeline.
   * @param containerId - String container ID
   * @return Pipeline
   * @throws IOException
   */
  Pipeline createContainer(String containerId, String owner) throws IOException;

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   * @param containerId - String Container ID
   * @return Pipeline
   * @throws IOException
   */
  Pipeline getContainer(String containerId) throws IOException;

  /**
   * Close a container by name.
   *
   * @param pipeline the container to be closed.
   * @throws IOException
   */
  void closeContainer(Pipeline pipeline) throws IOException;

  /**
   * Deletes an existing container.
   * @param pipeline - Pipeline that represents the container.
   * @param force - true to forcibly delete the container.
   * @throws IOException
   */
  void deleteContainer(Pipeline pipeline, boolean force) throws IOException;

  /**
   * Lists a range of containers and get their info.
   *
   * @param startName start name, if null, start searching at the head.
   * @param prefixName prefix name, if null, then filter is disabled.
   * @param count count, if count < 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   *
   * @return a list of pipeline.
   * @throws IOException
   */
  List<ContainerInfo> listContainer(String startName, String prefixName,
      int count) throws IOException;

  /**
   * Read meta data from an existing container.
   * @param pipeline - Pipeline that represents the container.
   * @return ContainerInfo
   * @throws IOException
   */
  ContainerData readContainer(Pipeline pipeline) throws IOException;


  /**
   * Gets the container size -- Computed by SCM from Container Reports.
   * @param pipeline - Pipeline
   * @return number of bytes used by this container.
   * @throws IOException
   */
  long getContainerSize(Pipeline pipeline) throws IOException;

  /**
   * Creates a Container on SCM and returns the pipeline.
   * @param type - Replication Type.
   * @param replicationFactor - Replication Factor
   * @param containerId - Container ID
   * @return Pipeline
   * @throws IOException - in case of error.
   */
  Pipeline createContainer(HdslProtos.ReplicationType type,
      HdslProtos.ReplicationFactor replicationFactor, String containerId,
      String owner) throws IOException;

  /**
   * Returns a set of Nodes that meet a query criteria.
   * @param nodeStatuses - A set of criteria that we want the node to have.
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  HdslProtos.NodePool queryNode(EnumSet<HdslProtos.NodeState> nodeStatuses,
      HdslProtos.QueryScope queryScope, String poolName) throws IOException;

  /**
   * Creates a specified replication pipeline.
   * @param type - Type
   * @param factor - Replication factor
   * @param nodePool - Set of machines.
   * @throws IOException
   */
  Pipeline createReplicationPipeline(HdslProtos.ReplicationType type,
      HdslProtos.ReplicationFactor factor, HdslProtos.NodePool nodePool)
      throws IOException;
}
