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

package org.apache.hadoop.scm.protocol;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.ozone.protocol.proto.StorageContainerLocationProtocolProtos.NotifyObjectCreationStageRequestProto;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;

/**
 * ContainerLocationProtocol is used by an HDFS node to find the set of nodes
 * that currently host a container.
 */
public interface StorageContainerLocationProtocol {
  /**
   * Asks SCM where a container should be allocated. SCM responds with the
   * set of datanodes that should be used creating this container.
   *
   */
  Pipeline allocateContainer(OzoneProtos.ReplicationType replicationType,
      OzoneProtos.ReplicationFactor factor, String containerName)
      throws IOException;

  /**
   * Ask SCM the location of the container. SCM responds with a group of
   * nodes where this container and its replicas are located.
   *
   * @param containerName - Name of the container.
   * @return Pipeline - the pipeline where container locates.
   * @throws IOException
   */
  Pipeline getContainer(String containerName) throws IOException;

  /**
   * Ask SCM a list of containers with a range of container names
   * and the limit of count.
   * Search container names between start name(exclusive), and
   * use prefix name to filter the result. the max size of the
   * searching range cannot exceed the value of count.
   *
   * @param startName start name, if null, start searching at the head.
   * @param prefixName prefix name, if null, then filter is disabled.
   * @param count count, if count < 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   *
   * @return a list of container.
   * @throws IOException
   */
  List<ContainerInfo> listContainer(String startName, String prefixName,
      int count) throws IOException;

  /**
   * Deletes a container in SCM.
   *
   * @param containerName
   * @throws IOException
   *   if failed to delete the container mapping from db store
   *   or container doesn't exist.
   */
  void deleteContainer(String containerName) throws IOException;

  /**
   *  Queries a list of Node Statuses.
   * @param nodeStatuses
   * @return List of Datanodes.
   */
  OzoneProtos.NodePool queryNode(EnumSet<OzoneProtos.NodeState> nodeStatuses,
      OzoneProtos.QueryScope queryScope, String poolName) throws IOException;

  /**
   * Notify from client when begin or finish creating objects like pipeline
   * or containers on datanodes.
   * Container will be in Operational state after that.
   * @param type object type
   * @param name object name
   * @param stage creation stage
   */
  void notifyObjectCreationStage(
      NotifyObjectCreationStageRequestProto.Type type, String name,
      NotifyObjectCreationStageRequestProto.Stage stage) throws IOException;

  /**
   * Creates a replication pipeline of a specified type.
   * @param type - replication type
   * @param factor - factor 1 or 3
   * @param nodePool - optional machine list to build a pipeline.
   * @throws IOException
   */
  Pipeline createReplicationPipeline(OzoneProtos.ReplicationType type,
      OzoneProtos.ReplicationFactor factor, OzoneProtos.NodePool nodePool)
      throws IOException;

  /**
   * Clsoe a container.
   *
   * @param containerName the name of the container to close.
   * @throws IOException
   */
  void closeContainer(String containerName) throws IOException;
}
