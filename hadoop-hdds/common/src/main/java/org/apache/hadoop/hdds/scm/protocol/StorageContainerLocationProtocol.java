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

package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerLocationProtocolProtos.ObjectStageChangeRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.security.KerberosInfo;

/**
 * ContainerLocationProtocol is used by an HDFS node to find the set of nodes
 * that currently host a container.
 */
@KerberosInfo(serverPrincipal = ScmConfigKeys.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
public interface StorageContainerLocationProtocol extends Closeable {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Asks SCM where a container should be allocated. SCM responds with the
   * set of datanodes that should be used creating this container.
   *
   */
  ContainerWithPipeline allocateContainer(
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor factor, String owner)
      throws IOException;

  /**
   * Ask SCM the location of the container. SCM responds with a group of
   * nodes where this container and its replicas are located.
   *
   * @param containerID - ID of the container.
   * @return ContainerInfo - the container info such as where the pipeline
   *                         is located.
   * @throws IOException
   */
  ContainerInfo getContainer(long containerID) throws IOException;

  /**
   * Ask SCM the location of the container. SCM responds with a group of
   * nodes where this container and its replicas are located.
   *
   * @param containerID - ID of the container.
   * @return ContainerWithPipeline - the container info with the pipeline.
   * @throws IOException
   */
  ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException;

  /**
   * Ask SCM a list of containers with a range of container names
   * and the limit of count.
   * Search container names between start name(exclusive), and
   * use prefix name to filter the result. the max size of the
   * searching range cannot exceed the value of count.
   *
   * @param startContainerID start container ID.
   * @param count count, if count {@literal <} 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   *
   * @return a list of container.
   * @throws IOException
   */
  List<ContainerInfo> listContainer(long startContainerID, int count)
      throws IOException;

  /**
   * Deletes a container in SCM.
   *
   * @param containerID
   * @throws IOException
   *   if failed to delete the container mapping from db store
   *   or container doesn't exist.
   */
  void deleteContainer(long containerID) throws IOException;

  /**
   *  Queries a list of Node Statuses.
   * @param state
   * @return List of Datanodes.
   */
  List<HddsProtos.Node> queryNode(HddsProtos.NodeState state,
      HddsProtos.QueryScope queryScope, String poolName) throws IOException;

  /**
   * Notify from client when begin or finish creating objects like pipeline
   * or containers on datanodes.
   * Container will be in Operational state after that.
   * @param type object type
   * @param id object id
   * @param op operation type (e.g., create, close, delete)
   * @param stage creation stage
   */
  void notifyObjectStageChange(
      ObjectStageChangeRequestProto.Type type, long id,
      ObjectStageChangeRequestProto.Op op,
      ObjectStageChangeRequestProto.Stage stage) throws IOException;

  /**
   * Creates a replication pipeline of a specified type.
   * @param type - replication type
   * @param factor - factor 1 or 3
   * @param nodePool - optional machine list to build a pipeline.
   * @throws IOException
   */
  Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException;

  /**
   * Returns the list of active Pipelines.
   *
   * @return list of Pipeline
   *
   * @throws IOException in case of any exception
   */
  List<Pipeline> listPipelines() throws IOException;

  /**
   * Closes a pipeline given the pipelineID.
   *
   * @param pipelineID ID of the pipeline to demolish
   * @throws IOException
   */
  void closePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Returns information about SCM.
   *
   * @return {@link ScmInfo}
   * @throws IOException
   */
  ScmInfo getScmInfo() throws IOException;

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  boolean inSafeMode() throws IOException;

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  boolean forceExitSafeMode() throws IOException;
}
