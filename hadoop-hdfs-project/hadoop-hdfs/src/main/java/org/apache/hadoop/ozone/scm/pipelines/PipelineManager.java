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
package org.apache.hadoop.ozone.scm.pipelines;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.List;

/**
 * Manage Ozone pipelines.
 */
public interface PipelineManager {

  /**
   * This function is called by the Container Manager while allocating a new
   * container. The client specifies what kind of replication pipeline is
   * needed and based on the replication type in the request appropriate
   * Interface is invoked.
   *
   * @param containerName Name of the container
   * @param replicationFactor - Replication Factor
   * @return a Pipeline.
   */
  Pipeline getPipeline(String containerName,
      OzoneProtos.ReplicationFactor replicationFactor) throws IOException;

  /**
   * Creates a pipeline from a specified set of Nodes.
   * @param pipelineID - Name of the pipeline
   * @param datanodes - The list of datanodes that make this pipeline.
   */
  void createPipeline(String pipelineID, List<DatanodeID> datanodes)
      throws IOException;;

  /**
   * Close the  pipeline with the given clusterId.
   */
  void closePipeline(String pipelineID) throws IOException;

  /**
   * list members in the pipeline .
   * @return the datanode
   */
  List<DatanodeID> getMembers(String pipelineID) throws IOException;

  /**
   * Update the datanode list of the pipeline.
   */
  void updatePipeline(String pipelineID, List<DatanodeID> newDatanodes)
      throws IOException;
}
