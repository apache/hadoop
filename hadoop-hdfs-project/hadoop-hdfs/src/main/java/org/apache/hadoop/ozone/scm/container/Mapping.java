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
package org.apache.hadoop.ozone.scm.container;


import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Mapping class contains the mapping from a name to a pipeline mapping. This is
 * used by SCM when allocating new locations and when looking up a key.
 */
public interface Mapping extends Closeable {
  /**
   * Returns the ContainerInfo from the container name.
   *
   * @param containerName - Name
   * @return - ContainerInfo such as creation state and the pipeline.
   * @throws IOException
   */
  ContainerInfo getContainer(String containerName) throws IOException;

  /**
   * Returns containers under certain conditions.
   * Search container names from start name(exclusive),
   * and use prefix name to filter the result. The max
   * size of the searching range cannot exceed the
   * value of count.
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
   * Allocates a new container for a given keyName and replication factor.
   *
   * @param replicationFactor - replication factor of the container.
   * @param containerName - Name.
   * @param owner
   * @return - Container Info.
   * @throws IOException
   */
  ContainerInfo allocateContainer(OzoneProtos.ReplicationType type,
      OzoneProtos.ReplicationFactor replicationFactor,
      String containerName, String owner) throws IOException;

  /**
   * Deletes a container from SCM.
   *
   * @param containerName - Container Name
   * @throws IOException
   */
  void deleteContainer(String containerName) throws IOException;

  /**
   * Update container state.
   * @param containerName - Container Name
   * @param event - container life cycle event
   * @return - new container state
   * @throws IOException
   */
  OzoneProtos.LifeCycleState updateContainerState(String containerName,
      OzoneProtos.LifeCycleEvent event) throws IOException;

  /**
   * Returns the container State Manager.
   * @return ContainerStateManager
   */
  ContainerStateManager getStateManager();

  /**
   * Process container report from Datanode.
   *
   * @param datanodeID Datanode ID
   * @param reportType Type of report
   * @param containerInfos container details
   */
  void processContainerReports(
      DatanodeID datanodeID,
      ContainerReportsRequestProto.reportType reportType,
      List<StorageContainerDatanodeProtocolProtos.ContainerInfo>
          containerInfos) throws IOException;

}
