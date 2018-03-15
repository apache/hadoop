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
package org.apache.hadoop.cblock.util;

import org.apache.hadoop.cblock.meta.ContainerDescriptor;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos.ContainerData;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is the one that directly talks to SCM server.
 *
 * NOTE : this is only a mock class, only to allow testing volume
 * creation without actually creating containers. In real world, need to be
 * replaced with actual container look up calls.
 *
 */
public class MockStorageClient implements ScmClient {
  private static AtomicInteger currentContainerId =
      new AtomicInteger(0);

  /**
   * Ask SCM to get a exclusive container.
   *
   * @return A container descriptor object to locate this container
   * @throws Exception
   */
  @Override
  public Pipeline createContainer(String containerId, String owner)
      throws IOException {
    int contId = currentContainerId.getAndIncrement();
    ContainerLookUpService.addContainer(Long.toString(contId));
    return ContainerLookUpService.lookUp(Long.toString(contId))
        .getPipeline();
  }

  /**
   * As this is only a testing class, with all "container" maintained in
   * memory, no need to really delete anything for now.
   * @throws IOException
   */
  @Override
  public void deleteContainer(Pipeline pipeline, boolean force)
      throws IOException {

  }

  /**
   * This is a mock class, so returns the container infos of start container
   * and end container.
   *
   * @param startName start container name.
   * @param prefixName prefix container name.
   * @param count count.
   * @return a list of pipeline.
   * @throws IOException
   */
  @Override
  public List<ContainerInfo> listContainer(String startName,
      String prefixName, int count) throws IOException {
    List<ContainerInfo> containerList = new ArrayList<>();
    ContainerDescriptor containerDescriptor =
        ContainerLookUpService.lookUp(startName);
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerName(containerDescriptor.getContainerID())
        .setPipeline(containerDescriptor.getPipeline())
        .setState(HdslProtos.LifeCycleState.ALLOCATED)
        .build();
    containerList.add(container);
    return containerList;
  }

  /**
   * Create a instance of ContainerData by a given container id,
   * since this is a testing class, there is no need set up the hold
   * env to get the meta data of the container.
   * @param pipeline
   * @return
   * @throws IOException
   */
  @Override
  public ContainerData readContainer(Pipeline pipeline) throws IOException {
    return ContainerData.newBuilder()
        .setName(pipeline.getContainerName())
        .build();
  }

  /**
   * Return reference to an *existing* container with given ID.
   *
   * @param containerId
   * @return
   * @throws IOException
   */
  public Pipeline getContainer(String containerId)
      throws IOException {
    return ContainerLookUpService.lookUp(containerId).getPipeline();
  }

  @Override
  public void closeContainer(Pipeline container) throws IOException {
    // Do nothing, because the mock container does not have the notion of
    // "open" and "close".
  }

  @Override
  public long getContainerSize(Pipeline pipeline) throws IOException {
    // just return a constant value for now
    return 5L * OzoneConsts.GB; // 5GB
  }

  @Override
  public Pipeline createContainer(HdslProtos.ReplicationType type,
      HdslProtos.ReplicationFactor replicationFactor, String containerId,
      String owner) throws IOException {
    int contId = currentContainerId.getAndIncrement();
    ContainerLookUpService.addContainer(Long.toString(contId));
    return ContainerLookUpService.lookUp(Long.toString(contId))
        .getPipeline();
  }

  /**
   * Returns a set of Nodes that meet a query criteria.
   *
   * @param nodeStatuses - A set of criteria that we want the node to have.
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  @Override
  public HdslProtos.NodePool queryNode(EnumSet<HdslProtos.NodeState>
      nodeStatuses, HdslProtos.QueryScope queryScope, String poolName)
      throws IOException {
    return null;
  }

  /**
   * Creates a specified replication pipeline.
   *
   * @param type - Type
   * @param factor - Replication factor
   * @param nodePool - Set of machines.
   * @throws IOException
   */
  @Override
  public Pipeline createReplicationPipeline(HdslProtos.ReplicationType type,
      HdslProtos.ReplicationFactor factor, HdslProtos.NodePool nodePool)
      throws IOException {
    return null;
  }
}
