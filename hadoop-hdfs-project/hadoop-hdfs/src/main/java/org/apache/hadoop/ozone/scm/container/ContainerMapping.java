
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

package org.apache.hadoop.ozone.scm.container;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB;

/**
 * Mapping class contains the mapping from a name to a pipeline mapping. This is
 * used by SCM when allocating new locations and when looking up a key.
 */
public class ContainerMapping implements Mapping {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerMapping.class);

  private final NodeManager nodeManager;
  private final long cacheSize;
  private final Lock lock;
  private final Charset encoding = Charset.forName("UTF-8");
  private final LevelDBStore containerStore;
  private final ContainerPlacementPolicy placementPolicy;
  private final long containerSize;

  /**
   * Constructs a mapping class that creates mapping between container names and
   * pipelines.
   *
   * @param nodeManager - NodeManager so that we can get the nodes that are
   * healthy to place new containers.
   * @param cacheSizeMB - Amount of memory reserved for the LSM tree to cache
   * its nodes. This is passed to LevelDB and this memory is allocated in Native
   * code space. CacheSize is specified in MB.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public ContainerMapping(final Configuration conf,
      final NodeManager nodeManager, final int cacheSizeMB) throws IOException {
    this.nodeManager = nodeManager;
    this.cacheSize = cacheSizeMB;

    // TODO: Fix this checking.
    String scmMetaDataDir = conf.get(OzoneConfigKeys
        .OZONE_CONTAINER_METADATA_DIRS);
    if ((scmMetaDataDir == null) || (scmMetaDataDir.isEmpty())) {
      throw
          new IllegalArgumentException("SCM metadata directory is not valid.");
    }
    Options options = new Options();
    options.cacheSize(this.cacheSize * OzoneConsts.MB);

    // Write the container name to pipeline mapping.
    File containerDBPath = new File(scmMetaDataDir, CONTAINER_DB);
    containerStore = new LevelDBStore(containerDBPath, options);

    this.lock = new ReentrantLock();

    this.containerSize = OzoneConsts.GB * conf.getInt(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);
    this.placementPolicy = createContainerPlacementPolicy(nodeManager, conf);
  }

  /**
   * Create pluggable container placement policy implementation instance.
   *
   * @param nodeManager - SCM node manager.
   * @param conf - configuration.
   * @return SCM container placement policy implementation instance.
   */
  @SuppressWarnings("unchecked")
  private static ContainerPlacementPolicy createContainerPlacementPolicy(
      final NodeManager nodeManager, final Configuration conf) {
    Class<? extends ContainerPlacementPolicy> implClass =
        (Class<? extends ContainerPlacementPolicy>) conf.getClass(
            ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
            SCMContainerPlacementRandom.class);

    try {
      Constructor<? extends ContainerPlacementPolicy> ctor =
          implClass.getDeclaredConstructor(NodeManager.class,
              Configuration.class);
      return ctor.newInstance(nodeManager, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(implClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
      LOG.error("Unhandled exception occured, Placement policy will not be " +
          "functional.");
      throw new IllegalArgumentException("Unable to load " +
          "ContainerPlacementPolicy", e);
    }
  }

  /**
   * Translates a list of nodes, ordered such that the first is the leader, into
   * a corresponding {@link Pipeline} object.
   * @param nodes - list of datanodes on which we will allocate the container.
   * The first of the list will be the leader node.
   * @param containerName container name
   * @return pipeline corresponding to nodes
   */
  private static Pipeline newPipelineFromNodes(final List<DatanodeID> nodes,
      final String containerName) {
    Preconditions.checkNotNull(nodes);
    Preconditions.checkArgument(nodes.size() > 0);
    String leaderId = nodes.get(0).getDatanodeUuid();
    Pipeline pipeline = new Pipeline(leaderId);
    for (DatanodeID node : nodes) {
      pipeline.addMember(node);
    }
    pipeline.setContainerName(containerName);
    return pipeline;
  }

  /**
   * Returns the Pipeline from the container name.
   *
   * @param containerName - Name
   * @return - Pipeline that makes up this container.
   */
  @Override
  public Pipeline getContainer(final String containerName) throws IOException {
    Pipeline pipeline;
    lock.lock();
    try {
      byte[] pipelineBytes =
          containerStore.get(containerName.getBytes(encoding));
      if (pipelineBytes == null) {
        throw new SCMException("Specified key does not exist. key : " +
            containerName, SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      pipeline = Pipeline.getFromProtoBuf(
          OzoneProtos.Pipeline.PARSER.parseFrom(pipelineBytes));
      return pipeline;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Allocates a new container.
   *
   * @param containerName - Name of the container.
   * @return - Pipeline that makes up this container.
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(final String containerName)
      throws IOException {
    return allocateContainer(containerName, ScmClient.ReplicationFactor.ONE);
  }

  /**
   * Allocates a new container.
   *
   * @param containerName - Name of the container.
   * @param replicationFactor - replication factor of the container.
   * @return - Pipeline that makes up this container.
   * @throws IOException
   */
  @Override
  public Pipeline allocateContainer(final String containerName,
      final ScmClient.ReplicationFactor replicationFactor) throws IOException {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    Pipeline pipeline = null;
    if (!nodeManager.isOutOfNodeChillMode()) {
      throw new SCMException("Unable to create container while in chill mode",
          SCMException.ResultCodes.CHILL_MODE_EXCEPTION);
    }

    lock.lock();
    try {
      byte[] pipelineBytes =
          containerStore.get(containerName.getBytes(encoding));
      if (pipelineBytes != null) {
        throw new SCMException("Specified container already exists. key : " +
            containerName, SCMException.ResultCodes.CONTAINER_EXISTS);
      }
      List<DatanodeID> datanodes = placementPolicy.chooseDatanodes(
          replicationFactor.getValue(), containerSize);
      // TODO: handle under replicated container
      if (datanodes != null && datanodes.size() > 0) {
        pipeline = newPipelineFromNodes(datanodes, containerName);
        containerStore.put(containerName.getBytes(encoding),
            pipeline.getProtobufMessage().toByteArray());
      } else {
        LOG.debug("Unable to find enough datanodes for new container. " +
            "Required {} found {}", replicationFactor,
            datanodes != null ? datanodes.size(): 0);
      }
    } finally {
      lock.unlock();
    }
    return pipeline;
  }

  /**
   * Deletes a container from SCM.
   *
   * @param containerName - Container name
   * @throws IOException
   *   if container doesn't exist
   *   or container store failed to delete the specified key.
   */
  @Override
  public void deleteContainer(String containerName) throws IOException {
    lock.lock();
    try {
      byte[] dbKey = containerName.getBytes(encoding);
      byte[] pipelineBytes =
          containerStore.get(dbKey);
      if(pipelineBytes == null) {
        throw new SCMException("Failed to delete container "
            + containerName + ", reason : container doesn't exist.",
            SCMException.ResultCodes.FAILED_TO_FIND_CONTAINER);
      }
      containerStore.delete(dbKey);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (containerStore != null) {
      containerStore.close();
    }
  }
}
