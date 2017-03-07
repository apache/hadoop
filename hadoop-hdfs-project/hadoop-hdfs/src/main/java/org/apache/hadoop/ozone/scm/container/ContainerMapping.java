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
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.utils.LevelDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.iq80.leveldb.Options;

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
  private final Random rand;

  /**
   * Constructs a mapping class that creates mapping between container names and
   * pipelines.
   *
   * @param nodeManager - NodeManager so that we can get the nodes that are
   * healthy to place new containers.
   * @param cacheSizeMB - Amount of memory reserved for the LSM tree to cache
   * its nodes. This is passed to LevelDB and this memory is allocated in Native
   * code space. CacheSize is specified in MB.
   */
  @SuppressWarnings("unchecked")
  public ContainerMapping(Configuration conf, NodeManager nodeManager,
      int cacheSizeMB) throws IOException {
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
    options.cacheSize(this.cacheSize * (1024L * 1024L));
    options.createIfMissing();

    // Write the container name to pipeline mapping.
    File containerDBPath = new File(scmMetaDataDir, "container.db");
    containerStore = new LevelDBStore(containerDBPath, options);

    this.lock = new ReentrantLock();
    rand = new Random();
  }

  /**
   * // TODO : Fix the code to handle multiple nodes.
   * Translates a list of nodes, ordered such that the first is the leader, into
   * a corresponding {@link Pipeline} object.
   *
   * @param node datanode on which we will allocate the contianer.
   * @param containerName container name
   * @return pipeline corresponding to nodes
   */
  private static Pipeline newPipelineFromNodes(DatanodeID node, String
      containerName) {
    Preconditions.checkNotNull(node);
    String leaderId = node.getDatanodeUuid();
    Pipeline pipeline = new Pipeline(leaderId);
    pipeline.addMember(node);
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
  public Pipeline getContainer(String containerName) throws IOException {
    Pipeline pipeline = null;
    lock.lock();
    try {
      byte[] pipelineBytes =
          containerStore.get(containerName.getBytes(encoding));
      if (pipelineBytes == null) {
        throw new IOException("Specified key does not exist. key : " +
            containerName);
      }
      pipeline = Pipeline.getFromProtoBuf(
          ContainerProtos.Pipeline.PARSER.parseFrom(pipelineBytes));
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
  public Pipeline allocateContainer(String containerName) throws IOException {
    Preconditions.checkNotNull(containerName);
    Preconditions.checkState(!containerName.isEmpty());
    Pipeline pipeline = null;
    if (!nodeManager.isOutOfNodeChillMode()) {
      throw new IOException("Unable to create container while in chill mode");
    }

    lock.lock();
    try {
      byte[] pipelineBytes =
          containerStore.get(containerName.getBytes(encoding));
      if (pipelineBytes != null) {
        throw new IOException("Specified container already exists. key : " +
            containerName);
      }
      DatanodeID id = getDatanodeID();
      if (id != null) {
        pipeline = newPipelineFromNodes(id, containerName);
        containerStore.put(containerName.getBytes(encoding),
            pipeline.getProtobufMessage().toByteArray());
      }
    } finally {
      lock.unlock();
    }
    return pipeline;
  }

  /**
   * Returns a random Datanode ID from the list of healthy nodes.
   *
   * @return Datanode ID
   * @throws IOException
   */
  private DatanodeID getDatanodeID() throws IOException {
    List<DatanodeID> healthyNodes =
        nodeManager.getNodes(NodeManager.NODESTATE.HEALTHY);

    if (healthyNodes.size() == 0) {
      throw new IOException("No healthy node found to allocate container.");
    }

    int index = rand.nextInt() % healthyNodes.size();
    return healthyNodes.get(Math.abs(index));
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
