/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.scm.node;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.scm.exceptions.SCMException;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;

import static org.apache.hadoop.ozone.web.util.ServerUtils.getOzoneMetaDirPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.NODEPOOL_DB;
import static org.apache.hadoop.ozone.scm
    .exceptions.SCMException.ResultCodes.FAILED_TO_LOAD_NODEPOOL;
import static org.apache.hadoop.ozone.scm
    .exceptions.SCMException.ResultCodes.FAILED_TO_FIND_NODE_IN_POOL;
import static org.apache.hadoop.scm
    .ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.scm
    .ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;

/**
 * SCM node pool manager that manges node pools.
 */
public final class SCMNodePoolManager implements NodePoolManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMNodePoolManager.class);
  private static final List<DatanodeID> EMPTY_NODE_LIST = new ArrayList<>();
  private static final List<String> EMPTY_NODEPOOL_LIST = new ArrayList<>();
  public static final String DEFAULT_NODEPOOL = "DefaultNodePool";

  // DB that saves the node to node pool mapping.
  private MetadataStore nodePoolStore;

  // In-memory node pool to nodes mapping
  private HashMap<String, Set<DatanodeID>> nodePools;

  // Read-write lock for nodepool operations
  private ReadWriteLock lock;

  /**
   * Construct SCMNodePoolManager class that manages node to node pool mapping.
   * @param conf - configuration.
   * @throws IOException
   */
  public SCMNodePoolManager(final OzoneConfiguration conf)
      throws IOException {
    final int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    File metaDir = getOzoneMetaDirPath(conf);
    String scmMetaDataDir = metaDir.getPath();
    File nodePoolDBPath = new File(scmMetaDataDir, NODEPOOL_DB);
    nodePoolStore = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setDbFile(nodePoolDBPath)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();
    nodePools = new HashMap<>();
    lock = new ReentrantReadWriteLock();
    init();
  }

  /**
   * Initialize the in-memory store based on persist store from level db.
   * No lock is needed as init() is only invoked by constructor.
   * @throws SCMException
   */
  private void init() throws SCMException {
    try {
      nodePoolStore.iterate(null, (key, value) -> {
        try {
          DatanodeID nodeId = DatanodeID.getFromProtoBuf(
              HdfsProtos.DatanodeIDProto.PARSER.parseFrom(key));
          String poolName = DFSUtil.bytes2String(value);

          Set<DatanodeID> nodePool = null;
          if (nodePools.containsKey(poolName)) {
            nodePool = nodePools.get(poolName);
          } else {
            nodePool = new HashSet<>();
            nodePools.put(poolName, nodePool);
          }
          nodePool.add(nodeId);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adding node: {} to node pool: {}",
                nodeId, poolName);
          }
        } catch (IOException e) {
          LOG.warn("Can't add a datanode to node pool, continue next...");
        }
        return true;
      });
    } catch (IOException e) {
      LOG.error("Loading node pool error " + e);
      throw new SCMException("Failed to load node pool",
          FAILED_TO_LOAD_NODEPOOL);
    }
  }

  /**
   * Add a datanode to a node pool.
   * @param pool - name of the node pool.
   * @param node - name of the datanode.
   */
  @Override
  public void addNode(final String pool, final DatanodeID node)
      throws IOException {
    Preconditions.checkNotNull(pool, "pool name is null");
    Preconditions.checkNotNull(node, "node is null");
    lock.writeLock().lock();
    try {
      // add to the persistent store
      nodePoolStore.put(node.getProtoBufMessage().toByteArray(),
          DFSUtil.string2Bytes(pool));

      // add to the in-memory store
      Set<DatanodeID> nodePool = null;
      if (nodePools.containsKey(pool)) {
        nodePool = nodePools.get(pool);
      } else {
        nodePool = new HashSet<DatanodeID>();
        nodePools.put(pool, nodePool);
      }
      nodePool.add(node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove a datanode from a node pool.
   * @param pool - name of the node pool.
   * @param node - datanode id.
   * @throws SCMException
   */
  @Override
  public void removeNode(final String pool, final DatanodeID node)
      throws SCMException {
    Preconditions.checkNotNull(pool, "pool name is null");
    Preconditions.checkNotNull(node, "node is null");
    lock.writeLock().lock();
    try {
      // Remove from the persistent store
      byte[] kName = node.getProtoBufMessage().toByteArray();
      byte[] kData = nodePoolStore.get(kName);
      if (kData == null) {
        throw new SCMException(String.format("Unable to find node %s from" +
            " pool %s in DB.", DFSUtil.bytes2String(kName), pool),
            FAILED_TO_FIND_NODE_IN_POOL);
      }
      nodePoolStore.delete(kName);

      // Remove from the in-memory store
      if (nodePools.containsKey(pool)) {
        Set<DatanodeID> nodePool = nodePools.get(pool);
        nodePool.remove(node);
      } else {
        throw new SCMException(String.format("Unable to find node %s from" +
            " pool %s in MAP.", DFSUtil.bytes2String(kName), pool),
            FAILED_TO_FIND_NODE_IN_POOL);      }
    } catch (IOException e) {
      throw new SCMException("Failed to remove node " + node.toString()
          + " from node pool " + pool, e,
          SCMException.ResultCodes.IO_EXCEPTION);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get all the node pools.
   * @return all the node pools.
   */
  @Override
  public List<String> getNodePools() {
    lock.readLock().lock();
    try {
      if (!nodePools.isEmpty()) {
        return nodePools.keySet().stream().collect(Collectors.toList());
      } else {
        return EMPTY_NODEPOOL_LIST;
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all datanodes of a specific node pool.
   * @param pool - name of the node pool.
   * @return all datanodes of the specified node pool.
   */
  @Override
  public List<DatanodeID> getNodes(final String pool) {
    Preconditions.checkNotNull(pool, "pool name is null");
    if (nodePools.containsKey(pool)) {
      return nodePools.get(pool).stream().collect(Collectors.toList());
    } else {
      return EMPTY_NODE_LIST;
    }
  }

  /**
   * Get the node pool name if the node has been added to a node pool.
   * @param datanodeID - datanode ID.
   * @return node pool name if it has been assigned.
   * null if the node has not been assigned to any node pool yet.
   * TODO: Put this in a in-memory map if performance is an issue.
   */
  @Override
  public String getNodePool(final DatanodeID datanodeID) throws SCMException {
    Preconditions.checkNotNull(datanodeID, "node is null");
    try {
      byte[]  result = nodePoolStore.get(
          datanodeID.getProtoBufMessage().toByteArray());
      return result == null ? null : DFSUtil.bytes2String(result);
    } catch (IOException e) {
      throw new SCMException("Failed to get node pool for node "
          + datanodeID.toString(), e,
          SCMException.ResultCodes.IO_EXCEPTION);
    }
  }

  /**
   * Close node pool level db store.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    nodePoolStore.close();
  }
}
