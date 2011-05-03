/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.client.replication;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * <p>
 * This class provides the administrative interface to HBase cluster
 * replication. In order to use it, the cluster and the client using
 * ReplicationAdmin must be configured with <code>hbase.replication</code>
 * set to true.
 * </p>
 * <p>
 * Adding a new peer results in creating new outbound connections from every
 * region server to a subset of region servers on the slave cluster. Each
 * new stream of replication will start replicating from the beginning of the
 * current HLog, meaning that edits from that past will be replicated.
 * </p>
 * <p>
 * Removing a peer is a destructive and irreversible operation that stops
 * all the replication streams for the given cluster and deletes the metadata
 * used to keep track of the replication state.
 * </p>
 * <p>
 * Enabling and disabling peers is currently not supported.
 * </p>
 * <p>
 * As cluster replication is still experimental, a kill switch is provided
 * in order to stop all replication-related operations, see
 * {@link #setReplicating(boolean)}. When setting it back to true, the new
 * state of all the replication streams will be unknown and may have holes.
 * Use at your own risk.
 * </p>
 * <p>
 * To see which commands are available in the shell, type
 * <code>replication</code>.
 * </p>
 */
public class ReplicationAdmin implements Closeable {

  private final ReplicationZookeeper replicationZk;
  private final Configuration configuration;
  private final HConnection connection;

  /**
   * Constructor that creates a connection to the local ZooKeeper ensemble.
   * @param conf Configuration to use
   * @throws IOException if the connection to ZK cannot be made
   * @throws RuntimeException if replication isn't enabled.
   */
  public ReplicationAdmin(Configuration conf) throws IOException {
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
      throw new RuntimeException("hbase.replication isn't true, please " +
          "enable it in order to use replication");
    }
    this.configuration = conf;
    this.connection = HConnectionManager.getConnection(conf);
    ZooKeeperWatcher zkw = this.connection.getZooKeeperWatcher();
    try {
      this.replicationZk = new ReplicationZookeeper(this.connection, conf, zkw);
    } catch (KeeperException e) {
      throw new IOException("Unable setup the ZooKeeper connection", e);
    }
  }

  /**
   * Add a new peer cluster to replicate to.
   * @param id a short that identifies the cluster
   * @param clusterKey the concatenation of the slave cluster's
   * <code>hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent</code>
   * @throws IllegalStateException if there's already one slave since
   * multi-slave isn't supported yet.
   */
  public void addPeer(String id, String clusterKey) throws IOException {
    this.replicationZk.addPeer(id, clusterKey);
  }

  /**
   * Removes a peer cluster and stops the replication to it.
   * @param id a short that identifies the cluster
   */
  public void removePeer(String id) throws IOException {
    this.replicationZk.removePeer(id);
  }

  /**
   * Restart the replication stream to the specified peer.
   * @param id a short that identifies the cluster
   */
  public void enablePeer(String id) {
    throw new NotImplementedException("Not implemented");
  }

  /**
   * Stop the replication stream to the specified peer.
   * @param id a short that identifies the cluster
   */
  public void disablePeer(String id) {
    throw new NotImplementedException("Not implemented");
  }

  /**
   * Get the number of slave clusters the local cluster has.
   * @return number of slave clusters
   */
  public int getPeersCount() {
    return this.replicationZk.listPeersIdsAndWatch().size();
  }

  /**
   * Get the current status of the kill switch, if the cluster is replicating
   * or not.
   * @return true if the cluster is replicated, otherwise false
   */
  public boolean getReplicating() throws IOException {
    try {
      return this.replicationZk.getReplication();
    } catch (KeeperException e) {
      throw new IOException("Couldn't get the replication status");
    }
  }

  /**
   * Kill switch for all replication-related features
   * @param newState true to start replication, false to stop it.
   * completely
   * @return the previous state
   */
  public boolean setReplicating(boolean newState) throws IOException {
    boolean prev = true;
    try {
      prev = getReplicating();
      this.replicationZk.setReplicating(newState);
    } catch (KeeperException e) {
      throw new IOException("Unable to set the replication state", e);
    }
    return prev;
  }

  /**
   * Get the ZK-support tool created and used by this object for replication.
   * @return the ZK-support tool
   */
  ReplicationZookeeper getReplicationZk() {
    return replicationZk;
  }

  @Override
  public void close() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
  }
}
