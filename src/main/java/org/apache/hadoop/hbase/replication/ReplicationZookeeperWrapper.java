/*
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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class serves as a helper for all things related to zookeeper
 * in replication.
 * <p/>
 * The layout looks something like this under zookeeper.znode.parent
 * for the master cluster:
 * <p/>
 * <pre>
 * replication/
 *  master     {contains a full cluster address}
 *  state      {contains true or false}
 *  clusterId  {contains a byte}
 *  peers/
 *    1/   {contains a full cluster address}
 *    2/
 *    ...
 *  rs/ {lists all RS that replicate}
 *    startcode1/ {lists all peer clusters}
 *      1/ {lists hlogs to process}
 *        10.10.1.76%3A53488.123456789 {contains nothing or a position}
 *        10.10.1.76%3A53488.123456790
 *        ...
 *      2/
 *      ...
 *    startcode2/
 *    ...
 * </pre>
 */
public class ReplicationZookeeperWrapper {

  private static final Log LOG =
      LogFactory.getLog(ReplicationZookeeperWrapper.class);
  // Name of znode we use to lock when failover
  private final static String RS_LOCK_ZNODE = "lock";
  // Our handle on zookeeper
  private final ZooKeeperWrapper zookeeperWrapper;
  // Map of addresses of peer clusters with their ZKW
  private final Map<String, ZooKeeperWrapper> peerClusters;
  // Path to the root replication znode
  private final String replicationZNode;
  // Path to the peer clusters znode
  private final String peersZNode;
  // Path to the znode that contains all RS that replicates
  private final String rsZNode;
  // Path to this region server's name under rsZNode
  private final String rsServerNameZnode;
  // Name node if the replicationState znode
  private final String replicationStateNodeName;
  // If this RS is part of a master cluster
  private final boolean replicationMaster;
  private final Configuration conf;
  // Is this cluster replicating at the moment?
  private final AtomicBoolean replicating;
  // Byte (stored as string here) that identifies this cluster
  private final String clusterId;

  /**
   * Constructor used by region servers, connects to the peer cluster right away.
   *
   * @param zookeeperWrapper zkw to wrap
   * @param conf             conf to use
   * @param replicating    atomic boolean to start/stop replication
   * @param rsName      the name of this region server, null if
   *                         using RZH only to use the helping methods
   * @throws IOException
   */
  public ReplicationZookeeperWrapper(
      ZooKeeperWrapper zookeeperWrapper, Configuration conf,
      final AtomicBoolean replicating, String rsName) throws IOException {
    this.zookeeperWrapper = zookeeperWrapper;
    this.conf = conf;
    String replicationZNodeName =
        conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName =
        conf.get("zookeeper.znode.replication.peers", "peers");
    String repMasterZNodeName =
        conf.get("zookeeper.znode.replication.master", "master");
    this.replicationStateNodeName =
        conf.get("zookeeper.znode.replication.state", "state");
    String clusterIdZNodeName =
        conf.get("zookeeper.znode.replication.clusterId", "clusterId");
    String rsZNodeName =
        conf.get("zookeeper.znode.replication.rs", "rs");
    String thisCluster = this.conf.get(HConstants.ZOOKEEPER_QUORUM) + ":" +
          this.conf.get("hbase.zookeeper.property.clientPort") + ":" +
          this.conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);

    this.peerClusters = new HashMap<String, ZooKeeperWrapper>();
    this.replicationZNode = zookeeperWrapper.getZNode(
        zookeeperWrapper.getParentZNode(), replicationZNodeName);
    this.peersZNode =
        zookeeperWrapper.getZNode(replicationZNode, peersZNodeName);
    this.rsZNode =
        zookeeperWrapper.getZNode(replicationZNode, rsZNodeName);

    this.replicating = replicating;
    setReplicating();
    String idResult = Bytes.toString(
        this.zookeeperWrapper.getData(this.replicationZNode,
        clusterIdZNodeName));
    this.clusterId =
        idResult == null ?
            Byte.toString(HConstants.DEFAULT_CLUSTER_ID) : idResult;
    String address = Bytes.toString(
        this.zookeeperWrapper.getData(this.replicationZNode,
          repMasterZNodeName));
    this.replicationMaster = thisCluster.equals(address);
    LOG.info("This cluster (" + thisCluster + ") is a "
          + (this.replicationMaster ? "master" : "slave") + " for replication" +
          ", compared with (" + address + ")");
    if (rsName != null) {
      this.rsServerNameZnode =
          this.zookeeperWrapper.getZNode(rsZNode, rsName);
      List<String> znodes = this.zookeeperWrapper.listZnodes(this.peersZNode,
          new ReplicationStatusWatcher());
      if (znodes != null) {
        for (String znode : znodes) {
          connectToPeer(znode);
        }
      }
    } else {
      this.rsServerNameZnode = null;
    }

  }

  /**
   * Returns all region servers from given peer
   *
   * @param peerClusterId (byte) the cluster to interrogate
   * @return addresses of all region servers
   */
  public List<HServerAddress> getPeersAddresses(String peerClusterId) {
    if (this.peerClusters.size() == 0) {
      return new ArrayList<HServerAddress>(0);
    }
    ZooKeeperWrapper zkw = this.peerClusters.get(peerClusterId);
    return zkw == null?
        new ArrayList<HServerAddress>(0) : zkw.scanRSDirectory();
  }

  /**
   * This method connects this cluster to another one and registers it
   * in this region server's replication znode
   * @param peerId id of the peer cluster
   */
  private void connectToPeer(String peerId) throws IOException {
    String[] ensemble =
        Bytes.toString(this.zookeeperWrapper.getData(this.peersZNode, peerId)).
            split(":");
    if (ensemble.length != 3) {
      throw new IllegalArgumentException("Wrong format of cluster address: " +
          this.zookeeperWrapper.getData(this.peersZNode, peerId));
    }
    Configuration otherConf = new Configuration(this.conf);
    otherConf.set(HConstants.ZOOKEEPER_QUORUM, ensemble[0]);
    otherConf.set("hbase.zookeeper.property.clientPort", ensemble[1]);
    otherConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, ensemble[2]);
    ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(otherConf,
        "connection to cluster: " + peerId);
    zkw.registerListener(new ReplicationStatusWatcher());
    this.peerClusters.put(peerId, zkw);
    this.zookeeperWrapper.ensureExists(this.zookeeperWrapper.getZNode(
        this.rsServerNameZnode, peerId));
    LOG.info("Added new peer cluster " + StringUtils.arrayToString(ensemble));
  }

  /**
   * This reads the state znode for replication and sets the atomic boolean
   */
  private void setReplicating() {
    String value = Bytes.toString(this.zookeeperWrapper.getDataAndWatch(
        this.replicationZNode, this.replicationStateNodeName,
        new ReplicationStatusWatcher()));
    if (value != null) {
      this.replicating.set(value.equals("true"));
      LOG.info("Replication is now " + (this.replicating.get() ?
          "started" : "stopped"));
    }
  }

  /**
   * Add a new log to the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param clusterId name of the cluster's znode
   */
  public void addLogToList(String filename, String clusterId) {
    try {
      this.zookeeperWrapper.writeZNode(
          this.zookeeperWrapper.getZNode(
              this.rsServerNameZnode, clusterId), filename, "");
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (KeeperException e) {
      LOG.error(e);
    }
  }

  /**
   * Remove a log from the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param clusterId name of the cluster's znode
   */
  public void removeLogFromList(String filename, String clusterId) {
    try {
      this.zookeeperWrapper.deleteZNode(
          this.zookeeperWrapper.getZNode(this.rsServerNameZnode,
              this.zookeeperWrapper.getZNode(clusterId, filename)));
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (KeeperException e) {
      LOG.error(e);
    }
  }

  /**
   * Set the current position of the specified cluster in the current hlog
   * @param filename filename name of the hlog's znode
   * @param clusterId clusterId name of the cluster's znode
   * @param position the position in the file
   * @throws IOException
   */
  public void writeReplicationStatus(String filename, String clusterId,
                                     long position) {
    try {
      String clusterZNode = this.zookeeperWrapper.getZNode(
          this.rsServerNameZnode, clusterId);
      this.zookeeperWrapper.writeZNode(clusterZNode, filename,
          Long.toString(position));
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (KeeperException e) {
      LOG.error(e);
    }
  }

  /**
   * Get a list of all the other region servers in this cluster
   * and set a watch
   * @param watch the watch to set
   * @return a list of server nanes
   */
  public List<String> getRegisteredRegionServers(Watcher watch) {
    return this.zookeeperWrapper.listZnodes(
        this.zookeeperWrapper.getRsZNode(), watch);
  }

  /**
   * Get the list of the replicators that have queues, they can be alive, dead
   * or simply from a previous run
   * @param watch the watche to set
   * @return a list of server names
   */
  public List<String> getListOfReplicators(Watcher watch) {
    return this.zookeeperWrapper.listZnodes(rsZNode, watch);
  }

  /**
   * Get the list of peer clusters for the specified server names
   * @param rs server names of the rs
   * @param watch the watch to set
   * @return a list of peer cluster
   */
  public List<String> getListPeersForRS(String rs, Watcher watch) {
    return this.zookeeperWrapper.listZnodes(
        zookeeperWrapper.getZNode(rsZNode, rs), watch);
  }

  /**
   * Get the list of hlogs for the specified region server and peer cluster
   * @param rs server names of the rs
   * @param id peer cluster
   * @param watch the watch to set
   * @return a list of hlogs
   */
  public List<String> getListHLogsForPeerForRS(String rs, String id, Watcher watch) {
    return this.zookeeperWrapper.listZnodes(
        zookeeperWrapper.getZNode(zookeeperWrapper.getZNode(rsZNode, rs), id), watch);
  }

  /**
   * Try to set a lock in another server's znode.
   * @param znode the server names of the other server
   * @return true if the lock was acquired, false in every other cases
   */
  public boolean lockOtherRS(String znode) {
    try {
      this.zookeeperWrapper.writeZNode(
          this.zookeeperWrapper.getZNode(this.rsZNode, znode),
          RS_LOCK_ZNODE, rsServerNameZnode, true);

    } catch (InterruptedException e) {
      LOG.error(e);
      return false;
    } catch (KeeperException e) {
      LOG.debug("Won't lock " + znode + " because " + e.getMessage());
      // TODO see if the other still exists!!
      return false;
    }
    return true;
  }

  /**
   * This methods copies all the hlogs queues from another region server
   * and returns them all sorted per peer cluster (appended with the dead
   * server's znode)
   * @param znode server names to copy
   * @return all hlogs for all peers of that cluster, null if an error occurred
   */
  public SortedMap<String, SortedSet<String>> copyQueuesFromRS(String znode) {
    // TODO this method isn't atomic enough, we could start copying and then
    // TODO fail for some reason and we would end up with znodes we don't want.
    SortedMap<String,SortedSet<String>> queues =
        new TreeMap<String,SortedSet<String>>();
    try {
      String nodePath = this.zookeeperWrapper.getZNode(rsZNode, znode);
      List<String> clusters = this.zookeeperWrapper.listZnodes(nodePath, null);
      // We have a lock znode in there, it will count as one.
      if (clusters == null || clusters.size() <= 1) {
        return queues;
      }
      // The lock isn't a peer cluster, remove it
      clusters.remove(RS_LOCK_ZNODE);
      for (String cluster : clusters) {
        // We add the name of the recovered RS to the new znode, we can even
        // do that for queues that were recovered 10 times giving a znode like
        // number-startcode-number-otherstartcode-number-anotherstartcode-etc
        String newCluster = cluster+"-"+znode;
        String newClusterZnode =
            this.zookeeperWrapper.getZNode(rsServerNameZnode, newCluster);
        this.zookeeperWrapper.ensureExists(newClusterZnode);
        String clusterPath = this.zookeeperWrapper.getZNode(nodePath, cluster);
        List<String> hlogs = this.zookeeperWrapper.listZnodes(clusterPath, null);
        // That region server didn't have anything to replicate for this cluster
        if (hlogs == null || hlogs.size() == 0) {
          continue;
        }
        SortedSet<String> logQueue = new TreeSet<String>();
        queues.put(newCluster, logQueue);
        for (String hlog : hlogs) {
          String position = Bytes.toString(
              this.zookeeperWrapper.getData(clusterPath, hlog));
          LOG.debug("Creating " + hlog + " with data " + position);
          this.zookeeperWrapper.writeZNode(newClusterZnode, hlog, position);
          logQueue.add(hlog);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn(e);
      return null;
    } catch (KeeperException e) {
      LOG.warn(e);
      return null;
    }
    return queues;
  }

  /**
   * Delete a complete queue of hlogs
   * @param peerZnode znode of the peer cluster queue of hlogs to delete
   */
  public void deleteSource(String peerZnode) {
    try {
      this.zookeeperWrapper.deleteZNode(
          this.zookeeperWrapper.getZNode(rsServerNameZnode, peerZnode), true);
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (KeeperException e) {
      LOG.error(e);
    }
  }

  /**
   * Recursive deletion of all znodes in specified rs' znode
   * @param znode
   */
  public void deleteRsQueues(String znode) {
    try {
      this.zookeeperWrapper.deleteZNode(
          this.zookeeperWrapper.getZNode(rsZNode, znode), true);
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (KeeperException e) {
      LOG.error(e);
    }
  }

  /**
   * Delete this cluster's queues
   */
  public void deleteOwnRSZNode() {
    deleteRsQueues(this.rsServerNameZnode);
  }

  /**
   * Get the position of the specified hlog in the specified peer znode
   * @param peerId znode of the peer cluster
   * @param hlog name of the hlog
   * @return the position in that hlog
   */
  public long getHLogRepPosition(String peerId, String hlog) {
    String clusterZnode =
        this.zookeeperWrapper.getZNode(rsServerNameZnode, peerId);
    String data = Bytes.toString(
        this.zookeeperWrapper.getData(clusterZnode, hlog));
    return data == null || data.length() == 0 ? 0 : Long.parseLong(data);
  }

  /**
   * Tells if this cluster replicates or not
   *
   * @return if this is a master
   */
  public boolean isReplicationMaster() {
    return this.replicationMaster;
  }

  /**
   * Get the identification of the cluster
   *
   * @return the id for the cluster
   */
  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * Get a map of all peer clusters
   * @return map of peer cluster, zk address to ZKW
   */
  public Map<String, ZooKeeperWrapper> getPeerClusters() {
    return this.peerClusters;
  }

  /**
   * Watcher for the status of the replication
   */
  public class ReplicationStatusWatcher implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
      Event.EventType type = watchedEvent.getType();
      LOG.info("Got event " + type + " with path " + watchedEvent.getPath());
      if (type.equals(Event.EventType.NodeDataChanged)) {
        setReplicating();
      }
    }
  }

}
