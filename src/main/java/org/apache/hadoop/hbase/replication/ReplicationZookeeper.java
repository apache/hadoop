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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

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
public class ReplicationZookeeper {
  private static final Log LOG =
    LogFactory.getLog(ReplicationZookeeper.class);
  // Name of znode we use to lock when failover
  private final static String RS_LOCK_ZNODE = "lock";
  // Our handle on zookeeper
  private final ZooKeeperWatcher zookeeper;
  // Map of addresses of peer clusters with their ZKW
  private final Map<String, ReplicationZookeeper> peerClusters;
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
  // Abortable
  private final Abortable abortable;

  /**
   * Constructor used by region servers, connects to the peer cluster right away.
   *
   * @param zookeeper
   * @param replicating    atomic boolean to start/stop replication
   * @throws IOException
   * @throws KeeperException 
   */
  public ReplicationZookeeper(final Server server, final AtomicBoolean replicating)
  throws IOException, KeeperException {
    this.abortable = server;
    this.zookeeper = server.getZooKeeper();
    this.conf = server.getConfiguration();
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

    this.peerClusters = new HashMap<String, ReplicationZookeeper>();
    this.replicationZNode =
      ZKUtil.joinZNode(this.zookeeper.baseZNode, replicationZNodeName);
    this.peersZNode = ZKUtil.joinZNode(replicationZNode, peersZNodeName);
    this.rsZNode = ZKUtil.joinZNode(replicationZNode, rsZNodeName);

    this.replicating = replicating;
    setReplicating();
    String znode = ZKUtil.joinZNode(this.replicationZNode, clusterIdZNodeName);
    byte [] data = ZKUtil.getData(this.zookeeper, znode);
    String idResult = Bytes.toString(data);
    this.clusterId = idResult == null?
      Byte.toString(HConstants.DEFAULT_CLUSTER_ID): idResult;

    znode = ZKUtil.joinZNode(this.replicationZNode, repMasterZNodeName);
    data = ZKUtil.getData(this.zookeeper, znode);
    String address = Bytes.toString(data);
    this.replicationMaster = thisCluster.equals(address);
    LOG.info("This cluster (" + thisCluster + ") is a " +
      (this.replicationMaster ? "master" : "slave") + " for replication" +
        ", compared with (" + address + ")");

    if (server.getServerName() != null) {
      this.rsServerNameZnode = ZKUtil.joinZNode(rsZNode, server.getServerName());
      // Set a tracker on replicationStateNodeNode
      ReplicationStatusTracker tracker =
        new ReplicationStatusTracker(this.zookeeper, getRepStateNode(), server);
      tracker.start();

      List<String> znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
      if (znodes != null) {
        for (String z : znodes) {
          connectToPeer(z);
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
    ReplicationZookeeper zkw = this.peerClusters.get(peerClusterId);
    return zkw == null?
      new ArrayList<HServerAddress>(0):
      zkw.scanAddressDirectory(this.zookeeper.rsZNode);
  }

  /**
   * Scan a directory of address data.
   * @param znode The parent node
   * @return The directory contents as HServerAddresses
   */
  public List<HServerAddress> scanAddressDirectory(String znode) {
    List<HServerAddress> list = new ArrayList<HServerAddress>();
    List<String> nodes = null;
    try {
      nodes = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Scanning " + znode, e);
    }
    if (nodes == null) {
      return list;
    }
    for (String node : nodes) {
      String path = ZKUtil.joinZNode(znode, node);
      list.add(readAddress(path));
    }
    return list;
  }

  private HServerAddress readAddress(String znode) {
    byte [] data = null;
    try {
      data = ZKUtil.getData(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Getting address", e);
    }
    return new HServerAddress(Bytes.toString(data));
  }

  /**
   * This method connects this cluster to another one and registers it
   * in this region server's replication znode
   * @param peerId id of the peer cluster
   * @throws KeeperException 
   */
  private void connectToPeer(String peerId) throws IOException, KeeperException {
    String znode = ZKUtil.joinZNode(this.peersZNode, peerId);
    byte [] data = ZKUtil.getData(this.zookeeper, znode);
    String [] ensemble = Bytes.toString(data).split(":");
    if (ensemble.length != 3) {
      throw new IllegalArgumentException("Wrong format of cluster address: " +
        Bytes.toStringBinary(data));
    }
    Configuration otherConf = new Configuration(this.conf);
    otherConf.set(HConstants.ZOOKEEPER_QUORUM, ensemble[0]);
    otherConf.set("hbase.zookeeper.property.clientPort", ensemble[1]);
    otherConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, ensemble[2]);
    // REENABLE -- FIX!!!!
    /*
    ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(otherConf,
        "connection to cluster: " + peerId);
    zkw.registerListener(new ReplicationStatusWatcher());
    this.peerClusters.put(peerId, zkw);
    this.zookeeperWrapper.ensureExists(this.zookeeperWrapper.getZNode(
        this.rsServerNameZnode, peerId));
        */
    LOG.info("Added new peer cluster " + StringUtils.arrayToString(ensemble));
  }

  /**
   * This reads the state znode for replication and sets the atomic boolean
   */
  private void setReplicating() {
    try {
      byte [] data = ZKUtil.getDataAndWatch(this.zookeeper, getRepStateNode());
      String value = Bytes.toString(data);
      if (value == null) LOG.info(getRepStateNode() + " data is null");
      else {
        this.replicating.set(Boolean.parseBoolean(value));
        LOG.info("Replication is now " + (this.replicating.get()?
          "started" : "stopped"));
      }
    } catch (KeeperException e) {
      this.abortable.abort("Failed getting data on from " + getRepStateNode(), e);
    }
  }

  private String getRepStateNode() {
    return ZKUtil.joinZNode(this.replicationZNode, this.replicationStateNodeName);
  }

  /**
   * Add a new log to the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param clusterId name of the cluster's znode
   */
  public void addLogToList(String filename, String clusterId) {
    try {
      String znode = ZKUtil.joinZNode(this.rsServerNameZnode, clusterId);
      znode = ZKUtil.joinZNode(znode, filename);
      ZKUtil.createAndWatch(this.zookeeper, znode, Bytes.toBytes(""));
    } catch (KeeperException e) {
      this.abortable.abort("Failed add log to list", e);
    }
  }

  /**
   * Remove a log from the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param clusterId name of the cluster's znode
   */
  public void removeLogFromList(String filename, String clusterId) {
    try {
      String znode = ZKUtil.joinZNode(rsServerNameZnode, clusterId);
      znode = ZKUtil.joinZNode(znode, filename);
      ZKUtil.deleteChildrenRecursively(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed remove from list", e);
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
      String znode = ZKUtil.joinZNode(this.rsServerNameZnode, clusterId);
      znode = ZKUtil.joinZNode(znode, filename);
      // Why serialize String of Long and note Long as bytes?
      ZKUtil.createAndWatch(this.zookeeper, znode,
        Bytes.toBytes(Long.toString(position)));
    } catch (KeeperException e) {
      this.abortable.abort("Writing replication status", e);
    }
  }

  /**
   * Get a list of all the other region servers in this cluster
   * and set a watch
   * @param watch the watch to set
   * @return a list of server nanes
   */
  public List<String> getRegisteredRegionServers(Watcher watch) {
    List<String> result = null;
    try {
      // TODO: This is rsZNode from zk which is like getListOfReplicators
      // but maybe these are from different zk instances?
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, rsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of registered region servers", e);
    }
    return result;
  }

  /**
   * Get the list of the replicators that have queues, they can be alive, dead
   * or simply from a previous run
   * @param watch the watche to set
   * @return a list of server names
   */
  public List<String> getListOfReplicators() {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, rsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of replicators", e);
    }
    return result;
  }

  /**
   * Get the list of peer clusters for the specified server names
   * @param rs server names of the rs
   * @param watch the watch to set
   * @return a list of peer cluster
   */
  public List<String> getListPeersForRS(String rs) {
    String znode = ZKUtil.joinZNode(rsZNode, rs);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of peers for rs", e);
    }
    return result;
  }

  /**
   * Get the list of hlogs for the specified region server and peer cluster
   * @param rs server names of the rs
   * @param id peer cluster
   * @param watch the watch to set
   * @return a list of hlogs
   */
  public List<String> getListHLogsForPeerForRS(String rs, String id) {
    String znode = ZKUtil.joinZNode(rsZNode, rs);
    znode = ZKUtil.joinZNode(znode, id);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of hlogs for peer", e);
    }
    return result;
  }

  /**
   * Try to set a lock in another server's znode.
   * @param znode the server names of the other server
   * @return true if the lock was acquired, false in every other cases
   */
  public boolean lockOtherRS(String znode) {
    try {
      String parent = ZKUtil.joinZNode(this.rsZNode, znode);
      String p = ZKUtil.joinZNode(parent, RS_LOCK_ZNODE);
      ZKUtil.createAndWatch(this.zookeeper, p, Bytes.toBytes(rsServerNameZnode));
    } catch (KeeperException e) {
      this.abortable.abort("Failed lock other rs", e);
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
      String nodePath = ZKUtil.joinZNode(rsZNode, znode);
      List<String> clusters =
        ZKUtil.listChildrenNoWatch(this.zookeeper, nodePath);
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
        String newClusterZnode = ZKUtil.joinZNode(rsServerNameZnode, newCluster);
        ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, newClusterZnode,
          HConstants.EMPTY_BYTE_ARRAY);
        String clusterPath = ZKUtil.joinZNode(nodePath, cluster);
        List<String> hlogs = ZKUtil.listChildrenNoWatch(this.zookeeper, clusterPath);
        // That region server didn't have anything to replicate for this cluster
        if (hlogs == null || hlogs.size() == 0) {
          continue;
        }
        SortedSet<String> logQueue = new TreeSet<String>();
        queues.put(newCluster, logQueue);
        for (String hlog : hlogs) {
          String z = ZKUtil.joinZNode(clusterPath, hlog);
          byte [] position = ZKUtil.getData(this.zookeeper, z);
          LOG.debug("Creating " + hlog + " with data " + Bytes.toString(position));
          String child = ZKUtil.joinZNode(newClusterZnode, hlog);
          ZKUtil.createAndWatch(this.zookeeper, child, position);
          logQueue.add(hlog);
        }
      }
    } catch (KeeperException e) {
      this.abortable.abort("Copy queues from rs", e);
    }
    return queues;
  }

  /**
   * Delete a complete queue of hlogs
   * @param peerZnode znode of the peer cluster queue of hlogs to delete
   */
  public void deleteSource(String peerZnode) {
    try {
      ZKUtil.deleteChildrenRecursively(this.zookeeper,
          ZKUtil.joinZNode(rsServerNameZnode, peerZnode));
    } catch (KeeperException e) {
      this.abortable.abort("Failed delete of " + peerZnode, e);
    }
  }

  /**
   * Recursive deletion of all znodes in specified rs' znode
   * @param znode
   */
  public void deleteRsQueues(String znode) {
    try {
      ZKUtil.deleteChildrenRecursively(this.zookeeper,
          ZKUtil.joinZNode(rsZNode, znode));
    } catch (KeeperException e) {
      this.abortable.abort("Failed delete of " + znode, e);
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
   * @throws KeeperException 
   */
  public long getHLogRepPosition(String peerId, String hlog)
  throws KeeperException {
    String clusterZnode = ZKUtil.joinZNode(rsServerNameZnode, peerId);
    String znode = ZKUtil.joinZNode(clusterZnode, hlog);
    String data = Bytes.toString(ZKUtil.getData(this.zookeeper, znode));
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
  public Map<String, ReplicationZookeeper> getPeerClusters() {
    return this.peerClusters;
  }

  /**
   * Tracker for status of the replication
   */
  public class ReplicationStatusTracker extends ZooKeeperNodeTracker {
    public ReplicationStatusTracker(ZooKeeperWatcher watcher, String node,
        Abortable abortable) {
      super(watcher, node, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      super.nodeDataChanged(path);
      setReplicating();
    }
  }
}