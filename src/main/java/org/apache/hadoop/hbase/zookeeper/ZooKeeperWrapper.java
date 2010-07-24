/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Wraps a ZooKeeper instance and adds HBase specific functionality.
 *
 * This class provides methods to:
 * - read/write/delete the root region location in ZooKeeper.
 * - set/check out of safe mode flag.
 *
 * ------------------------------------------
 * The following STATIC ZNodes are created:
 * ------------------------------------------
 * - parentZNode     : All the HBase directories are hosted under this parent
 *                     node, default = "/hbase"
 * - rsZNode         : This is the directory where the RS's create ephemeral
 *                     nodes. The master watches these nodes, and their expiry
 *                     indicates RS death. The default location is "/hbase/rs"
 *
 * ------------------------------------------
 * The following DYNAMIC ZNodes are created:
 * ------------------------------------------
 * - rootRegionZNode     : Specifies the RS hosting root.
 * - masterElectionZNode : ZNode used for election of the primary master when
 *                         there are secondaries. All the masters race to write
 *                         their addresses into this location, the one that
 *                         succeeds is the primary. Others block.
 * - clusterStateZNode   : Determines if the cluster is running. Its default
 *                         location is "/hbase/shutdown". It always has a value
 *                         of "up". If present with the valus, cluster is up
 *                         and running. If deleted, the cluster is shutting
 *                         down.
 * - rgnsInTransitZNode  : All the nodes under this node are names of regions
 *                         in transition. The first byte of the data for each
 *                         of these nodes is the event type. This is used to
 *                         deserialize the rest of the data.
 */
public class ZooKeeperWrapper implements Watcher {
  protected static final Log LOG = LogFactory.getLog(ZooKeeperWrapper.class);

  // instances of the watcher
  private static Map<String,ZooKeeperWrapper> INSTANCES =
    new HashMap<String,ZooKeeperWrapper>();
  // lock for ensuring a singleton per instance type
  private static Lock createLock = new ReentrantLock();
  // name of this instance
  private String instanceName;

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  private static final char ZNODE_PATH_SEPARATOR = '/';

  private String quorumServers = null;
  private final int sessionTimeout;
  private ZooKeeper zooKeeper;

  /*
   * All the HBase directories are hosted under this parent
   */
  public final String parentZNode;
  /*
   * Specifies the RS hosting root
   */
  private final String rootRegionZNode;
  /*
   * This is the directory where the RS's create ephemeral nodes. The master
   * watches these nodes, and their expiry indicates RS death.
   */
  private final String rsZNode;
  /*
   * ZNode used for election of the primary master when there are secondaries.
   */
  private final String masterElectionZNode;
  /*
   * State of the cluster - if up and running or shutting down
   */
  public final String clusterStateZNode;
  /*
   * Regions that are in transition
   */
  private final String rgnsInTransitZNode;
  /*
   * List of ZNodes in the unassgined region that are already being watched
   */
  private Set<String> unassignedZNodesWatched = new HashSet<String>();

  private List<Watcher> listeners = new ArrayList<Watcher>();

  // return the singleton given the name of the instance
  public static ZooKeeperWrapper getInstance(Configuration conf, String name) {
    name = getZookeeperClusterKey(conf, name);
    return INSTANCES.get(name);
  }
  // creates only one instance
  public static ZooKeeperWrapper createInstance(Configuration conf, String name) {
    if (getInstance(conf, name) != null) {
      return getInstance(conf, name);
    }
    ZooKeeperWrapper.createLock.lock();
    try {
      if (getInstance(conf, name) == null) {
        try {
          String fullname = getZookeeperClusterKey(conf, name);
          ZooKeeperWrapper instance = new ZooKeeperWrapper(conf, fullname);
          INSTANCES.put(fullname, instance);
        }
        catch (Exception e) {
          LOG.error("<" + name + ">" + "Error creating a ZooKeeperWrapper " + e);
        }
      }
    }
    finally {
      createLock.unlock();
    }
    return getInstance(conf, name);
  }

  /**
   * Create a ZooKeeperWrapper. The Zookeeper wrapper listens to all messages
   * from Zookeeper, and notifies all the listeners about all the messages. Any
   * component can subscribe to these messages by adding itself as a listener,
   * and remove itself from being a listener.
   *
   * @param conf HBaseConfiguration to read settings from.
   * @throws IOException If a connection error occurs.
   */
  private ZooKeeperWrapper(Configuration conf, String instanceName)
  throws IOException {
    this.instanceName = instanceName;
    Properties properties = HQuorumPeer.makeZKProps(conf);
    quorumServers = HQuorumPeer.getZKQuorumServersString(properties);
    if (quorumServers == null) {
      throw new IOException("Could not read quorum servers from " +
                            HConstants.ZOOKEEPER_CONFIG_NAME);
    }
    sessionTimeout = conf.getInt("zookeeper.session.timeout", 60 * 1000);
    reconnectToZk();

    parentZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);

    String rootServerZNodeName = conf.get("zookeeper.znode.rootserver", "root-region-server");
    String rsZNodeName         = conf.get("zookeeper.znode.rs", "rs");
    String masterAddressZNodeName = conf.get("zookeeper.znode.master", "master");
    String stateZNodeName      = conf.get("zookeeper.znode.state", "shutdown");
    String regionsInTransitZNodeName = conf.get("zookeeper.znode.regionInTransition", "UNASSIGNED");

    rootRegionZNode     = getZNode(parentZNode, rootServerZNodeName);
    rsZNode             = getZNode(parentZNode, rsZNodeName);
    rgnsInTransitZNode  = getZNode(parentZNode, regionsInTransitZNodeName);
    masterElectionZNode = getZNode(parentZNode, masterAddressZNodeName);
    clusterStateZNode   = getZNode(parentZNode, stateZNodeName);
  }

  public void reconnectToZk() throws IOException {
    try {
      LOG.info("Reconnecting to zookeeper");
      if(zooKeeper != null) {
        zooKeeper.close();
        LOG.debug("<" + instanceName + ">" + "Closed existing zookeeper client");
      }
      zooKeeper = new ZooKeeper(quorumServers, sessionTimeout, this);
      LOG.debug("<" + instanceName + ">" + "Connected to zookeeper again");
    } catch (IOException e) {
      LOG.error("<" + instanceName + ">" + "Failed to create ZooKeeper object: " + e);
      throw new IOException(e);
    } catch (InterruptedException e) {
      LOG.error("<" + instanceName + ">" + "Error closing ZK connection: " + e);
      throw new IOException(e);
    }
  }

  public synchronized void registerListener(Watcher watcher) {
    listeners.add(watcher);
  }

  public synchronized void unregisterListener(Watcher watcher) {
    listeners.remove(watcher);
  }

  /**
   * This is the primary ZK watcher
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public synchronized void process(WatchedEvent event) {
    for(Watcher w : listeners) {
      try {
        w.process(event);
      } catch (Throwable t) {
        LOG.error("<"+instanceName+">" + "ZK updates listener threw an exception in process()", t);
      }
    }
  }

  /** @return String dump of everything in ZooKeeper. */
  @SuppressWarnings({"ConstantConditions"})
  public String dump() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nHBase tree in ZooKeeper is rooted at ").append(parentZNode);
    sb.append("\n  Cluster up? ").append(exists(clusterStateZNode, true));
    sb.append("\n  Master address: ").append(readMasterAddress(null));
    sb.append("\n  Region server holding ROOT: ").append(readRootRegionLocation());
    sb.append("\n  Region servers:");
    for (HServerAddress address : scanRSDirectory()) {
      sb.append("\n    - ").append(address);
    }
    sb.append("\n  Quorum Server Statistics:");
    String[] servers = quorumServers.split(",");
    for (String server : servers) {
      sb.append("\n    - ").append(server);
      try {
        String[] stat = getServerStats(server);
        for (String s : stat) {
          sb.append("\n        ").append(s);
        }
      } catch (Exception e) {
        sb.append("\n        ERROR: ").append(e.getMessage());
      }
    }
    return sb.toString();
  }

  /**
   * Gets the statistics from the given server. Uses a 1 minute timeout.
   *
   * @param server  The server to get the statistics from.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public String[] getServerStats(String server)
  throws IOException {
    return getServerStats(server, 60 * 1000);
  }

  /**
   * Gets the statistics from the given server.
   *
   * @param server  The server to get the statistics from.
   * @param timeout  The socket timeout to use.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public String[] getServerStats(String server, int timeout)
  throws IOException {
    String[] sp = server.split(":");
    Socket socket = new Socket(sp[0],
      sp.length > 1 ? Integer.parseInt(sp[1]) : 2181);
    socket.setSoTimeout(timeout);
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    BufferedReader in = new BufferedReader(new InputStreamReader(
      socket.getInputStream()));
    out.println("stat");
    out.flush();
    ArrayList<String> res = new ArrayList<String>();
    while (true) {
      String line = in.readLine();
      if (line != null) res.add(line);
      else break;
    }
    socket.close();
    return res.toArray(new String[res.size()]);
  }

  public boolean exists(String znode, boolean watch) {
    try {
      return zooKeeper.exists(getZNode(parentZNode, znode), watch?this:null) != null;
    } catch (KeeperException.SessionExpiredException e) {
      // if the session has expired try to reconnect to ZK, then perform query
      try {
        // TODO: ZK-REFACTOR: We should not reconnect - we should just quit and restart.
        reconnectToZk();
        return zooKeeper.exists(getZNode(parentZNode, znode), watch?this:null) != null;
      } catch (IOException e1) {
        LOG.error("Error reconnecting to zookeeper", e1);
        throw new RuntimeException("Error reconnecting to zookeeper", e1);
      } catch (KeeperException e1) {
        LOG.error("Error reading after reconnecting to zookeeper", e1);
        throw new RuntimeException("Error reading after reconnecting to zookeeper", e1);
      } catch (InterruptedException e1) {
        LOG.error("Error reading after reconnecting to zookeeper", e1);
        throw new RuntimeException("Error reading after reconnecting to zookeeper", e1);
      }
    } catch (KeeperException e) {
      return false;
    } catch (InterruptedException e) {
      return false;
    }
  }

  /** @return ZooKeeper used by this wrapper. */
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

  /**
   * This is for testing KeeperException.SessionExpiredException.
   * See HBASE-1232.
   * @return long session ID of this ZooKeeper session.
   */
  public long getSessionID() {
    return zooKeeper.getSessionId();
  }

  /**
   * This is for testing KeeperException.SessionExpiredException.
   * See HBASE-1232.
   * @return byte[] password of this ZooKeeper session.
   */
  public byte[] getSessionPassword() {
    return zooKeeper.getSessionPasswd();
  }

  /** @return host:port list of quorum servers. */
  public String getQuorumServers() {
    return quorumServers;
  }

  /** @return true if currently connected to ZooKeeper, false otherwise. */
  public boolean isConnected() {
    return zooKeeper.getState() == States.CONNECTED;
  }

  /**
   * Read location of server storing root region.
   * @return HServerAddress pointing to server serving root region or null if
   *         there was a problem reading the ZNode.
   */
  public HServerAddress readRootRegionLocation() {
    return readAddress(rootRegionZNode, null);
  }

  /**
   * Read address of master server.
   * @return HServerAddress of master server.
   * @throws IOException if there's a problem reading the ZNode.
   */
  public HServerAddress readMasterAddressOrThrow() throws IOException {
    return readAddressOrThrow(masterElectionZNode, null);
  }

  /**
   * Read master address and set a watch on it.
   * @param watcher Watcher to set on master address ZNode if not null.
   * @return HServerAddress of master or null if there was a problem reading the
   *         ZNode. The watcher is set only if the result is not null.
   */
  public HServerAddress readMasterAddress(Watcher watcher) {
    return readAddress(masterElectionZNode, watcher);
  }

  /**
   * Watch the state of the cluster, up or down
   * @param watcher Watcher to set on cluster state node
   */
  public void setClusterStateWatch() {
    try {
      zooKeeper.exists(clusterStateZNode, this);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to check on ZNode " + clusterStateZNode, e);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to check on ZNode " + clusterStateZNode, e);
    }
  }

  /**
   * Set the cluster state, up or down
   * @param up True to write the node, false to delete it
   * @return true if it worked, else it's false
   */
  public boolean setClusterState(boolean up) {
    if (!ensureParentExists(clusterStateZNode)) {
      return false;
    }
    try {
      if(up) {
        byte[] data = Bytes.toBytes("up");
        zooKeeper.create(clusterStateZNode, data,
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOG.debug("<" + instanceName + ">" + "State node wrote in ZooKeeper");
      } else {
        zooKeeper.delete(clusterStateZNode, -1);
        LOG.debug("<" + instanceName + ">" + "State node deleted in ZooKeeper");
      }
      return true;
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to set state node in ZooKeeper", e);
    } catch (KeeperException e) {
      if(e.code() == KeeperException.Code.NODEEXISTS) {
        LOG.debug("<" + instanceName + ">" + "State node exists.");
      } else {
        LOG.warn("<" + instanceName + ">" + "Failed to set state node in ZooKeeper", e);
      }
    }

    return false;
  }

  /**
   * Set a watcher on the master address ZNode. The watcher will be set unless
   * an exception occurs with ZooKeeper.
   * @param watcher Watcher to set on master address ZNode.
   * @return true if watcher was set, false otherwise.
   */
  public boolean watchMasterAddress(Watcher watcher) {
    try {
      zooKeeper.exists(masterElectionZNode, watcher);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to set watcher on ZNode " + masterElectionZNode, e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to set watcher on ZNode " + masterElectionZNode, e);
      return false;
    }
    LOG.debug("<" + instanceName + ">" + "Set watcher on master address ZNode " + masterElectionZNode);
    return true;
  }

  private HServerAddress readAddress(String znode, Watcher watcher) {
    try {
      LOG.debug("<" + instanceName + ">" + "Trying to read " + znode);
      return readAddressOrThrow(znode, watcher);
    } catch (IOException e) {
      LOG.debug("<" + instanceName + ">" + "Failed to read " + e.getMessage());
      return null;
    }
  }

  private HServerAddress readAddressOrThrow(String znode, Watcher watcher) throws IOException {
    byte[] data;
    try {
      data = zooKeeper.getData(znode, watcher, null);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }

    String addressString = Bytes.toString(data);
    LOG.debug("<" + instanceName + ">" + "Read ZNode " + znode + " got " + addressString);
    return new HServerAddress(addressString);
  }

  /**
   * Make sure this znode exists by creating it if it's missing
   * @param znode full path to znode
   * @return true if it works
   */
  public boolean ensureExists(final String znode) {
    try {
      Stat stat = zooKeeper.exists(znode, false);
      if (stat != null) {
        return true;
      }
      zooKeeper.create(znode, new byte[0],
                       Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.debug("<" + instanceName + ">" + "Created ZNode " + znode);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      return true;      // ok, move on.
    } catch (KeeperException.NoNodeException e) {
      return ensureParentExists(znode) && ensureExists(znode);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create " + znode +
        " -- check quorum servers, currently=" + this.quorumServers, e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create " + znode +
        " -- check quorum servers, currently=" + this.quorumServers, e);
    }
    return false;
  }

  private boolean ensureParentExists(final String znode) {
    int index = znode.lastIndexOf(ZNODE_PATH_SEPARATOR);
    if (index <= 0) {   // Parent is root, which always exists.
      return true;
    }
    return ensureExists(znode.substring(0, index));
  }

  /**
   * Delete ZNode containing root region location.
   * @return true if operation succeeded, false otherwise.
   */
  public boolean deleteRootRegionLocation()  {
    if (!ensureParentExists(rootRegionZNode)) {
      return false;
    }

    try {
      deleteZNode(rootRegionZNode);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return true;    // ok, move on.
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to delete " + rootRegionZNode + ": " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to delete " + rootRegionZNode + ": " + e);
    }

    return false;
  }

  /**
   * Unrecursive deletion of specified znode
   * @param znode
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void deleteZNode(String znode)
      throws KeeperException, InterruptedException {
    deleteZNode(znode, false);
  }

  /**
   * Optionnally recursive deletion of specified znode
   * @param znode
   * @param recursive
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void deleteZNode(String znode, boolean recursive)
    throws KeeperException, InterruptedException {
    if (recursive) {
      LOG.info("<" + instanceName + ">" + "deleteZNode get children for " + znode);
      List<String> znodes = this.zooKeeper.getChildren(znode, false);
      if (znodes != null && znodes.size() > 0) {
        for (String child : znodes) {
          String childFullPath = getZNode(znode, child);
          LOG.info("<" + instanceName + ">" + "deleteZNode recursive call " + childFullPath);
          this.deleteZNode(childFullPath, true);
        }
      }
    }
    this.zooKeeper.delete(znode, -1);
    LOG.debug("<" + instanceName + ">" + "Deleted ZNode " + znode);
  }

  private boolean createRootRegionLocation(String address) {
    byte[] data = Bytes.toBytes(address);
    try {
      zooKeeper.create(rootRegionZNode, data, Ids.OPEN_ACL_UNSAFE,
                       CreateMode.PERSISTENT);
      LOG.debug("<" + instanceName + ">" + "Created ZNode " + rootRegionZNode + " with data " + address);
      return true;
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create root region in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create root region in ZooKeeper: " + e);
    }

    return false;
  }

  private boolean updateRootRegionLocation(String address) {
    byte[] data = Bytes.toBytes(address);
    try {
      zooKeeper.setData(rootRegionZNode, data, -1);
      LOG.debug("<" + instanceName + ">" + "SetData of ZNode " + rootRegionZNode + " with " + address);
      return true;
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to set root region location in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to set root region location in ZooKeeper: " + e);
    }

    return false;
  }

  /**
   * Write root region location to ZooKeeper. If address is null, delete ZNode.
   * containing root region location.
   * @param address HServerAddress to write to ZK.
   * @return true if operation succeeded, false otherwise.
   */
  public boolean writeRootRegionLocation(HServerAddress address) {
    if (address == null) {
      return deleteRootRegionLocation();
    }

    if (!ensureParentExists(rootRegionZNode)) {
      return false;
    }

    String addressString = address.toString();

    if (checkExistenceOf(rootRegionZNode)) {
      return updateRootRegionLocation(addressString);
    }

    return createRootRegionLocation(addressString);
  }

  /**
   * Write address of master to ZooKeeper.
   * @param address HServerAddress of master.
   * @return true if operation succeeded, false otherwise.
   */
  public boolean writeMasterAddress(final HServerAddress address) {
    LOG.debug("<" + instanceName + ">" + "Writing master address " + address.toString() + " to znode " + masterElectionZNode);
    if (!ensureParentExists(masterElectionZNode)) {
      return false;
    }
    LOG.debug("<" + instanceName + ">" + "Znode exists : " + masterElectionZNode);

    String addressStr = address.toString();
    byte[] data = Bytes.toBytes(addressStr);
    try {
      zooKeeper.create(masterElectionZNode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("<" + instanceName + ">" + "Wrote master address " + address + " to ZooKeeper");
      return true;
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to write master address " + address + " to ZooKeeper", e);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to write master address " + address + " to ZooKeeper", e);
    }

    return false;
  }

  /**
   * Write in ZK this RS startCode and address.
   * Ensures that the full path exists.
   * @param info The RS info
   * @return true if the location was written, false if it failed
   */
  public boolean writeRSLocation(HServerInfo info) {
    ensureExists(rsZNode);
    byte[] data = Bytes.toBytes(info.getServerAddress().toString());
    String znode = joinPath(rsZNode, info.getServerName());
    try {
      zooKeeper.create(znode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("<" + instanceName + ">" + "Created ZNode " + znode
          + " with data " + info.getServerAddress().toString());
      return true;
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create " + znode + " znode in ZooKeeper: " + e);
    }
    return false;
  }

  /**
   * Update the RS address and set a watcher on the znode
   * @param info The RS info
   * @param watcher The watcher to put on the znode
   * @return true if the update is done, false if it failed
   */
  public boolean updateRSLocationGetWatch(HServerInfo info, Watcher watcher) {
    byte[] data = Bytes.toBytes(info.getServerAddress().toString());
    String znode = rsZNode + ZNODE_PATH_SEPARATOR + info.getServerName();
    try {
      zooKeeper.setData(znode, data, -1);
      LOG.debug("<" + instanceName + ">" + "Updated ZNode " + znode
          + " with data " + info.getServerAddress().toString());
      zooKeeper.getData(znode, watcher, null);
      return true;
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to update " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to update " + znode + " znode in ZooKeeper: " + e);
    }

    return false;
  }

  /**
   * Scans the regions servers directory
   * @return A list of server addresses
   */
  public List<HServerAddress> scanRSDirectory() {
    return scanAddressDirectory(rsZNode, null);
  }

  /**
   * Scans the regions servers directory and sets a watch on each znode
   * @param watcher a watch to use for each znode
   * @return A list of server addresses
   */
  public List<HServerAddress> scanRSDirectory(Watcher watcher) {
    return scanAddressDirectory(rsZNode, watcher);
  }

  /**
   * Method used to make sure the region server directory is empty.
   *
   */
  public void clearRSDirectory() {
    try {
      List<String> nodes = zooKeeper.getChildren(rsZNode, false);
      for (String node : nodes) {
        LOG.debug("<" + instanceName + ">" + "Deleting node: " + node);
        zooKeeper.delete(joinPath(this.rsZNode, node), -1);
      }
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to delete " + rsZNode + " znodes in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to delete " + rsZNode + " znodes in ZooKeeper: " + e);
    }
  }

  /**
   * @return the number of region server znodes in the RS directory
   */
  public int getRSDirectoryCount() {
    Stat stat = null;
    try {
      stat = zooKeeper.exists(rsZNode, false);
    } catch (KeeperException e) {
      LOG.warn("Problem getting stats for " + rsZNode, e);
    } catch (InterruptedException e) {
      LOG.warn("Problem getting stats for " + rsZNode, e);
    }
    return (stat != null) ? stat.getNumChildren() : 0;
  }

  private boolean checkExistenceOf(String path) {
    Stat stat = null;
    try {
      stat = zooKeeper.exists(path, false);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "checking existence of " + path, e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "checking existence of " + path, e);
    }

    return stat != null;
  }

  /**
   * Close this ZooKeeper session.
   */
  public void close() {
    try {
      zooKeeper.close();
      INSTANCES.remove(instanceName);
      LOG.debug("<" + instanceName + ">" + "Closed connection with ZooKeeper; " + this.rootRegionZNode);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to close connection with ZooKeeper");
    }
  }

  public String getZNode(String parentZNode, String znodeName) {
    return znodeName.charAt(0) == ZNODE_PATH_SEPARATOR ?
        znodeName : joinPath(parentZNode, znodeName);
  }

  public String getZNodePathForHBase(String znodeName) {
    return getZNode(parentZNode, znodeName);
  }

  private String joinPath(String parent, String child) {
    return parent + ZNODE_PATH_SEPARATOR + child;
  }

  /**
   * Get the path of the masterElectionZNode
   * @return the path to masterElectionZNode
   */
  public String getMasterElectionZNode() {
    return masterElectionZNode;
  }

  /**
   * Get the path of the parent ZNode
   * @return path of that znode
   */
  public String getParentZNode() {
    return parentZNode;
  }

  /**
   * Scan a directory of address data.
   * @param znode The parent node
   * @param watcher The watcher to put on the found znodes, if not null
   * @return The directory contents
   */
  public List<HServerAddress> scanAddressDirectory(String znode,
      Watcher watcher) {
    List<HServerAddress> list = new ArrayList<HServerAddress>();
    List<String> nodes = this.listZnodes(znode);
    if(nodes == null) {
      return list;
    }
    for (String node : nodes) {
      String path = joinPath(znode, node);
      list.add(readAddress(path, watcher));
    }
    return list;
  }

  /**
   * List all znodes in the specified path
   * @param znode path to list
   * @return a list of all the znodes
   */
  public List<String> listZnodes(String znode) {
    return listZnodes(znode, this);
  }

  /**
   * List all znodes in the specified path and set a watcher on each
   * @param znode path to list
   * @param watcher watch to set, can be null
   * @return a list of all the znodes
   */
  public List<String> listZnodes(String znode, Watcher watcher) {
    List<String> nodes = null;
    if (watcher == null) {
      watcher = this;
    }
    try {
      if (checkExistenceOf(znode)) {
        nodes = zooKeeper.getChildren(znode, watcher);
        for (String node : nodes) {
          getDataAndWatch(znode, node, watcher);
        }
      }
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to read " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to read " + znode + " znode in ZooKeeper: " + e);
    }
    return nodes;
  }

  public byte[] getData(String parentZNode, String znode) {
    return getDataAndWatch(parentZNode, znode, null);
  }

  public byte[] getDataAndWatch(String parentZNode,
                                String znode, Watcher watcher) {
    byte[] data = null;
    try {
      String path = joinPath(parentZNode, znode);
      // TODO: ZK-REFACTOR: remove existance check?
      if (checkExistenceOf(path)) {
        data = zooKeeper.getData(path, watcher, null);
      }
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to read " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to read " + znode + " znode in ZooKeeper: " + e);
    }
    return data;
  }

  /**
   * Write a znode and fail if it already exists
   * @param parentPath parent path to the new znode
   * @param child name of the znode
   * @param strData data to insert
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void writeZNode(String parentPath, String child, String strData)
      throws InterruptedException, KeeperException {
    writeZNode(parentPath, child, strData, false);
  }


  /**
   * Write (and optionally over-write) a znode
   * @param parentPath parent path to the new znode
   * @param child name of the znode
   * @param strData data to insert
   * @param failOnWrite true if an exception should be returned if the znode
   * already exists, false if it should be overwritten
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void writeZNode(String parentPath, String child, String strData,
      boolean failOnWrite) throws InterruptedException, KeeperException {
    String path = joinPath(parentPath, child);
    if (!ensureExists(parentPath)) {
      LOG.error("<" + instanceName + ">" + "unable to ensure parent exists: " + parentPath);
    }
    byte[] data = Bytes.toBytes(strData);
    Stat stat = this.zooKeeper.exists(path, false);
    if (failOnWrite || stat == null) {
      this.zooKeeper.create(path, data,
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.debug("<" + instanceName + ">" + "Created " + path + " with data " + strData);
    } else {
      this.zooKeeper.setData(path, data, -1);
      LOG.debug("<" + instanceName + ">" + "Updated " + path + " with data " + strData);
    }
  }

  /**
   * Get the key to the ZK ensemble for this configuration without
   * adding a name at the end
   * @param conf Configuration to use to build the key
   * @return ensemble key without a name
   */
  public static String getZookeeperClusterKey(Configuration conf) {
    return getZookeeperClusterKey(conf, null);
  }

  /**
   * Get the key to the ZK ensemble for this configuration and append
   * a name at the end
   * @param conf Configuration to use to build the key
   * @param name Name that should be appended at the end if not empty or null
   * @return ensemble key with a name (if any)
   */
  public static String getZookeeperClusterKey(Configuration conf, String name) {
    String quorum = conf.get(HConstants.ZOOKEEPER_QUORUM.replaceAll(
        "[\\t\\n\\x0B\\f\\r]", ""));
    StringBuilder builder = new StringBuilder(quorum);
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    if (name != null && !name.isEmpty()) {
      builder.append(",");
      builder.append(name);
    }
    return builder.toString();
  }

  /**
   * Get the znode that has all the regions in transition.
   * @return path to znode
   */
  public String getRegionInTransitionZNode() {
    return this.rgnsInTransitZNode;
  }

  /**
   * Get the path of this region server's znode
   * @return path to znode
   */
  public String getRsZNode() {
    return this.rsZNode;
  }

  public void deleteZNode(String zNodeName, int version) {
    String fullyQualifiedZNodeName = getZNode(parentZNode, zNodeName);
    try
    {
      zooKeeper.delete(fullyQualifiedZNodeName, version);
    }
    catch (InterruptedException e)
    {
      LOG.warn("<" + instanceName + ">" + "Failed to delete ZNode " + fullyQualifiedZNodeName + " in ZooKeeper", e);
    }
    catch (KeeperException e)
    {
      LOG.warn("<" + instanceName + ">" + "Failed to delete ZNode " + fullyQualifiedZNodeName + " in ZooKeeper", e);
    }
  }

  public String createZNodeIfNotExists(String zNodeName) {
    return createZNodeIfNotExists(zNodeName, null, CreateMode.PERSISTENT, true);
  }

  public void watchZNode(String zNodeName) {
    String fullyQualifiedZNodeName = getZNode(parentZNode, zNodeName);

    try {
      zooKeeper.exists(fullyQualifiedZNodeName, this);
      zooKeeper.getData(fullyQualifiedZNodeName, this, null);
      zooKeeper.getChildren(fullyQualifiedZNodeName, this);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create ZNode " + fullyQualifiedZNodeName + " in ZooKeeper", e);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create ZNode " + fullyQualifiedZNodeName + " in ZooKeeper", e);
    }
  }

  public String createZNodeIfNotExists(String zNodeName, byte[] data, CreateMode createMode, boolean watch) {
    String fullyQualifiedZNodeName = getZNode(parentZNode, zNodeName);

    if (!ensureParentExists(fullyQualifiedZNodeName)) {
      return null;
    }

    try {
      // create the znode
      zooKeeper.create(fullyQualifiedZNodeName, data, Ids.OPEN_ACL_UNSAFE, createMode);
      LOG.debug("<" + instanceName + ">" + "Created ZNode " + fullyQualifiedZNodeName + " in ZooKeeper");
      // watch the znode for deletion, data change, creation of children
      if(watch) {
        watchZNode(zNodeName);
      }
      return fullyQualifiedZNodeName;
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create ZNode " + fullyQualifiedZNodeName + " in ZooKeeper", e);
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to create ZNode " + fullyQualifiedZNodeName + " in ZooKeeper", e);
    }

    return null;
  }

  public byte[] readZNode(String znodeName, Stat stat) throws IOException {
    byte[] data;
    try {
      String fullyQualifiedZNodeName = getZNode(parentZNode, znodeName);
      data = zooKeeper.getData(fullyQualifiedZNodeName, this, stat);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    return data;
  }

  // TODO: perhaps return the version number from this write?
  public boolean writeZNode(String znodeName, byte[] data, int version, boolean watch) throws IOException {
      try {
        String fullyQualifiedZNodeName = getZNode(parentZNode, znodeName);
        zooKeeper.setData(fullyQualifiedZNodeName, data, version);
        if(watch) {
          zooKeeper.getData(fullyQualifiedZNodeName, this, null);
        }
        return true;
      } catch (InterruptedException e) {
        LOG.warn("<" + instanceName + ">" + "Failed to write data to ZooKeeper", e);
        throw new IOException(e);
      } catch (KeeperException e) {
        LOG.warn("<" + instanceName + ">" + "Failed to write data to ZooKeeper", e);
        throw new IOException(e);
      }
    }

  /**
   * Given a region name and some data, this method creates a new the region
   * znode data under the UNASSGINED znode with the data passed in. This method
   * will not update data for existing znodes.
   *
   * @param regionName - encoded name of the region
   * @param data - new serialized data to update the region znode
   */
  private void createUnassignedRegion(String regionName, byte[] data) {
    String znode = getZNode(getRegionInTransitionZNode(), regionName);
    if(LOG.isDebugEnabled()) {
      // check if this node already exists -
      //   - it should not exist
      //   - if it does, it should be in the CLOSED state
      if(exists(znode, true)) {
        Stat stat = new Stat();
        byte[] oldData = null;
        try {
          oldData = readZNode(znode, stat);
        } catch (IOException e) {
          LOG.error("Error reading data for " + znode);
        }
        if(oldData == null) {
          LOG.debug("While creating UNASSIGNED region " + regionName + " exists with no data" );
        }
        else {
          LOG.debug("While creating UNASSIGNED region " + regionName + " exists, state = " + (HBaseEventType.fromByte(oldData[0])));
        }
      }
      else {
        if(data == null) {
          LOG.debug("Creating UNASSIGNED region " + regionName + " with no data" );
        }
        else {
          LOG.debug("Creating UNASSIGNED region " + regionName + " in state = " + (HBaseEventType.fromByte(data[0])));
        }
      }
    }
    synchronized(unassignedZNodesWatched) {
      unassignedZNodesWatched.add(znode);
      createZNodeIfNotExists(znode, data, CreateMode.PERSISTENT, true);
    }
  }

  /**
   * Given a region name and some data, this method updates the region znode
   * data under the UNASSGINED znode with the latest data. This method will
   * update the znode data only if it already exists.
   *
   * @param regionName - encoded name of the region
   * @param data - new serialized data to update the region znode
   */
  public void updateUnassignedRegion(String regionName, byte[] data) {
    String znode = getZNode(getRegionInTransitionZNode(), regionName);
    // this is an update - make sure the node already exists
    if(!exists(znode, true)) {
      LOG.error("Cannot update " + znode + " - node does not exist" );
      return;
    }

    Stat stat = new Stat();
    byte[] oldData = null;
    try {
      oldData = readZNode(znode, stat);
    } catch (IOException e) {
      LOG.error("Error reading data for " + znode);
    }
    // If there is no data in the ZNode, then update it
    if(oldData == null) {
      LOG.debug("While updating UNASSIGNED region " + regionName + " - node exists with no data" );
    }
    // If there is data in the ZNode, do not update if it is already correct
    else {
      HBaseEventType curState = HBaseEventType.fromByte(oldData[0]);
      HBaseEventType newState = HBaseEventType.fromByte(data[0]);
      // If the znode has the right state already, do not update it. Updating
      // the znode again and again will bump up the zk version. This may cause
      // the region server to fail. The RS expects that the znode is never
      // updated by anyone else while it is opening/closing a region.
      if(curState == newState) {
        LOG.debug("No need to update UNASSIGNED region " + regionName +
                  " as it already exists in state = " + curState);
        return;
      }

      // If the ZNode is in another state, then update it
      LOG.debug("UNASSIGNED region " + regionName + " is currently in state = " +
                curState + ", updating it to " + newState);
    }
    // Update the ZNode
    synchronized(unassignedZNodesWatched) {
      unassignedZNodesWatched.add(znode);
      try {
        writeZNode(znode, data, -1, true);
      } catch (IOException e) {
        LOG.error("Error writing data for " + znode + ", could not update state to " + (HBaseEventType.fromByte(data[0])));
      }
    }
  }

  /**
   * This method will create a new region in transition entry in ZK with the
   * speficied data if none exists. If one already exists, it will update the
   * data with whatever is passed in.
   *
   * @param regionName - encoded name of the region
   * @param data - serialized data for the region znode
   */
  public void createOrUpdateUnassignedRegion(String regionName, byte[] data) {
    String znode = getZNode(getRegionInTransitionZNode(), regionName);
    if(exists(znode, true)) {
      updateUnassignedRegion(regionName, data);
    }
    else {
      createUnassignedRegion(regionName, data);
    }
  }

  public void deleteUnassignedRegion(String regionName) {
    String znode = getZNode(getRegionInTransitionZNode(), regionName);
    try {
      LOG.debug("Deleting ZNode " + znode + " in ZooKeeper as region is open...");
      synchronized(unassignedZNodesWatched) {
        unassignedZNodesWatched.remove(znode);
        deleteZNode(znode);
      }
    } catch (KeeperException.SessionExpiredException e) {
      LOG.error("Zookeeper session has expired", e);
      // if the session has expired try to reconnect to ZK, then perform query
      try {
        // TODO: ZK-REFACTOR: should just quit on reconnect??
        reconnectToZk();
        synchronized(unassignedZNodesWatched) {
          unassignedZNodesWatched.remove(znode);
          deleteZNode(znode);
        }
      } catch (IOException e1) {
        LOG.error("Error reconnecting to zookeeper", e1);
        throw new RuntimeException("Error reconnecting to zookeeper", e1);
      } catch (KeeperException.SessionExpiredException e1) {
        LOG.error("Error reading after reconnecting to zookeeper", e1);
        throw new RuntimeException("Error reading after reconnecting to zookeeper", e1);
      } catch (KeeperException e1) {
        LOG.error("Error reading after reconnecting to zookeeper", e1);
      } catch (InterruptedException e1) {
        LOG.error("Error reading after reconnecting to zookeeper", e1);
      }
    } catch (KeeperException e) {
      LOG.error("Error deleting region " + regionName, e);
    } catch (InterruptedException e) {
      LOG.error("Error deleting region " + regionName, e);
    }
  }

  /**
   * Atomically adds a watch and reads data from the unwatched znodes in the
   * UNASSGINED region. This works because the master is the only person
   * deleting nodes.
   * @param znode
   * @return
   */
  public List<ZNodePathAndData> watchAndGetNewChildren(String znode) {
    List<String> nodes = null;
    List<ZNodePathAndData> newNodes = new ArrayList<ZNodePathAndData>();
    try {
      if (checkExistenceOf(znode)) {
        synchronized(unassignedZNodesWatched) {
          nodes = zooKeeper.getChildren(znode, this);
          for (String node : nodes) {
            String znodePath = joinPath(znode, node);
            if(!unassignedZNodesWatched.contains(znodePath)) {
              byte[] data = getDataAndWatch(znode, node, this);
              newNodes.add(new ZNodePathAndData(znodePath, data));
              unassignedZNodesWatched.add(znodePath);
            }
          }
        }
      }
    } catch (KeeperException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to read " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("<" + instanceName + ">" + "Failed to read " + znode + " znode in ZooKeeper: " + e);
    }
    return newNodes;
  }

  public static class ZNodePathAndData {
    private String zNodePath;
    private byte[] data;

    public ZNodePathAndData(String zNodePath, byte[] data) {
      this.zNodePath = zNodePath;
      this.data = data;
    }

    public String getzNodePath() {
      return zNodePath;
    }
    public byte[] getData() {
      return data;
    }

  }
}
