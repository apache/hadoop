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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Wraps a ZooKeeper instance and adds HBase specific functionality.
 *
 * This class provides methods to:
 * - read/write/delete the root region location in ZooKeeper.
 * - set/check out of safe mode flag.
 */
public class ZooKeeperWrapper implements HConstants {
  protected static final Log LOG = LogFactory.getLog(ZooKeeperWrapper.class);

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  private static final char ZNODE_PATH_SEPARATOR = '/';

  private String quorumServers = null;

  private final ZooKeeper zooKeeper;

  private final String parentZNode;
  private final String rootRegionZNode;
  private final String outOfSafeModeZNode;
  private final String rsZNode;
  private final String masterElectionZNode;
  public final String clusterStateZNode;

  /**
   * Create a ZooKeeperWrapper.
   * @param conf HBaseConfiguration to read settings from.
   * @param watcher ZooKeeper watcher to register.
   * @throws IOException If a connection error occurs.
   */
  public ZooKeeperWrapper(HBaseConfiguration conf, Watcher watcher)
  throws IOException {
    Properties properties = HQuorumPeer.makeZKProps(conf);
    setQuorumServers(properties);
    if (quorumServers == null) {
      throw new IOException("Could not read quorum servers from " +
                            ZOOKEEPER_CONFIG_NAME);
    }

    int sessionTimeout = conf.getInt("zookeeper.session.timeout", 60 * 1000);
    try {
      zooKeeper = new ZooKeeper(quorumServers, sessionTimeout, watcher);
    } catch (IOException e) {
      LOG.error("Failed to create ZooKeeper object: " + e);
      throw new IOException(e);
    }

    parentZNode = conf.get("zookeeper.znode.parent", "/hbase");

    String rootServerZNodeName = conf.get("zookeeper.znode.rootserver",
                                          "root-region-server");
    String outOfSafeModeZNodeName = conf.get("zookeeper.znode.safemode",
                                             "safe-mode");
    String rsZNodeName = conf.get("zookeeper.znode.rs", "rs");
    String masterAddressZNodeName = conf.get("zookeeper.znode.master",
      "master");
    String stateZNodeName = conf.get("zookeeper.znode.state",
    "shutdown");
    
    rootRegionZNode = getZNode(parentZNode, rootServerZNodeName);
    outOfSafeModeZNode = getZNode(parentZNode, outOfSafeModeZNodeName);
    rsZNode = getZNode(parentZNode, rsZNodeName);
    masterElectionZNode = getZNode(parentZNode, masterAddressZNodeName);
    clusterStateZNode = getZNode(parentZNode, stateZNodeName);
  }

  private void setQuorumServers(Properties properties) {
    String clientPort = null;
    List<String> servers = new ArrayList<String>();

    // The clientPort option may come after the server.X hosts, so we need to
    // grab everything and then create the final host:port comma separated list.
    boolean anyValid = false;
    for (Entry<Object,Object> property : properties.entrySet()) {
      String key = property.getKey().toString().trim();
      String value = property.getValue().toString().trim();
      if (key.equals("clientPort")) {
        clientPort = value;
      }
      else if (key.startsWith("server.")) {
        String host = value.substring(0, value.indexOf(':'));
        servers.add(host);
        try {
          InetAddress.getByName(host);
          anyValid = true;
        } catch (UnknownHostException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
    }

    if (!anyValid) {
      LOG.error("no valid quorum servers found in " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    if (clientPort == null) {
      LOG.error("no clientPort found in " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    if (servers.isEmpty()) {
      LOG.fatal("No server.X lines found in conf/zoo.cfg. HBase must have a " +
                "ZooKeeper cluster configured for its operation.");
      return;
    }

    StringBuilder hostPortBuilder = new StringBuilder();
    for (int i = 0; i < servers.size(); ++i) {
      String host = servers.get(i);
      if (i > 0) {
        hostPortBuilder.append(',');
      }
      hostPortBuilder.append(host);
      hostPortBuilder.append(':');
      hostPortBuilder.append(clientPort);
    }

    quorumServers = hostPortBuilder.toString();
  }

  /** @return String dump of everything in ZooKeeper. */
  public String dump() {
    StringBuilder sb = new StringBuilder();
    sb.append("\nHBase tree in ZooKeeper is rooted at ").append(parentZNode);
    sb.append("\n  Cluster up? ").append(exists(clusterStateZNode));
    sb.append("\n  In safe mode? ").append(!checkOutOfSafeMode());
    sb.append("\n  Master address: ").append(readMasterAddress(null));
    sb.append("\n  Region server holding ROOT: ").append(readRootRegionLocation());
    sb.append("\n  Region servers:");
    for (HServerAddress address : scanRSDirectory()) {
      sb.append("\n    - ").append(address);
    }
    return sb.toString();
  }

  private boolean exists(String znode) {
    try {
      return zooKeeper.exists(znode, null) != null;
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
  public void setClusterStateWatch(Watcher watcher) {
    try {
      zooKeeper.exists(clusterStateZNode, watcher);
    } catch (InterruptedException e) {
      LOG.warn("Failed to check on ZNode " + clusterStateZNode, e);
    } catch (KeeperException e) {
      LOG.warn("Failed to check on ZNode " + clusterStateZNode, e);
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
        LOG.debug("State node wrote in ZooKeeper");
      } else {
        zooKeeper.delete(clusterStateZNode, -1);
        LOG.debug("State node deleted in ZooKeeper");
      }
      return true;
    } catch (InterruptedException e) {
      LOG.warn("Failed to set state node in ZooKeeper", e);
    } catch (KeeperException e) {
      LOG.warn("Failed to set state node in ZooKeeper", e);
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
      LOG.warn("Failed to set watcher on ZNode " + masterElectionZNode, e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn("Failed to set watcher on ZNode " + masterElectionZNode, e);
      return false;
    }
    LOG.debug("Set watcher on master address ZNode " + masterElectionZNode);
    return true;
  }

  private HServerAddress readAddress(String znode, Watcher watcher) {
    try {
      return readAddressOrThrow(znode, watcher);
    } catch (IOException e) {
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
    LOG.debug("Read ZNode " + znode + " got " + addressString);
    HServerAddress address = new HServerAddress(addressString);
    return address;
  }

  private boolean ensureExists(final String znode) {
    try {
      zooKeeper.create(znode, new byte[0],
                       Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOG.debug("Created ZNode " + znode);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      return true;      // ok, move on.
    } catch (KeeperException.NoNodeException e) {
      return ensureParentExists(znode) && ensureExists(znode);
    } catch (KeeperException e) {
      LOG.warn("Failed to create " + znode +
        " -- check quorum servers, currently=" + this.quorumServers, e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create " + znode +
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
      zooKeeper.delete(rootRegionZNode, -1);
      LOG.debug("Deleted ZNode " + rootRegionZNode);
      return true;
    } catch (KeeperException.NoNodeException e) {
      return true;    // ok, move on.
    } catch (KeeperException e) {
      LOG.warn("Failed to delete " + rootRegionZNode + ": " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to delete " + rootRegionZNode + ": " + e);
    }

    return false;
  }

  private boolean createRootRegionLocation(String address) {
    byte[] data = Bytes.toBytes(address);
    try {
      zooKeeper.create(rootRegionZNode, data, Ids.OPEN_ACL_UNSAFE,
                       CreateMode.PERSISTENT);
      LOG.debug("Created ZNode " + rootRegionZNode + " with data " + address);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to create root region in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create root region in ZooKeeper: " + e);
    }

    return false;
  }

  private boolean updateRootRegionLocation(String address) {
    byte[] data = Bytes.toBytes(address);
    try {
      zooKeeper.setData(rootRegionZNode, data, -1);
      LOG.debug("SetData of ZNode " + rootRegionZNode + " with " + address);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to set root region location in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to set root region location in ZooKeeper: " + e);
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
  public boolean writeMasterAddress(HServerAddress address) {
    if (!ensureParentExists(masterElectionZNode)) {
      return false;
    }

    String addressStr = address.toString();
    byte[] data = Bytes.toBytes(addressStr);
    try {
      zooKeeper.create(masterElectionZNode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("Wrote master address " + address + " to ZooKeeper");
      return true;
    } catch (InterruptedException e) {
      LOG.warn("Failed to write master address " + address + " to ZooKeeper", e);
    } catch (KeeperException e) {
      LOG.warn("Failed to write master address " + address + " to ZooKeeper", e);
    }

    return false;
  }

  /**
   * Check if we're out of safe mode. Being out of safe mode is signified by an
   * ephemeral ZNode existing in ZooKeeper.
   * @return true if we're out of safe mode, false otherwise.
   */
  public boolean checkOutOfSafeMode() {
    if (!ensureParentExists(outOfSafeModeZNode)) {
      return false;
    }

    return checkExistenceOf(outOfSafeModeZNode);
  }

  /**
   * Create ephemeral ZNode signifying that we're out of safe mode.
   * @return true if ephemeral ZNode created successfully, false otherwise.
   */
  public boolean writeOutOfSafeMode() {
    if (!ensureParentExists(outOfSafeModeZNode)) {
      return false;
    }

    try {
      zooKeeper.create(outOfSafeModeZNode, new byte[0], Ids.OPEN_ACL_UNSAFE,
                       CreateMode.EPHEMERAL);
      LOG.debug("Wrote out of safe mode");
      return true;
    } catch (InterruptedException e) {
      LOG.warn("Failed to create out of safe mode in ZooKeeper: " + e);
    } catch (KeeperException e) {
      LOG.warn("Failed to create out of safe mode in ZooKeeper: " + e);
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
    String znode = joinPath(rsZNode, Long.toString(info.getStartCode()));
    try {
      zooKeeper.create(znode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("Created ZNode " + znode
          + " with data " + info.getServerAddress().toString());
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to create " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create " + znode + " znode in ZooKeeper: " + e);
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
    String znode = rsZNode + ZNODE_PATH_SEPARATOR + info.getStartCode();
    try {
      zooKeeper.setData(znode, data, -1);
      LOG.debug("Updated ZNode " + znode
          + " with data " + info.getServerAddress().toString());
      zooKeeper.getData(znode, watcher, null);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to update " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to update " + znode + " znode in ZooKeeper: " + e);
    }

    return false;
  }
  
  /**
   * Scans the regions servers directory
   * @return A list of server addresses
   */
  public List<HServerAddress> scanRSDirectory() {
    List<HServerAddress> addresses = new ArrayList<HServerAddress>();
    try {
      List<String> nodes = zooKeeper.getChildren(rsZNode, false);
      for (String node : nodes) {
        addresses.add(readAddress(rsZNode + ZNODE_PATH_SEPARATOR + node, null));
      }
    } catch (KeeperException e) {
      LOG.warn("Failed to read " + rsZNode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to read " + rsZNode + " znode in ZooKeeper: " + e);
    }
    return addresses;
  }
  
  /**
   * Method used to make sure the region server directory is empty.
   *
   */
  public void clearRSDirectory() {
    try {
      List<String> nodes = zooKeeper.getChildren(rsZNode, false);
      for (String node : nodes) {
        LOG.debug("Deleting node: " + node);
        zooKeeper.delete(joinPath(this.rsZNode, node), -1);
      }
    } catch (KeeperException e) {
      LOG.warn("Failed to delete " + rsZNode + " znodes in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to delete " + rsZNode + " znodes in ZooKeeper: " + e);
    }
  }
  
  private boolean checkExistenceOf(String path) {
    Stat stat = null;
    try {
      stat = zooKeeper.exists(path, false);
    } catch (KeeperException e) {
      LOG.warn("checking existence of " + path, e);
    } catch (InterruptedException e) {
      LOG.warn("checking existence of " + path, e);
    }

    return stat != null;
  }

  /**
   * Close this ZooKeeper session.
   */
  public void close() {
    try {
      zooKeeper.close();
      LOG.debug("Closed connection with ZooKeeper");
    } catch (InterruptedException e) {
      LOG.warn("Failed to close connection with ZooKeeper");
    }
  }
  
  private String getZNode(String parentZNode, String znodeName) {
    return znodeName.charAt(0) == ZNODE_PATH_SEPARATOR ?
        znodeName : joinPath(parentZNode, znodeName);
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
  
  
}
