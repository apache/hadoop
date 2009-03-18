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
import java.io.InputStream;
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

  private static String quorumServers = null;
  static {
    loadZooKeeperConfig();
  }

  private final ZooKeeper zooKeeper;
  private final WatcherWrapper watcher;

  private final String rootRegionZNode;
  private final String outOfSafeModeZNode;
  private final String rsZNode;

  /**
   * Create a ZooKeeperWrapper.
   * @param conf HBaseConfiguration to read settings from.
   * @throws IOException If a connection error occurs.
   */
  public ZooKeeperWrapper(HBaseConfiguration conf)
  throws IOException {
    this(conf, null);
  }

  /**
   * Create a ZooKeeperWrapper.
   * @param conf HBaseConfiguration to read settings from.
   * @param watcher ZooKeeper watcher to register.
   * @throws IOException If a connection error occurs.
   */
  public ZooKeeperWrapper(HBaseConfiguration conf, Watcher watcher)
  throws IOException {
    if (quorumServers == null) {
      throw new IOException("Could not read quorum servers from " +
                            ZOOKEEPER_CONFIG_NAME);
    }

    int sessionTimeout = conf.getInt("zookeeper.session.timeout", 10 * 1000);
    this.watcher = new WatcherWrapper(watcher);
    try {
      zooKeeper = new ZooKeeper(quorumServers, sessionTimeout, this.watcher);
    } catch (IOException e) {
      LOG.error("Failed to create ZooKeeper object: " + e);
      throw new IOException(e);
    }

    String parentZNode = conf.get("zookeeper.znode.parent", "/hbase");

    String rootServerZNodeName = conf.get("zookeeper.znode.rootserver",
                                          "root-region-server");
    String outOfSafeModeZNodeName = conf.get("zookeeper.znode.safemode",
                                             "safe-mode");
    String rsZNodeName = conf.get("zookeeper.znode.rs", "rs");
    
    rootRegionZNode = getZNode(parentZNode, rootServerZNodeName);
    outOfSafeModeZNode = getZNode(parentZNode, outOfSafeModeZNodeName);
    rsZNode = getZNode(parentZNode, rsZNodeName);
  }

  /**
   * This is for tests to directly set the ZooKeeper quorum servers.
   * @param servers comma separated host:port ZooKeeper quorum servers.
   */
  public static void setQuorumServers(String servers) {
    quorumServers = servers;
  }

  private static void loadZooKeeperConfig() {
    InputStream inputStream =
      ZooKeeperWrapper.class.getClassLoader().getResourceAsStream(ZOOKEEPER_CONFIG_NAME);
    if (inputStream == null) {
      LOG.error("fail to open ZooKeeper config file " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      LOG.error("fail to read properties from " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    String clientPort = null;
    List<String> servers = new ArrayList<String>();

    // The clientPort option may come after the server.X hosts, so we need to
    // grab everything and then create the final host:port comma separated list.
    for (Entry<Object,Object> property : properties.entrySet()) {
      String key = property.getKey().toString().trim();
      String value = property.getValue().toString().trim();
      if (key.equals("clientPort")) {
        clientPort = value;
      }
      else if (key.startsWith("server.")) {
        String host = value.substring(0, value.indexOf(':'));
        servers.add(host);
      }
    }

    if (clientPort == null) {
      LOG.error("no clientPort found in " + ZOOKEEPER_CONFIG_NAME);
      return;
    }

    // If no server.X lines exist, then we're using a single instance ZooKeeper
    // on the master node.
    if (servers.isEmpty()) {
      HBaseConfiguration conf = new HBaseConfiguration();
      String masterAddress = conf.get(MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS);
      String masterHost = "localhost";
      if (!masterAddress.equals("local")) {
        masterHost = masterAddress.substring(0, masterAddress.indexOf(':'));
      }
      servers.add(masterHost);
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
    LOG.info("Quorum servers: " + quorumServers);
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
    byte[] data;
    try {
      data = zooKeeper.getData(rootRegionZNode, false, null);
    } catch (InterruptedException e) {
      return null;
    } catch (KeeperException e) {
      return null;
    }

    String addressString = Bytes.toString(data);
    LOG.debug("Read ZNode " + rootRegionZNode + " got " + addressString);
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
      LOG.warn("Failed to create " + znode + ":", e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to create " + znode + ":", e);
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
    byte[] data = Bytes.toBytes(info.getServerAddress().getBindAddress());
    String znode = joinPath(rsZNode, Long.toString(info.getStartCode()));
    try {
      zooKeeper.create(znode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOG.debug("Created ZNode " + znode
          + " with data " + info.getServerAddress().getBindAddress());
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
    byte[] data = Bytes.toBytes(info.getServerAddress().getBindAddress());
    String znode = rsZNode + "/" + info.getStartCode();
    try {
      zooKeeper.setData(znode, data, -1);
      LOG.debug("Updated ZNode " + znode
          + " with data " + info.getServerAddress().getBindAddress());
      zooKeeper.getData(znode, watcher, null);
      return true;
    } catch (KeeperException e) {
      LOG.warn("Failed to update " + znode + " znode in ZooKeeper: " + e);
    } catch (InterruptedException e) {
      LOG.warn("Failed to update " + znode + " znode in ZooKeeper: " + e);
    }

    return false;
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
}
