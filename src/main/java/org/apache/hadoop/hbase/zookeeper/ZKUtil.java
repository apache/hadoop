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
package org.apache.hadoop.hbase.zookeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Internal HBase utility class for ZooKeeper.
 *
 * <p>Contains only static methods and constants.
 *
 * <p>Methods all throw {@link KeeperException} if there is an unexpected
 * zookeeper exception, so callers of these methods must handle appropriately.
 * If ZK is required for the operation, the server will need to be aborted.
 */
public class ZKUtil {
  private static final Log LOG = LogFactory.getLog(ZKUtil.class);

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  private static final char ZNODE_PATH_SEPARATOR = '/';

  /**
   * Creates a new connection to ZooKeeper, pulling settings and ensemble config
   * from the specified configuration object using methods from {@link ZKConfig}.
   *
   * Sets the connection status monitoring watcher to the specified watcher.
   *
   * @param conf configuration to pull ensemble and other settings from
   * @param watcher watcher to monitor connection changes
   * @return connection to zookeeper
   * @throws IOException if unable to connect to zk or config problem
   */
  public static ZooKeeper connect(Configuration conf, Watcher watcher)
  throws IOException {
    Properties properties = ZKConfig.makeZKProps(conf);
    String ensemble = ZKConfig.getZKQuorumServersString(properties);
    return connect(conf, ensemble, watcher);
  }

  public static ZooKeeper connect(Configuration conf, String ensemble,
      Watcher watcher)
  throws IOException {
    return connect(conf, ensemble, watcher, "");
  }

  public static ZooKeeper connect(Configuration conf, String ensemble,
      Watcher watcher, final String descriptor)
  throws IOException {
    if(ensemble == null) {
      throw new IOException("Unable to determine ZooKeeper ensemble");
    }
    int timeout = conf.getInt("zookeeper.session.timeout", 180 * 1000);
    LOG.debug(descriptor + " opening connection to ZooKeeper with ensemble (" +
        ensemble + ")");
    return new ZooKeeper(ensemble, timeout, watcher);
  }

  //
  // Helper methods
  //

  /**
   * Join the prefix znode name with the suffix znode name to generate a proper
   * full znode name.
   *
   * Assumes prefix does not end with slash and suffix does not begin with it.
   *
   * @param prefix beginning of znode name
   * @param suffix ending of znode name
   * @return result of properly joining prefix with suffix
   */
  public static String joinZNode(String prefix, String suffix) {
    return prefix + ZNODE_PATH_SEPARATOR + suffix;
  }

  /**
   * Returns the full path of the immediate parent of the specified node.
   * @param node path to get parent of
   * @return parent of path, null if passed the root node or an invalid node
   */
  public static String getParent(String node) {
    int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
    return idx <= 0 ? null : node.substring(0, idx);
  }

  /**
   * Get the name of the current node from the specified fully-qualified path.
   * @param path fully-qualified path
   * @return name of the current node
   */
  public static String getNodeName(String path) {
    return path.substring(path.lastIndexOf("/")+1);
  }

  /**
   * Get the key to the ZK ensemble for this configuration without
   * adding a name at the end
   * @param conf Configuration to use to build the key
   * @return ensemble key without a name
   */
  public static String getZooKeeperClusterKey(Configuration conf) {
    return getZooKeeperClusterKey(conf, null);
  }

  /**
   * Get the key to the ZK ensemble for this configuration and append
   * a name at the end
   * @param conf Configuration to use to build the key
   * @param name Name that should be appended at the end if not empty or null
   * @return ensemble key with a name (if any)
   */
  public static String getZooKeeperClusterKey(Configuration conf, String name) {
    String ensemble = conf.get(HConstants.ZOOKEEPER_QUORUM.replaceAll(
        "[\\t\\n\\x0B\\f\\r]", ""));
    StringBuilder builder = new StringBuilder(ensemble);
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    if (name != null && !name.isEmpty()) {
      builder.append(",");
      builder.append(name);
    }
    return builder.toString();
  }

  /**
   * Apply the settings in the given key to the given configuration, this is
   * used to communicate with distant clusters
   * @param conf configuration object to configure
   * @param key string that contains the 3 required configuratins
   * @throws IOException
   */
  public static void applyClusterKeyToConf(Configuration conf, String key)
      throws IOException{
    String[] parts = transformClusterKey(key);
    conf.set(HConstants.ZOOKEEPER_QUORUM, parts[0]);
    conf.set("hbase.zookeeper.property.clientPort", parts[1]);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts[2]);
  }

  /**
   * Separate the given key into the three configurations it should contain:
   * hbase.zookeeper.quorum, hbase.zookeeper.client.port
   * and zookeeper.znode.parent
   * @param key
   * @return the three configuration in the described order
   * @throws IOException
   */
  public static String[] transformClusterKey(String key) throws IOException {
    String[] parts = key.split(":");
    if (parts.length != 3) {
      throw new IOException("Cluster key invalid, the format should be:" +
          HConstants.ZOOKEEPER_QUORUM + ":hbase.zookeeper.client.port:"
          + HConstants.ZOOKEEPER_ZNODE_PARENT);
    }
    return parts;
  }

  //
  // Existence checks and watches
  //

  /**
   * Watch the specified znode for delete/create/change events.  The watcher is
   * set whether or not the node exists.  If the node already exists, the method
   * returns true.  If the node does not exist, the method returns false.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if znode exists, false if does not exist or error
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat s = zkw.getZooKeeper().exists(znode, zkw);
      LOG.debug(zkw.prefix("Set watcher on existing znode " + znode));
      return s != null ? true : false;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Check if the specified node exists.  Sets no watches.
   *
   * Returns true if node exists, false if not.  Returns an exception if there
   * is an unexpected zookeeper exception.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return version of the node if it exists, -1 if does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int checkExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat s = zkw.getZooKeeper().exists(znode, null);
      return s != null ? s.getVersion() : -1;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
      zkw.keeperException(e);
      return -1;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
      zkw.interruptedException(e);
      return -1;
    }
  }

  //
  // Znode listings
  //

  /**
   * Lists the children znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param znode path of node to list and watch children of
   * @return list of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenAndWatchForNewChildren(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      List<String> children = zkw.getZooKeeper().getChildren(znode, zkw);
      return children;
    } catch(KeeperException.NoNodeException ke) {
      LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
          "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * List all the children of the specified znode, setting a watch for children
   * changes and also setting a watch on every individual child in order to get
   * the NodeCreated and NodeDeleted events.
   * @param zkw zookeeper reference
   * @param znode node to get children of and watch
   * @return list of znode names, null if the node doesn't exist
   * @throws KeeperException
   */
  public static List<String> listChildrenAndWatchThem(ZooKeeperWatcher zkw,
      String znode) throws KeeperException {
    List<String> children = listChildrenAndWatchForNewChildren(zkw, znode);
    if (children == null) {
      return null;
    }
    for (String child : children) {
      watchAndCheckExists(zkw, joinZNode(znode, child));
    }
    return children;
  }

  /**
   * Lists the children of the specified znode without setting any watches.
   *
   * Used to list the currently online regionservers and their addresses.
   *
   * Sets no watches at all, this method is best effort.
   *
   * Returns an empty list if the node has no children.  Returns null if the
   * parent node itself does not exist.
   *
   * @param zkw zookeeper reference
   * @param znode node to get children of as addresses
   * @return list of data of children of specified znode, empty if no children,
   *         null if parent does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenNoWatch(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    List<String> children = null;
    try {
      // List the children without watching
      children = zkw.getZooKeeper().getChildren(znode, null);
    } catch(KeeperException.NoNodeException nne) {
      return null;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
    return children;
  }

  /**
   * Atomically add watches and read data from all unwatched unassigned nodes.
   *
   * <p>This works because master is the only person deleting nodes.
   */
  public static List<NodeAndData> watchAndGetNewChildren(ZooKeeperWatcher zkw,
      String baseNode)
  throws KeeperException {
    List<NodeAndData> newNodes = new ArrayList<NodeAndData>();
    synchronized(zkw.getNodes()) {
      List<String> nodes =
        ZKUtil.listChildrenAndWatchForNewChildren(zkw, baseNode);
      for(String node : nodes) {
        String nodePath = ZKUtil.joinZNode(baseNode, node);
        if(!zkw.getNodes().contains(nodePath)) {
          byte [] data = ZKUtil.getDataAndWatch(zkw, nodePath);
          newNodes.add(new NodeAndData(nodePath, data));
          zkw.getNodes().add(nodePath);
        }
      }
    }
    return newNodes;
  }

  /**
   * Simple class to hold a node path and node data.
   */
  public static class NodeAndData {
    private String node;
    private byte [] data;
    public NodeAndData(String node, byte [] data) {
      this.node = node;
      this.data = data;
    }
    public String getNode() {
      return node;
    }
    public byte [] getData() {
      return data;
    }
    @Override
    public String toString() {
      return node + " (" + RegionTransitionData.fromBytes(data) + ")";
    }
  }

  /**
   * Checks if the specified znode has any children.  Sets no watches.
   *
   * Returns true if the node exists and has children.  Returns false if the
   * node does not exist or if the node does not have any children.
   *
   * Used during master initialization to determine if the master is a
   * failed-over-to master or the first master during initial cluster startup.
   * If the directory for regionserver ephemeral nodes is empty then this is
   * a cluster startup, if not then it is not cluster startup.
   *
   * @param zkw zk reference
   * @param znode path of node to check for children of
   * @return true if node has children, false if not or node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean nodeHasChildren(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      return !zkw.getZooKeeper().getChildren(znode, null).isEmpty();
    } catch(KeeperException.NoNodeException ke) {
      LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
      "because node does not exist (not an error)"));
      return false;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Get the number of children of the specified node.
   *
   * If the node does not exist or has no children, returns 0.
   *
   * Sets no watches at all.
   *
   * @param zkw zk reference
   * @param znode path of node to count children of
   * @return number of children of specified node, 0 if none or parent does not
   *         exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int getNumberOfChildren(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat stat = zkw.getZooKeeper().exists(znode, null);
      return stat == null ? 0 : stat.getNumChildren();
    } catch(KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get children of node " + znode));
      zkw.keeperException(e);
    } catch(InterruptedException e) {
      zkw.interruptedException(e);
    }
    return 0;
  }

  //
  // Data retrieval
  //

  /**
   * Get znode data. Does not set a watcher.
   * @return ZNode data
   */
  public static byte [] getData(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      byte [] data = zkw.getZooKeeper().getData(znode, null, null);
      logRetrievedMsg(zkw, znode, data, false);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
        "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Get the data at the specified znode and set a watch.
   *
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataAndWatch(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      byte [] data = zkw.getZooKeeper().getData(znode, zkw, null);
      logRetrievedMsg(zkw, znode, data, true);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
        "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Get the data at the specified znode without setting a watch.
   *
   * Returns the data if the node exists.  Returns null if the node does not
   * exist.
   *
   * Sets the stats of the node in the passed Stat object.  Pass a null stat if
   * not interested.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param stat node status to get if node exists
   * @return data of the specified znode, or null if node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataNoWatch(ZooKeeperWatcher zkw, String znode,
      Stat stat)
  throws KeeperException {
    try {
      byte [] data = zkw.getZooKeeper().getData(znode, null, stat);
      logRetrievedMsg(zkw, znode, data, false);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
          "because node does not exist (not necessarily an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Update the data of an existing node with the expected version to have the
   * specified data.
   *
   * Throws an exception if there is a version mismatch or some other problem.
   *
   * Sets no watches under any conditions.
   *
   * @param zkw zk reference
   * @param znode
   * @param data
   * @param expectedVersion
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.BadVersionException if version mismatch
   */
  public static void updateExistingNodeData(ZooKeeperWatcher zkw, String znode,
      byte [] data, int expectedVersion)
  throws KeeperException {
    try {
      zkw.getZooKeeper().setData(znode, data, expectedVersion);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Data setting
  //

  /**
   * Sets the data of the existing znode to be the specified data.  Ensures that
   * the current data has the specified expected version.
   *
   * <p>If the node does not exist, a {@link NoNodeException} will be thrown.
   *
   * <p>If their is a version mismatch, method returns null.
   *
   * <p>No watches are set but setting data will trigger other watchers of this
   * node.
   *
   * <p>If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @param expectedVersion version expected when setting data
   * @return true if data set, false if version mismatch
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setData(ZooKeeperWatcher zkw, String znode,
      byte [] data, int expectedVersion)
  throws KeeperException, KeeperException.NoNodeException {
    try {
      return zkw.getZooKeeper().setData(znode, data, expectedVersion) != null;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Set data into node creating node if it doesn't yet exist.
   * Does not set watch.
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @throws KeeperException
   */
  public static void createSetData(final ZooKeeperWatcher zkw, final String znode,
      final byte [] data)
  throws KeeperException {
    if (checkExists(zkw, znode) == -1) {
      ZKUtil.createWithParents(zkw, znode);
    }
    ZKUtil.setData(zkw, znode, data);
  }

  /**
   * Sets the data of the existing znode to be the specified data.  The node
   * must exist but no checks are done on the existing data or version.
   *
   * <p>If the node does not exist, a {@link NoNodeException} will be thrown.
   *
   * <p>No watches are set but setting data will trigger other watchers of this
   * node.
   *
   * <p>If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void setData(ZooKeeperWatcher zkw, String znode, byte [] data)
  throws KeeperException, KeeperException.NoNodeException {
    setData(zkw, znode, data, -1);
  }

  //
  // Node creation
  //

  /**
   *
   * Set the specified znode to be an ephemeral node carrying the specified
   * data.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw,
      String znode, byte [] data)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      if(!watchAndCheckExists(zkw, znode)) {
        // It did exist but now it doesn't, try again
        return createEphemeralNodeAndWatch(zkw, znode, data);
      }
      return false;
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
      Thread.currentThread().interrupt();
    }
    return true;
  }

  /**
   * Creates the specified znode to be a persistent node carrying the specified
   * data.
   *
   * Returns true if the node was successfully created, false if the node
   * already existed.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node but return false.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createNodeIfNotExistsAndWatch(
      ZooKeeperWatcher zkw, String znode, byte [] data)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException nee) {
      try {
        zkw.getZooKeeper().exists(znode, zkw);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return false;
      }
      return false;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
    return true;
  }

  /**
   * Creates the specified node with the specified data and watches it.
   *
   * <p>Throws an exception if the node already exists.
   *
   * <p>The node created is persistent and open access.
   *
   * <p>Returns the version number of the created node if successful.
   *
   * @param zkw zk reference
   * @param znode path of node to create
   * @param data data of node to create
   * @return version of node created
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static int createAndWatch(ZooKeeperWatcher zkw,
      String znode, byte [] data)
  throws KeeperException, KeeperException.NodeExistsException {
    try {
      zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      return zkw.getZooKeeper().exists(znode, zkw).getVersion();
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return -1;
    }
  }

  /**
   * Async creates the specified node with the specified data.
   *
   * <p>Throws an exception if the node already exists.
   *
   * <p>The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node to create
   * @param data data of node to create
   * @param cb
   * @param ctx
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static void asyncCreate(ZooKeeperWatcher zkw,
      String znode, byte [] data, final AsyncCallback.StringCallback cb,
      final Object ctx) {
    zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
       CreateMode.PERSISTENT, cb, ctx);
  }

  /**
   * Creates the specified node, if the node does not exist.  Does not set a
   * watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createAndFailSilent(ZooKeeperWatcher zkw,
      String znode)
  throws KeeperException {
    try {
      ZooKeeper zk = zkw.getZooKeeper();
      if (zk.exists(znode, false) == null) {
        zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      }
    } catch(KeeperException.NodeExistsException nee) {
    } catch(KeeperException.NoAuthException nee){
      try {
        if (null == zkw.getZooKeeper().exists(znode, false)) {
          // If we failed to create the file and it does not already exist.
          throw(nee);
        }
      } catch (InterruptedException ie) {
        zkw.interruptedException(ie);
      }

    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  /**
   * Creates the specified node and all parent nodes required for it to exist.
   *
   * No watches are set and no errors are thrown if the node already exists.
   *
   * The nodes created are persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createWithParents(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      if(znode == null) {
        return;
      }
      zkw.getZooKeeper().create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch(KeeperException.NodeExistsException nee) {
      return;
    } catch(KeeperException.NoNodeException nne) {
      createWithParents(zkw, getParent(znode));
      createWithParents(zkw, znode);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Deletes
  //

  /**
   * Delete the specified node.  Sets no watches.  Throws all exceptions.
   */
  public static void deleteNode(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    deleteNode(zkw, node, -1);
  }

  /**
   * Delete the specified node with the specified version.  Sets no watches.
   * Throws all exceptions.
   */
  public static boolean deleteNode(ZooKeeperWatcher zkw, String node,
      int version)
  throws KeeperException {
    try {
      zkw.getZooKeeper().delete(node, version);
      return true;
    } catch(KeeperException.BadVersionException bve) {
      return false;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
      return false;
    }
  }

  /**
   * Deletes the specified node.  Fails silent if the node does not exist.
   * @param zkw
   * @param node
   * @throws KeeperException
   */
  public static void deleteNodeFailSilent(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    try {
      zkw.getZooKeeper().delete(node, -1);
    } catch(KeeperException.NoNodeException nne) {
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  /**
   * Delete the specified node and all of it's children.
   *
   * Sets no watches.  Throws all exceptions besides dealing with deletion of
   * children.
   */
  public static void deleteNodeRecursively(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    try {
      List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
      if(!children.isEmpty()) {
        for(String child : children) {
          deleteNodeRecursively(zkw, joinZNode(node, child));
        }
      }
      zkw.getZooKeeper().delete(node, -1);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  /**
   * Delete all the children of the specified node but not the node itself.
   *
   * Sets no watches.  Throws all exceptions besides dealing with deletion of
   * children.
   */
  public static void deleteChildrenRecursively(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
    if (children == null || children.isEmpty()) return;
    for(String child : children) {
      deleteNodeRecursively(zkw, joinZNode(node, child));
    }
  }

  //
  // ZooKeeper cluster information
  //

  /** @return String dump of everything in ZooKeeper. */
  public static String dump(ZooKeeperWatcher zkw) {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("HBase is rooted at ").append(zkw.baseZNode);
      sb.append("\nMaster address: ").append(
        Bytes.toStringBinary(getData(zkw, zkw.masterAddressZNode)));
      sb.append("\nRegion server holding ROOT: ").append(
        Bytes.toStringBinary(getData(zkw, zkw.rootServerZNode)));
      sb.append("\nRegion servers:");
      for (String child: listChildrenNoWatch(zkw, zkw.rsZNode)) {
        sb.append("\n ").append(child);
      }
      sb.append("\nQuorum Server Statistics:");
      String[] servers = zkw.getQuorum().split(",");
      for (String server : servers) {
        sb.append("\n ").append(server);
        try {
          String[] stat = getServerStats(server);
          for (String s : stat) {
            sb.append("\n  ").append(s);
          }
        } catch (Exception e) {
          sb.append("\n  ERROR: ").append(e.getMessage());
        }
      }
    } catch(KeeperException ke) {
      sb.append("\nFATAL ZooKeeper Exception!\n");
      sb.append("\n" + ke.getMessage());
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
  public static String[] getServerStats(String server)
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
  public static String[] getServerStats(String server, int timeout)
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
      if (line != null) {
        res.add(line);
      } else {
        break;
      }
    }
    socket.close();
    return res.toArray(new String[res.size()]);
  }

  private static void logRetrievedMsg(final ZooKeeperWatcher zkw,
      final String znode, final byte [] data, final boolean watcherSet) {
    if (!LOG.isDebugEnabled()) return;
    LOG.debug(zkw.prefix("Retrieved " + ((data == null)? 0: data.length) +
      " byte(s) of data from znode " + znode +
      (watcherSet? " and set watcher; ": "; data=") +
      (data == null? "null": data.length == 0? "empty": (
          znode.startsWith(zkw.assignmentZNode) ?
              RegionTransitionData.fromBytes(data).toString()
              : StringUtils.abbreviate(Bytes.toStringBinary(data), 32)))));
  }
}
