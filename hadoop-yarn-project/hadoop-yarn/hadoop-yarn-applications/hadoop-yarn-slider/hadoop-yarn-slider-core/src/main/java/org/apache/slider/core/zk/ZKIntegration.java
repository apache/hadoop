/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.core.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


public class ZKIntegration implements Watcher, Closeable {

/**
 * Base path for services
 */
  public static String ZK_SERVICES = "services";
  /**
   * Base path for all Slider references
   */
  public static String ZK_SLIDER = "slider";
  public static String ZK_USERS = "users";
  public static String SVC_SLIDER = "/" + ZK_SERVICES + "/" + ZK_SLIDER;
  public static String SVC_SLIDER_USERS = SVC_SLIDER + "/" + ZK_USERS;

  public static final List<String> ZK_USERS_PATH_LIST = new ArrayList<String>();
  static {
    ZK_USERS_PATH_LIST.add(ZK_SERVICES);
    ZK_USERS_PATH_LIST.add(ZK_SLIDER);
    ZK_USERS_PATH_LIST.add(ZK_USERS);
  }

  public static int SESSION_TIMEOUT = 30000;
  protected static final Logger log =
    LoggerFactory.getLogger(ZKIntegration.class);
  private ZooKeeper zookeeper;
  private final String username;
  private final String clustername;
  private final String userPath;
  private int sessionTimeout = SESSION_TIMEOUT;
/**
 flag to set to indicate that the user path should be created if
 it is not already there
 */
  private final AtomicBoolean toInit = new AtomicBoolean(false);
  private final boolean createClusterPath;
  private final Watcher watchEventHandler;
  private final String zkConnection;
  private final boolean canBeReadOnly;

  protected ZKIntegration(String zkConnection,
                          String username,
                          String clustername,
                          boolean canBeReadOnly,
                          boolean createClusterPath,
                          Watcher watchEventHandler,
                          int sessionTimeout
  ) throws IOException {
    this.username = username;
    this.clustername = clustername;
    this.watchEventHandler = watchEventHandler;
    this.zkConnection = zkConnection;
    this.canBeReadOnly = canBeReadOnly;
    this.createClusterPath = createClusterPath;
    this.sessionTimeout = sessionTimeout;
    this.userPath = mkSliderUserPath(username);
  }

  public void init() throws IOException {
    assert zookeeper == null;
    log.debug("Binding ZK client to {}", zkConnection);
    zookeeper = new ZooKeeper(zkConnection, sessionTimeout, this, canBeReadOnly);
  }

  /**
   * Create an instance bonded to the specific closure
   * @param zkConnection
   * @param username
   * @param clustername
   * @param canBeReadOnly
   * @param watchEventHandler
   * @return the new instance
   * @throws IOException
   */
  public static ZKIntegration newInstance(String zkConnection,
      String username,
      String clustername,
      boolean createClusterPath,
      boolean canBeReadOnly,
      Watcher watchEventHandler,
      int sessionTimeout) throws IOException {

    return new ZKIntegration(zkConnection,
                             username,
                             clustername,
                             canBeReadOnly,
                             createClusterPath,
                             watchEventHandler,
                             sessionTimeout);
  }


  @Override
  public synchronized void close() throws IOException {
    if (zookeeper != null) {
      try {
        zookeeper.close();
      } catch (InterruptedException ignored) {

      }
      zookeeper = null;
    }
  }

  public String getConnectionString() {
    return zkConnection;
  }

  public String getClusterPath() {
    return mkClusterPath(username, clustername);
  }

  public boolean getConnected() {
    return zookeeper.getState().isConnected();
  }

  public boolean getAlive() {
    return zookeeper.getState().isAlive();
  }

  public ZooKeeper.States getState() {
    return zookeeper.getState();
  }

  public Stat getClusterStat() throws KeeperException, InterruptedException {
    return stat(getClusterPath());
  }

  public boolean exists(String path) throws
                                     KeeperException,
                                     InterruptedException {
    return stat(path) != null;
  }

  public Stat stat(String path) throws KeeperException, InterruptedException {
    return zookeeper.exists(path, false);
  }

  @Override
  public String toString() {
    return "ZK integration bound @  " + zkConnection + ": " + zookeeper;
  }
  
/**
 * Event handler to notify of state events
 * @param event
 */
  @Override
  public void process(WatchedEvent event) {
    log.debug("{}", event);
    try {
      maybeInit();
    } catch (Exception e) {
      log.error("Failed to init", e);
    }
    if (watchEventHandler != null) {
      watchEventHandler.process(event);
    }
  }

  private void maybeInit() throws KeeperException, InterruptedException {
    if (!toInit.getAndSet(true) && createClusterPath) {
      log.debug("initing");
      //create the user path
      mkPath(ZK_USERS_PATH_LIST, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      //create the specific user
      createPath(userPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  /**
   * Create a path under a parent, don't care if it already exists
   * As the path isn't returned, this isn't the way to create sequentially
   * numbered nodes.
   * @param parent parent dir. Must have a trailing / if entry!=null||empty 
   * @param entry entry -can be null or "", in which case it is not appended
   * @param acl
   * @param createMode
   * @return the path if created; null if not
   */
  public String createPath(String parent,
                           String entry,
                           List<ACL> acl,
                           CreateMode createMode) throws KeeperException, InterruptedException {
    //initial create of full path
    assert acl != null;
    assert !acl.isEmpty();
    assert parent != null;
    String path = parent;
    if (entry != null) {
      path = path + entry;
    }
    try {
      log.debug("Creating ZK path {}", path);
      return zookeeper.create(path, null, acl, createMode);
    } catch (KeeperException.NodeExistsException ignored) {
      //node already there
      log.debug("node already present:{}",path);
      return null;
    }
  }

  /**
   * Recursive path create
   * @param paths path list
   * @param acl acl list
   * @param createMode create modes
   */
  public void mkPath(List<String> paths,
                     List<ACL> acl,
                     CreateMode createMode) throws KeeperException, InterruptedException {
    String history = "/";
    for (String entry : paths) {
      createPath(history, entry, acl, createMode);
      history = history + entry + "/";
    }
  }

/**
 * Blocking enum of users
 * @return an unordered list of clusters under a user
 */
  public List<String> getClusters() throws KeeperException, InterruptedException {
    return zookeeper.getChildren(userPath, null);
  }

  /**
   * Delete a node, does not throw an exception if the path is not fond
   * @param path path to delete
   * @return true if the path could be deleted, false if there was no node to delete 
   *
   */
  public boolean delete(String path) throws
                                     InterruptedException,
                                     KeeperException {
    try {
      zookeeper.delete(path, -1);
      log.debug("Deleting {}", path);
      return true;
    } catch (KeeperException.NoNodeException ignored) {
      return false;
    }
  }

  /**
   * Recursively delete a node, does not throw exception if any node does not exist.
   * @param path
   * @return true if delete was successful
   */
  public boolean deleteRecursive(String path) throws KeeperException, InterruptedException {

    try {
      List<String> children = zookeeper.getChildren(path, false);
      for (String child : children) {
        deleteRecursive(path + "/" + child);
      }
      delete(path);
    } catch (KeeperException.NoNodeException ignored) {
      return false;
    }

    return true;
  }

  /**
 * Build the path to a cluster; exists once the cluster has come up.
 * Even before that, a ZK watcher could wait for it.
 * @param username user
 * @param clustername name of the cluster
 * @return a strin
 */
  public static String mkClusterPath(String username, String clustername) {
    return mkSliderUserPath(username) + "/" + clustername;
  }
/**
 * Build the path to a cluster; exists once the cluster has come up.
 * Even before that, a ZK watcher could wait for it.
 * @param username user
 * @return a string
 */
  public static String mkSliderUserPath(String username) {
    return SVC_SLIDER_USERS + "/" + username;
  }


}
