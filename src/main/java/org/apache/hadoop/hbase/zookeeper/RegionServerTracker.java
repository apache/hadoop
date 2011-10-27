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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the online region servers via ZK.
 *
 * <p>Handling of new RSs checking in is done via RPC.  This class
 * is only responsible for watching for expired nodes.  It handles
 * listening for changes in the RS node list and watching each node.
 *
 * <p>If an RS node gets deleted, this automatically handles calling of
 * {@link ServerManager#expireServer(ServerName)}
 */
public class RegionServerTracker extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(RegionServerTracker.class);
  private NavigableSet<ServerName> regionServers = new TreeSet<ServerName>();
  private ServerManager serverManager;
  private Abortable abortable;

  public RegionServerTracker(ZooKeeperWatcher watcher,
      Abortable abortable, ServerManager serverManager) {
    super(watcher);
    this.abortable = abortable;
    this.serverManager = serverManager;
  }

  /**
   * Starts the tracking of online RegionServers.
   *
   * <p>All RSs will be tracked after this method is called.
   *
   * @throws KeeperException
   * @throws IOException
   */
  public void start() throws KeeperException, IOException {
    watcher.registerListener(this);
    List<String> servers =
      ZKUtil.listChildrenAndWatchThem(watcher, watcher.rsZNode);
    add(servers);
  }

  private void add(final List<String> servers) throws IOException {
    synchronized(this.regionServers) {
      this.regionServers.clear();
      for (String n: servers) {
        ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(n));
        this.regionServers.add(sn);
      }
    }
  }

  private void remove(final ServerName sn) {
    synchronized(this.regionServers) {
      this.regionServers.remove(sn);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (path.startsWith(watcher.rsZNode)) {
      String serverName = ZKUtil.getNodeName(path);
      LOG.info("RegionServer ephemeral node deleted, processing expiration [" +
        serverName + "]");
      ServerName sn = ServerName.parseServerName(serverName);
      if (!serverManager.isServerOnline(sn)) {
        LOG.info(serverName.toString() + " is not online");
        return;
      }
      remove(sn);
      this.serverManager.expireServer(sn);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.rsZNode)) {
      try {
        List<String> servers =
          ZKUtil.listChildrenAndWatchThem(watcher, watcher.rsZNode);
        add(servers);
      } catch (IOException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      } catch (KeeperException e) {
        abortable.abort("Unexpected zk exception getting RS nodes", e);
      }
    }
  }

  /**
   * Gets the online servers.
   * @return list of online servers
   */
  public List<ServerName> getOnlineServers() {
    synchronized (this.regionServers) {
      return new ArrayList<ServerName>(this.regionServers);
    }
  }
}
