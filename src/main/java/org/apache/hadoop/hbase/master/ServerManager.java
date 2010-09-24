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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.util.Threads;

/**
 * The ServerManager class manages info about region servers - HServerInfo,
 * load numbers, dying servers, etc.
 * <p>
 * Maintains lists of online and dead servers.  Processes the startups,
 * shutdowns, and deaths of region servers.
 * <p>
 * Servers are distinguished in two different ways.  A given server has a
 * location, specified by hostname and port, and of which there can only be one
 * online at any given time.  A server instance is specified by the location
 * (hostname and port) as well as the startcode (timestamp from when the server
 * was started).  This is used to differentiate a restarted instance of a given
 * server from the original instance.
 */
public class ServerManager {
  private static final Log LOG = LogFactory.getLog(ServerManager.class);

  // Set if we are to shutdown the cluster.
  private volatile boolean clusterShutdown = false;

  /** The map of known server names to server info */
  private final Map<String, HServerInfo> onlineServers =
    new ConcurrentHashMap<String, HServerInfo>();

  // TODO: This is strange to have two maps but HSI above is used on both sides
  /**
   * Map from full server-instance name to the RPC connection for this server.
   */
  private final Map<String, HRegionInterface> serverConnections =
    new HashMap<String, HRegionInterface>();

  private final Server master;
  private final MasterServices services;

  private final ServerMonitor serverMonitorThread;

  private final LogCleaner logCleaner;

  // Reporting to track master metrics.
  private final MasterMetrics metrics;

  private final DeadServer deadservers = new DeadServer();

  /**
   * Dumps into log current stats on dead servers and number of servers
   * TODO: Make this a metric; dump metrics into log.
   */
  class ServerMonitor extends Chore {
    ServerMonitor(final int period, final Stoppable stopper) {
      super("ServerMonitor", period, stopper);
    }

    @Override
    protected void chore() {
      int numServers = countOfRegionServers();
      int numDeadServers = deadservers.size();
      double averageLoad = getAverageLoad();
      String deadServersList = deadservers.toString();
      LOG.info("regionservers=" + numServers +
        ", averageload=" + StringUtils.limitDecimalTo2(averageLoad) +
        ((numDeadServers > 0)?  (", deadservers=" + deadServersList): ""));
    }
  }

  /**
   * Constructor.
   * @param master
   * @param services
   */
  public ServerManager(final Server master, final MasterServices services) {
    this.master = master;
    this.services = services;
    Configuration c = master.getConfiguration();
    int monitorInterval = c.getInt("hbase.master.monitor.interval", 60 * 1000);
    this.metrics = new MasterMetrics(master.getServerName());
    this.serverMonitorThread = new ServerMonitor(monitorInterval, master);
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(this.serverMonitorThread, n + ".serverMonitor");
    this.logCleaner =
      new LogCleaner(c.getInt("hbase.master.cleaner.interval", 60 * 1000),
        master, c, this.services.getMasterFileSystem().getFileSystem(),
        this.services.getMasterFileSystem().getOldLogDir());
    Threads.setDaemonThreadRunning(logCleaner, n + ".oldLogCleaner");
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param serverInfo
   * @throws IOException
   */
  void regionServerStartup(final HServerInfo serverInfo)
  throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddresstoServerInfo.  If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.
    HServerInfo info = new HServerInfo(serverInfo);
    String hostAndPort = info.getServerAddress().toString();
    HServerInfo existingServer =
      haveServerWithSameHostAndPortAlready(info.getHostnamePort());
    if (existingServer != null) {
      String message = "Server start rejected; we already have " + hostAndPort +
        " registered; existingServer=" + existingServer + ", newServer=" + info;
      LOG.info(message);
      if (existingServer.getStartCode() < info.getStartCode()) {
        LOG.info("Triggering server recovery; existingServer looks stale");
        expireServer(existingServer);
      }
      throw new PleaseHoldException(message);
    }
    checkIsDead(info.getServerName(), "STARTUP");
    LOG.info("Received start message from: " + info.getServerName());
    recordNewServer(info);
  }

  private HServerInfo haveServerWithSameHostAndPortAlready(final String hostnamePort) {
    synchronized (this.onlineServers) {
      for (Map.Entry<String, HServerInfo> e: this.onlineServers.entrySet()) {
        if (e.getValue().getHostnamePort().equals(hostnamePort)) {
          return e.getValue();
        }
      }
    }
    return null;
  }

  /**
   * If this server is on the dead list, reject it with a LeaseStillHeldException
   * @param serverName Server name formatted as host_port_startcode.
   * @param what START or REPORT
   * @throws LeaseStillHeldException
   */
  private void checkIsDead(final String serverName, final String what)
  throws YouAreDeadException {
    if (!this.deadservers.isDeadServer(serverName)) return;
    String message = "Server " + what + " rejected; currently processing " +
      serverName + " as dead server";
    LOG.debug(message);
    throw new YouAreDeadException(message);
  }

  /**
   * Adds the HSI to the RS list and creates an empty load
   * @param info The region server informations
   */
  public void recordNewServer(HServerInfo info) {
    recordNewServer(info, false, null);
  }

  /**
   * Adds the HSI to the RS list
   * @param info The region server informations
   * @param useInfoLoad True if the load from the info should be used
   *                    like under a master failover
   */
  void recordNewServer(HServerInfo info, boolean useInfoLoad,
      HRegionInterface hri) {
    HServerLoad load = useInfoLoad ? info.getLoad() : new HServerLoad();
    String serverName = info.getServerName();
    info.setLoad(load);
    // TODO: Why did we update the RS location ourself?  Shouldn't RS do this?
    // masterStatus.getZooKeeper().updateRSLocationGetWatch(info, watcher);
    this.onlineServers.put(serverName, info);
    if (hri == null) {
      serverConnections.remove(serverName);
    } else {
      serverConnections.put(serverName, hri);
    }
  }

  /**
   * Called to process the messages sent from the region server to the master
   * along with the heart beat.
   *
   * @param serverInfo
   * @param msgs
   * @param mostLoadedRegions Array of regions the region server is submitting
   * as candidates to be rebalanced, should it be overloaded
   * @return messages from master to region server indicating what region
   * server should do.
   *
   * @throws IOException
   */
  HMsg [] regionServerReport(final HServerInfo serverInfo,
    final HMsg [] msgs, final HRegionInfo[] mostLoadedRegions)
  throws IOException {
    // Be careful. This method does returns in the middle.
    HServerInfo info = new HServerInfo(serverInfo);

    // Check if dead.  If it is, it'll get a 'You Are Dead!' exception.
    checkIsDead(info.getServerName(), "REPORT");

    // If we don't know this server, tell it shutdown.
    HServerInfo storedInfo = this.onlineServers.get(info.getServerName());
    if (storedInfo == null) {
      LOG.warn("Received report from unknown server -- telling it " +
        "to " + HMsg.Type.STOP_REGIONSERVER + ": " + info.getServerName());
      return HMsg.STOP_REGIONSERVER_ARRAY;
    }

    // Check startcodes
    if (raceThatShouldNotHappenAnymore(storedInfo, info)) {
      return HMsg.STOP_REGIONSERVER_ARRAY;
    }

    for (HMsg msg: msgs) {
      LOG.info("Received " + msg);
      switch (msg.getType()) {
      case REGION_SPLIT:
        this.services.getAssignmentManager().handleSplitReport(serverInfo,
            msg.getRegionInfo(), msg.getDaughterA(), msg.getDaughterB());
        break;

        default:
          LOG.error("Unhandled msg type " + msg);
      }
    }

    HMsg [] reply = null;
    int numservers = countOfRegionServers();
    if (this.clusterShutdown) {
      if (numservers <= 2) {
        // Shutdown needs to be staggered; the meta regions need to close last
        // in case they need to be updated during the close melee.  If <= 2
        // servers left, then these are the two that were carrying root and meta
        // most likely (TODO: This presumes unsplittable meta -- FIX). Tell
        // these servers can shutdown now too.
        reply = HMsg.STOP_REGIONSERVER_ARRAY;
      }
    }
    return processRegionServerAllsWell(info, mostLoadedRegions, reply);
  }

  private boolean raceThatShouldNotHappenAnymore(final HServerInfo storedInfo,
      final HServerInfo reportedInfo) {
    if (storedInfo.getStartCode() != reportedInfo.getStartCode()) {
      // TODO: I don't think this possible any more.  We check startcodes when
      // server comes in on regionServerStartup -- St.Ack
      // This state is reachable if:
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      // The answer is to ask A to shut down for good.
      LOG.warn("Race condition detected: " + reportedInfo.getServerName());
      synchronized (this.onlineServers) {
        removeServerInfo(reportedInfo.getServerName());
        notifyOnlineServers();
      }
      return true;
    }
    return false;
  }

  /**
   *  RegionServer is checking in, no exceptional circumstances
   * @param serverInfo
   * @param mostLoadedRegions
   * @param msgs
   * @return
   * @throws IOException
   */
  private HMsg[] processRegionServerAllsWell(HServerInfo serverInfo,
      final HRegionInfo[] mostLoadedRegions, HMsg[] msgs)
  throws IOException {
    // Refresh the info object and the load information
    this.onlineServers.put(serverInfo.getServerName(), serverInfo);
    HServerLoad load = serverInfo.getLoad();
    if (load != null && this.metrics != null) {
      this.metrics.incrementRequests(load.getNumberOfRequests());
    }
    // No more piggyback messages on heartbeats for other stuff
    return msgs;
  }

  /**
   * @param serverName
   * @return True if we removed server from the list.
   */
  private boolean removeServerInfo(final String serverName) {
    HServerInfo info = this.onlineServers.remove(serverName);
    if (info != null) {
      return true;
    }
    return false;
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  public double getAverageLoad() {
    int totalLoad = 0;
    int numServers = 0;
    double averageLoad = 0.0;
    for (HServerInfo hsi : onlineServers.values()) {
        numServers++;
        totalLoad += hsi.getLoad().getNumberOfRegions();
    }
    averageLoad = (double)totalLoad / (double)numServers;
    return averageLoad;
  }

  /** @return the count of active regionservers */
  int countOfRegionServers() {
    // Presumes onlineServers is a concurrent map
    return this.onlineServers.size();
  }

  /**
   * @param name server name
   * @return HServerInfo for the given server address
   */
  public HServerInfo getServerInfo(String name) {
    return this.onlineServers.get(name);
  }

  /**
   * @return Read-only map of servers to serverinfo
   */
  public Map<String, HServerInfo> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  public Set<String> getDeadServers() {
    return this.deadservers.clone();
  }

  /**
   * @param hsa
   * @return The HServerInfo whose HServerAddress is <code>hsa</code> or null
   * if nothing found.
   */
  public HServerInfo getHServerInfo(final HServerAddress hsa) {
    synchronized(this.onlineServers) {
      // TODO: This is primitive.  Do a better search.
      for (Map.Entry<String, HServerInfo> e: this.onlineServers.entrySet()) {
        if (e.getValue().getServerAddress().equals(hsa)) {
          return e.getValue();
        }
      }
    }
    return null;
  }

  private void notifyOnlineServers() {
    synchronized (this.onlineServers) {
      this.onlineServers.notifyAll();
    }
  }

  /*
   * Wait on regionservers to report in
   * with {@link #regionServerReport(HServerInfo, HMsg[])} so they get notice
   * the master is going down.  Waits until all region servers come back with
   * a MSG_REGIONSERVER_STOP.
   */
  void letRegionServersShutdown() {
    synchronized (onlineServers) {
      while (onlineServers.size() > 0) {
        LOG.info("Waiting on following regionserver(s) to go down " +
          this.onlineServers.values());
        try {
          this.onlineServers.wait(1000);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }

  /*
   * Expire the passed server.  Add it to list of deadservers and queue a
   * shutdown processing.
   */
  public synchronized void expireServer(final HServerInfo hsi) {
    // First check a server to expire.  ServerName is of the form:
    // <hostname> , <port> , <startcode>
    String serverName = hsi.getServerName();
    HServerInfo info = this.onlineServers.get(serverName);
    if (info == null) {
      LOG.warn("Received expiration of " + hsi.getServerName() +
        " but server is not currently online");
      return;
    }
    if (this.deadservers.contains(serverName)) {
      // TODO: Can this happen?  It shouldn't be online in this case?
      LOG.warn("Received expiration of " + hsi.getServerName() +
          " but server shutdown is already in progress");
      return;
    }
    // Remove the server from the known servers lists and update load info
    this.onlineServers.remove(serverName);
    this.serverConnections.remove(serverName);
    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (this.clusterShutdown) {
      LOG.info("Cluster shutdown set; " + hsi.getServerName() +
        " expired; onlineServers=" + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return;
    }
    this.services.getExecutorService().submit(new ServerShutdownHandler(this.master,
        this.services, deadservers, info));
    LOG.debug("Added=" + serverName +
      " to dead servers, submitted shutdown handler to be executed");
  }

  // RPC methods to region servers

  /**
   * Sends an OPEN RPC to the specified server to open the specified region.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   * @param server server to open a region
   * @param regionName region to open
   */
  public void sendRegionOpen(HServerInfo server, HRegionInfo region) {
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.getServerName()
          + " failed because no RPC connection found to this server");
      return;
    }
    hri.openRegion(region);
  }

  /**
   * Sends an CLOSE RPC to the specified server to close the specified region.
   * <p>
   * A region server could reject the close request because it either does not
   * have the specified region or the region is being split.
   * @param server server to open a region
   * @param regionName region to open
   * @return true if server acknowledged close, false if not
   * @throws NotServingRegionException
   */
  public void sendRegionClose(HServerInfo server, HRegionInfo region)
  throws NotServingRegionException {
    HRegionInterface hri = getServerConnection(server);
    if(hri == null) {
      LOG.warn("Attempting to send CLOSE RPC to server " + server.getServerName()
          + " failed because no RPC connection found to this server");
      return;
    }
    hri.closeRegion(region);
  }

  private HRegionInterface getServerConnection(HServerInfo info) {
    try {
      HConnection connection =
        HConnectionManager.getConnection(this.master.getConfiguration());
      HRegionInterface hri = serverConnections.get(info.getServerName());
      if (hri == null) {
        LOG.debug("New connection to " + info.getServerName());
        hri = connection.getHRegionConnection(info.getServerAddress(), false);
        serverConnections.put(info.getServerName(), hri);
      }
      return hri;
    } catch (IOException e) {
      LOG.error("Error connecting to region server", e);
      throw new RuntimeException("Fatal error connection to RS", e);
    }
  }

  /**
   * Waits for the regionservers to report in.
   * @throws InterruptedException 
   */
  public void waitForRegionServers()
  throws InterruptedException {
    long interval = this.master.getConfiguration().
      getLong("hbase.master.wait.on.regionservers.interval", 3000);
    // So, number of regionservers > 0 and its been n since last check in, break,
    // else just stall here
    int count = 0;
    for (int oldcount = countOfRegionServers(); !this.master.isStopped();) {
      Thread.sleep(interval);
      count = countOfRegionServers();
      if (count == oldcount && count > 0) break;
      if (count == 0) {
        LOG.info("Waiting on regionserver(s) to checkin");
      } else {
        LOG.info("Waiting on regionserver(s) count to settle; currently=" + count);
      }
      oldcount = count;
    }
    LOG.info("Exiting wait on regionserver(s) to checkin; count=" + count +
      ", stopped=" + this.master.isStopped());
  }

  public List<HServerInfo> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    return new ArrayList<HServerInfo>(onlineServers.values());
  }

  public boolean isServerOnline(String serverName) {
    return onlineServers.containsKey(serverName);
  }

  public void shutdownCluster() {
    this.clusterShutdown = true;
    this.master.stop("Cluster shutdown requested");
  }

  public boolean isClusterShutdown() {
    return this.clusterShutdown;
  }
}
