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
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.handler.MetaServerShutdownHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;

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

  // Reporting to track master metrics.
  private final MasterMetrics metrics;

  private final DeadServer deadservers;

  private final long maxSkew;

  /**
   * Constructor.
   * @param master
   * @param services
   * @param metrics
   */
  public ServerManager(final Server master, final MasterServices services,
      MasterMetrics metrics) {
    this.master = master;
    this.services = services;
    this.metrics = metrics;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    this.deadservers =
      new DeadServer(c.getInt("hbase.master.maxdeadservers", 100));
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param serverInfo
   * @param serverCurrentTime The current time of the region server in ms
   * @throws IOException
   */
  void regionServerStartup(final HServerInfo serverInfo, long serverCurrentTime)
  throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddresstoServerInfo.  If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.
    HServerInfo info = new HServerInfo(serverInfo);
    checkIsDead(info.getServerName(), "STARTUP");
    checkAlreadySameHostPort(info);
    checkClockSkew(info, serverCurrentTime);
    recordNewServer(info, false, null);
  }

  /**
   * Test to see if we have a server of same host and port already.
   * @param serverInfo
   * @throws PleaseHoldException
   */
  void checkAlreadySameHostPort(final HServerInfo serverInfo)
  throws PleaseHoldException {
    String hostAndPort = serverInfo.getServerAddress().toString();
    HServerInfo existingServer =
      haveServerWithSameHostAndPortAlready(serverInfo.getHostnamePort());
    if (existingServer != null) {
      String message = "Server start rejected; we already have " + hostAndPort +
        " registered; existingServer=" + existingServer + ", newServer=" + serverInfo;
      LOG.info(message);
      if (existingServer.getStartCode() < serverInfo.getStartCode()) {
        LOG.info("Triggering server recovery; existingServer " +
          existingServer.getServerName() + " looks stale");
        expireServer(existingServer);
      }
      throw new PleaseHoldException(message);
    }
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
   * Checks if the clock skew between the server and the master. If the clock
   * skew is too much it will throw an Exception.
   * @throws ClockOutOfSyncException
   */
  private void checkClockSkew(final HServerInfo serverInfo,
      final long serverCurrentTime)
  throws ClockOutOfSyncException {
    long skew = System.currentTimeMillis() - serverCurrentTime;
    if (skew > maxSkew) {
      String message = "Server " + serverInfo.getServerName() + " has been " +
        "rejected; Reported time is too far out of sync with master.  " +
        "Time difference of " + skew + "ms > max allowed of " + maxSkew + "ms";
      LOG.warn(message);
      throw new ClockOutOfSyncException(message);
    }
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
   * Adds the HSI to the RS list
   * @param info The region server informations
   * @param useInfoLoad True if the load from the info should be used; e.g.
   * under a master failover
   * @param hri Region interface.  Can be null.
   */
  void recordNewServer(HServerInfo info, boolean useInfoLoad,
      HRegionInterface hri) {
    HServerLoad load = useInfoLoad? info.getLoad(): new HServerLoad();
    String serverName = info.getServerName();
    LOG.info("Registering server=" + serverName + ", regionCount=" +
      load.getLoad() + ", userLoad=" + useInfoLoad);
    info.setLoad(load);
    // TODO: Why did we update the RS location ourself?  Shouldn't RS do this?
    // masterStatus.getZooKeeper().updateRSLocationGetWatch(info, watcher);
    // -- If I understand the question, the RS does not update the location
    // because could be disagreement over locations because of DNS issues; only
    // master does DNS now -- St.Ack 20100929.
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
      // Maybe we already have this host+port combo and its just different
      // start code?
      checkAlreadySameHostPort(info);
      // Just let the server in. Presume master joining a running cluster.
      // recordNewServer is what happens at the end of reportServerStartup.
      // The only thing we are skipping is passing back to the regionserver
      // the HServerInfo to use. Here we presume a master has already done
      // that so we'll press on with whatever it gave us for HSI.
      recordNewServer(info, true, null);
      // If msgs, put off their processing but this is not enough because
      // its possible that the next time the server reports in, we'll still
      // not be up and serving. For example, if a split, we'll need the
      // regions and servers setup in the master before the below
      // handleSplitReport will work. TODO: FIx!!
      if (msgs.length > 0)
        throw new PleaseHoldException("FIX! Putting off " +
          "message processing because not yet rwady but possible we won't be " +
          "ready next on next report");
    }

    for (HMsg msg: msgs) {
      LOG.info("Received " + msg + " from " + serverInfo.getServerName());
      switch (msg.getType()) {
        default:
          LOG.error("Unhandled msg type " + msg);
      }
    }

    HMsg [] reply = null;
    if (this.clusterShutdown) {
      if (isOnlyMetaRegionServersOnline()) {
        LOG.info("Only catalog regions remaining; running unassign");
        // The only remaining regions are catalog regions.
        // Shutdown needs to be staggered; the meta regions need to close last
        // in case they need to be updated during the close melee. If only
        // catalog reigons remaining, tell them they can go down now too.  On
        // close of region, the regionservers should then shut themselves down.
        this.services.getAssignmentManager().unassignCatalogRegions();
      }
    }
    return processRegionServerAllsWell(info, mostLoadedRegions, reply);
  }

  /**
   * @return True if all online servers are carrying one or more catalog
   * regions, there are no servers online carrying user regions only
   */
  private boolean isOnlyMetaRegionServersOnline() {
    List<HServerInfo> onlineServers = getOnlineServersList();
    for (HServerInfo hsi: onlineServers) {
      if (!this.services.getAssignmentManager().isMetaRegionServer(hsi)) {
        return false;
      }
    }
    return true;
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
   * Checks if any dead servers are currently in progress.
   * @return true if any RS are being processed as dead, false if not
   */
  public boolean areDeadServersInProgress() {
    return this.deadservers.areDeadServersInProgress();
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
        StringBuilder sb = new StringBuilder();
        for (String key: this.onlineServers.keySet()) {
          if (sb.length() > 0) {
            sb.append(", ");
          }
          sb.append(key);
        }
        LOG.info("Waiting on regionserver(s) to go down " + sb.toString());
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
    // Remove the server from the known servers lists and update load info BUT
    // add to deadservers first; do this so it'll show in dead servers list if
    // not in online servers list.
    this.deadservers.add(serverName);
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
    CatalogTracker ct = this.master.getCatalogTracker();
    // Was this server carrying root?
    boolean carryingRoot;
    try {
      HServerAddress address = ct.getRootLocation();
      carryingRoot = address != null &&
        hsi.getServerAddress().equals(address);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted");
      return;
    }
    // Was this server carrying meta?  Can't ask CatalogTracker because it
    // may have reset the meta location as null already (it may have already
    // run into fact that meta is dead).  I can ask assignment manager. It
    // has an inmemory list of who has what.  This list will be cleared as we
    // process the dead server but should be  find asking it now.
    HServerAddress address = ct.getMetaLocation();
    boolean carryingMeta =
      address != null && hsi.getServerAddress().equals(address);
    if (carryingRoot || carryingMeta) {
      this.services.getExecutorService().submit(new MetaServerShutdownHandler(this.master,
        this.services, this.deadservers, info, carryingRoot, carryingMeta));
    } else {
      this.services.getExecutorService().submit(new ServerShutdownHandler(this.master,
        this.services, this.deadservers, info));
    }
    LOG.debug("Added=" + serverName +
      " to dead servers, submitted shutdown handler to be executed, root=" +
        carryingRoot + ", meta=" + carryingMeta);
  }

  // RPC methods to region servers

  /**
   * Sends an OPEN RPC to the specified server to open the specified region.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   * @param server server to open a region
   * @param region region to open
   */
  public void sendRegionOpen(HServerInfo server, HRegionInfo region)
  throws IOException {
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.getServerName()
          + " failed because no RPC connection found to this server");
      return;
    }
    hri.openRegion(region);
  }

  /**
   * Sends an OPEN RPC to the specified server to open the specified region.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   * @param server server to open a region
   * @param regions regions to open
   */
  public void sendRegionOpen(HServerInfo server, List<HRegionInfo> regions)
  throws IOException {
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.getServerName()
          + " failed because no RPC connection found to this server");
      return;
    }
    hri.openRegions(regions);
  }

  /**
   * Sends an CLOSE RPC to the specified server to close the specified region.
   * <p>
   * A region server could reject the close request because it either does not
   * have the specified region or the region is being split.
   * @param server server to open a region
   * @param region region to open
   * @return true if server acknowledged close, false if not
   * @throws IOException
   */
  public boolean sendRegionClose(HServerInfo server, HRegionInfo region)
  throws IOException {
    if (server == null) throw new NullPointerException("Passed server is null");
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      throw new IOException("Attempting to send CLOSE RPC to server " +
        server.getServerName() + " for region " +
        region.getRegionNameAsString() +
        " failed because no RPC connection found to this server");
    }
    return hri.closeRegion(region);
  }

  /**
   * @param info
   * @return
   * @throws IOException
   * @throws RetriesExhaustedException wrapping a ConnectException if failed
   * putting up proxy.
   */
  private HRegionInterface getServerConnection(HServerInfo info)
  throws IOException {
    HConnection connection =
      HConnectionManager.getConnection(this.master.getConfiguration());
    HRegionInterface hri = serverConnections.get(info.getServerName());
    if (hri == null) {
      LOG.debug("New connection to " + info.getServerName());
      hri = connection.getHRegionConnection(info.getServerAddress(), false);
      this.serverConnections.put(info.getServerName(), hri);
    }
    return hri;
  }

  /**
   * Waits for the regionservers to report in.
   * @return Count of regions out on cluster
   * @throws InterruptedException
   */
  public int waitForRegionServers()
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
    // Count how many regions deployed out on cluster.  If fresh start, it'll
    // be none but if not a fresh start, we'll have registered servers when
    // they came in on the {@link #regionServerReport(HServerInfo)} as opposed to
    // {@link #regionServerStartup(HServerInfo)} and it'll be carrying an
    // actual server load.
    int regionCount = 0;
    for (Map.Entry<String, HServerInfo> e: this.onlineServers.entrySet()) {
      HServerLoad load = e.getValue().getLoad();
      if (load != null) regionCount += load.getLoad();
    }
    LOG.info("Exiting wait on regionserver(s) to checkin; count=" + count +
      ", stopped=" + this.master.isStopped() +
      ", count of regions out on cluster=" + regionCount);
    return regionCount;
  }

  /**
   * @return A copy of the internal list of online servers.
   */
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

  /**
   * Stop the ServerManager.  Currently does nothing.
   */
  public void stop() {

  }
}
