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
import java.net.InetAddress;
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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.handler.MetaServerShutdownHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;

/**
 * The ServerManager class manages info about region servers.
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

  /** Map of registered servers to their current load */
  private final Map<ServerName, HServerLoad> onlineServers =
    new ConcurrentHashMap<ServerName, HServerLoad>();

  // TODO: This is strange to have two maps but HSI above is used on both sides
  /**
   * Map from full server-instance name to the RPC connection for this server.
   */
  private final Map<ServerName, HRegionInterface> serverConnections =
    new HashMap<ServerName, HRegionInterface>();

  private final Server master;
  private final MasterServices services;
  private final HConnection connection;

  private final DeadServer deadservers;

  private final long maxSkew;

  /**
   * Constructor.
   * @param master
   * @param services
   * @throws ZooKeeperConnectionException
   */
  public ServerManager(final Server master, final MasterServices services)
      throws ZooKeeperConnectionException {
    this(master, services, true);
  }

  ServerManager(final Server master, final MasterServices services,
      final boolean connect) throws ZooKeeperConnectionException {
    this.master = master;
    this.services = services;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    this.deadservers = new DeadServer();
    this.connection = connect ? HConnectionManager.getConnection(c) : null;
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param ia The remote address
   * @param port The remote port
   * @param serverStartcode
   * @param serverCurrentTime The current time of the region server in ms
   * @return The ServerName we know this server as.
   * @throws IOException
   */
  ServerName regionServerStartup(final InetAddress ia, final int port,
    final long serverStartcode, long serverCurrentTime)
  throws IOException {
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddresstoServerInfo.  If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.
    ServerName sn = new ServerName(ia.getHostName(), port, serverStartcode);
    checkClockSkew(sn, serverCurrentTime);
    checkIsDead(sn, "STARTUP");
    checkAlreadySameHostPort(sn);
    recordNewServer(sn, HServerLoad.EMPTY_HSERVERLOAD);
    return sn;
  }

  void regionServerReport(ServerName sn, HServerLoad hsl)
  throws YouAreDeadException, PleaseHoldException {
    checkIsDead(sn, "REPORT");
    if (!this.onlineServers.containsKey(sn)) {
      // Already have this host+port combo and its just different start code?
      checkAlreadySameHostPort(sn);
      // Just let the server in. Presume master joining a running cluster.
      // recordNewServer is what happens at the end of reportServerStartup.
      // The only thing we are skipping is passing back to the regionserver
      // the ServerName to use. Here we presume a master has already done
      // that so we'll press on with whatever it gave us for ServerName.
      recordNewServer(sn, hsl);
    } else {
      this.onlineServers.put(sn, hsl);
    }
  }

  /**
   * Test to see if we have a server of same host and port already.
   * @param serverName
   * @throws PleaseHoldException
   */
  void checkAlreadySameHostPort(final ServerName serverName)
  throws PleaseHoldException {
    ServerName existingServer =
      ServerName.findServerWithSameHostnamePort(getOnlineServersList(), serverName);
    if (existingServer != null) {
      String message = "Server serverName=" + serverName +
        " rejected; we already have " + existingServer.toString() +
        " registered with same hostname and port";
      LOG.info(message);
      if (existingServer.getStartcode() < serverName.getStartcode()) {
        LOG.info("Triggering server recovery; existingServer " +
          existingServer + " looks stale");
        expireServer(existingServer);
      }
      throw new PleaseHoldException(message);
    }
  }

  /**
   * Checks if the clock skew between the server and the master. If the clock
   * skew is too much it will throw an Exception.
   * @param serverName Incoming servers's name
   * @param serverCurrentTime
   * @throws ClockOutOfSyncException
   */
  private void checkClockSkew(final ServerName serverName,
      final long serverCurrentTime)
  throws ClockOutOfSyncException {
    long skew = System.currentTimeMillis() - serverCurrentTime;
    if (skew > maxSkew) {
      String message = "Server " + serverName + " has been " +
        "rejected; Reported time is too far out of sync with master.  " +
        "Time difference of " + skew + "ms > max allowed of " + maxSkew + "ms";
      LOG.warn(message);
      throw new ClockOutOfSyncException(message);
    }
  }

  /**
   * If this server is on the dead list, reject it with a YouAreDeadException.
   * If it was dead but came back with a new start code, remove the old entry
   * from the dead list.
   * @param serverName
   * @param what START or REPORT
   * @throws YouAreDeadException
   */
  private void checkIsDead(final ServerName serverName, final String what)
      throws YouAreDeadException {
    if (this.deadservers.isDeadServer(serverName)) {
      // host name, port and start code all match with existing one of the
      // dead servers. So, this server must be dead.
      String message = "Server " + what + " rejected; currently processing " +
          serverName + " as dead server";
      LOG.debug(message);
      throw new YouAreDeadException(message);
    }

    if (this.deadservers.cleanPreviousInstance(serverName)) {
      // This server has now become alive after we marked it as dead.
      // We removed it's previous entry from the dead list to reflect it.
      LOG.debug("Server " + serverName + " came back up, removed it from the" +
          " dead servers list");
    }
  }

  /**
   * Adds the onlineServers list.
   * @param hsl
   * @param serverName The remote servers name.
   */
  void recordNewServer(final ServerName serverName, final  HServerLoad hsl) {
    LOG.info("Registering server=" + serverName);
    this.onlineServers.put(serverName, hsl);
    this.serverConnections.remove(serverName);
  }

  /**
   * @param serverName
   * @return HServerLoad if serverName is known else null
   */
  public HServerLoad getLoad(final ServerName serverName) {
    return this.onlineServers.get(serverName);
  }

  /**
   * @param serverName
   * @return HServerLoad if serverName is known else null
   * @deprecated Use {@link #getLoad(HServerAddress)}
   */
  public HServerLoad getLoad(final HServerAddress address) {
    ServerName sn = new ServerName(address.toString(), -1);
    ServerName actual =
      ServerName.findServerWithSameHostnamePort(this.getOnlineServersList(), sn);
    return actual == null? null: getLoad(actual);
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
    for (HServerLoad hsl: this.onlineServers.values()) {
        numServers++;
        totalLoad += hsl.getNumberOfRegions();
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
   * @return Read-only map of servers to serverinfo
   */
  public Map<ServerName, HServerLoad> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  public Set<ServerName> getDeadServers() {
    return this.deadservers.clone();
  }

  /**
   * Checks if any dead servers are currently in progress.
   * @return true if any RS are being processed as dead, false if not
   */
  public boolean areDeadServersInProgress() {
    return this.deadservers.areDeadServersInProgress();
  }

  void letRegionServersShutdown() {
    synchronized (onlineServers) {
      while (!onlineServers.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        for (ServerName key: this.onlineServers.keySet()) {
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
  public synchronized void expireServer(final ServerName serverName) {
    if (!this.onlineServers.containsKey(serverName)) {
      LOG.warn("Received expiration of " + serverName +
        " but server is not currently online");
      return;
    }
    if (this.deadservers.contains(serverName)) {
      // TODO: Can this happen?  It shouldn't be online in this case?
      LOG.warn("Received expiration of " + serverName +
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
      LOG.info("Cluster shutdown set; " + serverName +
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
      ServerName address = ct.getRootLocation();
      carryingRoot = address != null && address.equals(serverName);
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
    ServerName address = ct.getMetaLocation();
    boolean carryingMeta = address != null && address.equals(serverName);
    if (carryingRoot || carryingMeta) {
      this.services.getExecutorService().submit(new MetaServerShutdownHandler(this.master,
        this.services, this.deadservers, serverName, carryingRoot, carryingMeta));
    } else {
      this.services.getExecutorService().submit(new ServerShutdownHandler(this.master,
        this.services, this.deadservers, serverName));
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
  public void sendRegionOpen(final ServerName server, HRegionInfo region)
  throws IOException {
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.toString() +
        " failed because no RPC connection found to this server");
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
  public void sendRegionOpen(ServerName server, List<HRegionInfo> regions)
  throws IOException {
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.toString() +
        " failed because no RPC connection found to this server");
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
  public boolean sendRegionClose(ServerName server, HRegionInfo region)
  throws IOException {
    if (server == null) throw new NullPointerException("Passed server is null");
    HRegionInterface hri = getServerConnection(server);
    if (hri == null) {
      throw new IOException("Attempting to send CLOSE RPC to server " +
        server.toString() + " for region " +
        region.getRegionNameAsString() +
        " failed because no RPC connection found to this server");
    }
    return hri.closeRegion(region);
  }

  /**
   * @param sn
   * @return
   * @throws IOException
   * @throws RetriesExhaustedException wrapping a ConnectException if failed
   * putting up proxy.
   */
  private HRegionInterface getServerConnection(final ServerName sn)
  throws IOException {
    HRegionInterface hri = this.serverConnections.get(sn.toString());
    if (hri == null) {
      LOG.debug("New connection to " + sn.toString());
      hri = this.connection.getHRegionConnection(sn.getHostname(), sn.getPort());
      this.serverConnections.put(sn, hri);
    }
    return hri;
  }

  /**
   * Waits for the regionservers to report in.
   * @throws InterruptedException
   */
  public void waitForRegionServers(MonitoredTask status)
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

      String msg;
      if (count == 0) {
        msg = "Waiting on regionserver(s) to checkin";
      } else {
        msg = "Waiting on regionserver(s) count to settle; currently=" + count;
      }
      LOG.info(msg);
      status.setStatus(msg);
      oldcount = count;
    }
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    // TODO: FIX. THIS IS POPULAR CALL.
    return new ArrayList<ServerName>(this.onlineServers.keySet());
  }

  public boolean isServerOnline(ServerName serverName) {
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
   * Stop the ServerManager.  Currently closes the connection to the master.
   */
  public void stop() {
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("Attempt to close connection to master failed", e);
      }
    }
  }
}
