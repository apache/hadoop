/**
 * Copyright 2008 The Apache Software Foundation
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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.HMsg.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * The ServerManager class manages info about region servers - HServerInfo, 
 * load numbers, dying servers, etc.
 */
class ServerManager implements HConstants {
  static final Log LOG =
    LogFactory.getLog(ServerManager.class.getName());
  private static final HMsg REGIONSERVER_QUIESCE =
    new HMsg(Type.MSG_REGIONSERVER_QUIESCE);
  private static final HMsg REGIONSERVER_STOP =
    new HMsg(Type.MSG_REGIONSERVER_STOP);
  private static final HMsg CALL_SERVER_STARTUP =
    new HMsg(Type.MSG_CALL_SERVER_STARTUP);
  private static final HMsg [] EMPTY_HMSG_ARRAY = new HMsg[0];
  
  private final AtomicInteger quiescedServers = new AtomicInteger(0);
  private final ZooKeeperWrapper zooKeeperWrapper;

  /** The map of known server names to server info */
  final Map<String, HServerInfo> serversToServerInfo =
    new ConcurrentHashMap<String, HServerInfo>();

  final Map<HServerAddress, HServerInfo> serverAddressToServerInfo =
      new ConcurrentHashMap<HServerAddress, HServerInfo>();
  
  /**
   * Set of known dead servers.  On znode expiration, servers are added here.
   * This is needed in case of a network partitioning where the server's lease
   * expires, but the server is still running. After the network is healed,
   * and it's server logs are recovered, it will be told to call server startup
   * because by then, its regions have probably been reassigned.
   */
  protected final Set<String> deadServers =
    Collections.synchronizedSet(new HashSet<String>());

  /** SortedMap server load -> Set of server names */
  final SortedMap<HServerLoad, Set<String>> loadToServers =
    Collections.synchronizedSortedMap(new TreeMap<HServerLoad, Set<String>>());

  /** Map of server names -> server load */
  final Map<String, HServerLoad> serversToLoad =
    new ConcurrentHashMap<String, HServerLoad>();  

  protected HMaster master;
  
  /* The regionserver will not be assigned or asked close regions if it
   * is currently opening >= this many regions.
   */
  private final int nobalancingCount;

  class ServerMonitor extends Chore {

    ServerMonitor(final int period, final AtomicBoolean stop) {
      super(period, stop);
    }

    protected void chore() {
      int numServers = serverAddressToServerInfo.size();
      int numDeadServers = deadServers.size();
      double averageLoad = getAverageLoad();
      LOG.info(numServers + " region servers, " + numDeadServers + 
        " dead, average load " + averageLoad);
      if (numDeadServers > 0) {
        LOG.info("DEAD [");
        for (String server: deadServers) {
          LOG.info("  " + server);
        }
        LOG.info("]");
      }
    }
    
  }

  ServerMonitor serverMonitorThread;

  /**
   * @param master
   */
  public ServerManager(HMaster master) {
    this.master = master;
    zooKeeperWrapper = master.getZooKeeperWrapper();
    this.nobalancingCount = master.getConfiguration().
      getInt("hbase.regions.nobalancing.count", 4);
    serverMonitorThread = new ServerMonitor(master.metaRescanInterval,
      master.shutdownRequested);
    serverMonitorThread.start();
  }
 
  /**
   * Let the server manager know a new regionserver has come online
   * @param serverInfo
   * @throws Leases.LeaseStillHeldException
   */
  public void regionServerStartup(final HServerInfo serverInfo) 
  throws Leases.LeaseStillHeldException {
    HServerInfo info = new HServerInfo(serverInfo);
    String serverName = HServerInfo.getServerName(info);
    if (serversToServerInfo.containsKey(serverName) ||
        deadServers.contains(serverName)) {
      LOG.debug("Server start was rejected: " + serverInfo);
      LOG.debug("serversToServerInfo.containsKey: " + serversToServerInfo.containsKey(serverName));
      LOG.debug("deadServers.contains: " + deadServers.contains(serverName));
      throw new Leases.LeaseStillHeldException(serverName);
    }
    Watcher watcher = new ServerExpirer(serverName, info.getServerAddress());
    zooKeeperWrapper.updateRSLocationGetWatch(info, watcher);
    
    LOG.info("Received start message from: " + serverName);
    // Go on to process the regionserver registration.
    HServerLoad load = serversToLoad.remove(serverName);
    if (load != null) {
      // The startup message was from a known server.
      // Remove stale information about the server's load.
      synchronized (loadToServers) {
        Set<String> servers = loadToServers.get(load);
        if (servers != null) {
          servers.remove(serverName);
          loadToServers.put(load, servers);
        }
      }
    }
    HServerInfo storedInfo = serversToServerInfo.remove(serverName);
    if (storedInfo != null && !master.closed.get()) {
      // The startup message was from a known server with the same name.
      // Timeout the old one right away.
      HServerAddress root = master.getRootRegionLocation();
      boolean rootServer = false;
      if (root != null && root.equals(storedInfo.getServerAddress())) {
        master.regionManager.unsetRootRegion();
        rootServer = true;
      }
      try {
        master.toDoQueue.put(
            new ProcessServerShutdown(master, storedInfo, rootServer));
      } catch (InterruptedException e) {
        LOG.error("Insertion into toDoQueue was interrupted", e);
      }
    }
    // record new server
    load = new HServerLoad();
    info.setLoad(load);
    serversToServerInfo.put(serverName, info);
    serverAddressToServerInfo.put(info.getServerAddress(), info);
    serversToLoad.put(serverName, load);
    synchronized (loadToServers) {
      Set<String> servers = loadToServers.get(load);
      if (servers == null) {
        servers = new HashSet<String>();
      }
      servers.add(serverName);
      loadToServers.put(load, servers);
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
  public HMsg [] regionServerReport(final HServerInfo serverInfo,
    final HMsg msgs[], final HRegionInfo[] mostLoadedRegions)
  throws IOException {
    HServerInfo info = new HServerInfo(serverInfo);
    if (isDead(info.getServerName())) {
      throw new Leases.LeaseStillHeldException(info.getServerName());
    }
    if (msgs.length > 0) {
      if (msgs[0].isType(HMsg.Type.MSG_REPORT_EXITING)) {
        processRegionServerExit(info, msgs);
        return EMPTY_HMSG_ARRAY;
      } else if (msgs[0].isType(HMsg.Type.MSG_REPORT_QUIESCED)) {
        LOG.info("Region server " + info.getServerName() + " quiesced");
        quiescedServers.incrementAndGet();
      }
    }

    if (master.shutdownRequested.get()) {
      if(quiescedServers.get() >= serversToServerInfo.size()) {
        // If the only servers we know about are meta servers, then we can
        // proceed with shutdown
        LOG.info("All user tables quiesced. Proceeding with shutdown");
        master.startShutdown();
      }

      if (!master.closed.get()) {
        if (msgs.length > 0 &&
            msgs[0].isType(HMsg.Type.MSG_REPORT_QUIESCED)) {
          // Server is already quiesced, but we aren't ready to shut down
          // return empty response
          return EMPTY_HMSG_ARRAY;
        }
        // Tell the server to stop serving any user regions
        return new HMsg [] {REGIONSERVER_QUIESCE};
      }
    }

    if (master.closed.get()) {
      // Tell server to shut down if we are shutting down.  This should
      // happen after check of MSG_REPORT_EXITING above, since region server
      // will send us one of these messages after it gets MSG_REGIONSERVER_STOP
      return new HMsg [] {REGIONSERVER_STOP};
    }

    HServerInfo storedInfo = serversToServerInfo.get(info.getServerName());
    if (storedInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("received server report from unknown server: " +
            info.getServerName());
      }

      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to start over and call regionServerStartup()
      return new HMsg[]{CALL_SERVER_STARTUP};
    } else if (storedInfo.getStartCode() != info.getStartCode()) {
      // This state is reachable if:
      //
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then 
      //    clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      //
      // The answer is to ask A to shut down for good.
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("region server race condition detected: " +
            info.getServerName());
      }

      synchronized (serversToServerInfo) {
        removeServerInfo(info.getServerName(), info.getServerAddress());
        serversToServerInfo.notifyAll();
      }
      
      return new HMsg[]{REGIONSERVER_STOP};
    } else {
      return processRegionServerAllsWell(info, mostLoadedRegions, msgs);
    }
  }

  /** Region server is exiting */
  private void processRegionServerExit(HServerInfo serverInfo, HMsg[] msgs) {
    synchronized (serversToServerInfo) {
      try {
        // HRegionServer is shutting down. 
        if (removeServerInfo(serverInfo.getServerName(),
            serverInfo.getServerAddress())) {
          // Only process the exit message if the server still has registered info.
          // Otherwise we could end up processing the server exit twice.
          LOG.info("Region server " + serverInfo.getServerName() +
            ": MSG_REPORT_EXITING");
          // Get all the regions the server was serving reassigned
          // (if we are not shutting down).
          if (!master.closed.get()) {
            for (int i = 1; i < msgs.length; i++) {
              LOG.info("Processing " + msgs[i] + " from " +
                  serverInfo.getServerName());
              HRegionInfo info = msgs[i].getRegionInfo();
              synchronized (master.regionManager) {
                if (info.isRootRegion()) {
                  master.regionManager.reassignRootRegion();
                } else {
                  if (info.isMetaTable()) {
                    master.regionManager.offlineMetaRegion(info.getStartKey());
                  }
                  if (!master.regionManager.isOfflined(
                      info.getRegionNameAsString())) {
                    master.regionManager.setUnassigned(info, true);
                  } else {
                    master.regionManager.removeRegion(info);
                  }
                }
              }
            }
          }
        }
        // We don't need to return anything to the server because it isn't
        // going to do any more work.
      } finally {
        serversToServerInfo.notifyAll();
      }
    }    
  }

  /**
   *  RegionServer is checking in, no exceptional circumstances
   * @param serverInfo
   * @param mostLoadedRegions
   * @param msgs
   * @return
   * @throws IOException
   */
  private HMsg[] processRegionServerAllsWell(HServerInfo serverInfo, HRegionInfo[] mostLoadedRegions, HMsg[] msgs)
  throws IOException {

    // Refresh the info object and the load information
    serverAddressToServerInfo.put(serverInfo.getServerAddress(), serverInfo);
    serversToServerInfo.put(serverInfo.getServerName(), serverInfo);

    HServerLoad load = serversToLoad.get(serverInfo.getServerName());
    if (load != null) {
      this.master.getMetrics().incrementRequests(load.getNumberOfRequests());
      if (!load.equals(serverInfo.getLoad())) {
        // We have previous information about the load on this server
        // and the load on this server has changed
        synchronized (loadToServers) {
          Set<String> servers = loadToServers.get(load);
          // Note that servers should never be null because loadToServers
          // and serversToLoad are manipulated in pairs
          servers.remove(serverInfo.getServerName());
          loadToServers.put(load, servers);
        }
      }
    }

    // Set the current load information
    load = serverInfo.getLoad();
    serversToLoad.put(serverInfo.getServerName(), load);
    synchronized (loadToServers) {
      Set<String> servers = loadToServers.get(load);
      if (servers == null) {
        servers = new HashSet<String>();
      }
      servers.add(serverInfo.getServerName());
      loadToServers.put(load, servers);
    }

    // Next, process messages for this server
    return processMsgs(serverInfo, mostLoadedRegions, msgs);
  }

  /*
   * Process all the incoming messages from a server that's contacted us.
   * Note that we never need to update the server's load information because
   * that has already been done in regionServerReport.
   * @param serverInfo
   * @param mostLoadedRegions
   * @param incomingMsgs
   * @return
   */
  private HMsg[] processMsgs(HServerInfo serverInfo,
      HRegionInfo[] mostLoadedRegions, HMsg incomingMsgs[]) { 
    ArrayList<HMsg> returnMsgs = new ArrayList<HMsg>();
    if (serverInfo.getServerAddress() == null) {
      throw new NullPointerException("Server address cannot be null; " +
        "hbase-958 debugging");
    }
    // Get reports on what the RegionServer did.
    // Be careful that in message processors we don't throw exceptions that
    // break the switch below because then we might drop messages on the floor.
    int openingCount = 0;
    for (int i = 0; i < incomingMsgs.length; i++) {
      HRegionInfo region = incomingMsgs[i].getRegionInfo();
      LOG.info("Received " + incomingMsgs[i] + " from " +
        serverInfo.getServerName() + "; " + i + " of " + incomingMsgs.length);
      switch (incomingMsgs[i].getType()) {
        case MSG_REPORT_PROCESS_OPEN:
          openingCount++;
          break;
        
        case MSG_REPORT_OPEN:
          processRegionOpen(serverInfo, region, returnMsgs);
          break;

        case MSG_REPORT_CLOSE:
          processRegionClose(region);
          break;

        case MSG_REPORT_SPLIT:
          processSplitRegion(region, incomingMsgs[++i], incomingMsgs[++i]);
          break;

        default:
          LOG.warn("Impossible state during message processing. Instruction: " +
            incomingMsgs[i].getType());
      }
    }

    synchronized (master.regionManager) {
      // Tell the region server to close regions that we have marked for closing.
      for (HRegionInfo i:
        master.regionManager.getMarkedToClose(serverInfo.getServerName())) {
        returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, i));
        // Transition the region from toClose to closing state
        master.regionManager.setPendingClose(i.getRegionNameAsString());
      }

      // Figure out what the RegionServer ought to do, and write back.
      
      // Should we tell it close regions because its overloaded?  If its
      // currently opening regions, leave it alone till all are open.
      if (openingCount < this.nobalancingCount) {
        this.master.regionManager.assignRegions(serverInfo, mostLoadedRegions,
            returnMsgs);
      }
      // Send any pending table actions.
      this.master.regionManager.applyActions(serverInfo, returnMsgs);
    }
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }
  
  /*
   * A region has split.
   *
   * @param region
   * @param splitA
   * @param splitB
   * @param returnMsgs
   */
  private void processSplitRegion(HRegionInfo region, HMsg splitA, HMsg splitB) {
    synchronized (master.regionManager) {
      // Cancel any actions pending for the affected region.
      // This prevents the master from sending a SPLIT message if the table
      // has already split by the region server. 
      master.regionManager.endActions(region.getRegionName());
      HRegionInfo newRegionA = splitA.getRegionInfo();
      master.regionManager.setUnassigned(newRegionA, false);
      HRegionInfo newRegionB = splitB.getRegionInfo();
      master.regionManager.setUnassigned(newRegionB, false);
      if (region.isMetaTable()) {
        // A meta region has split.
        master.regionManager.offlineMetaRegion(region.getStartKey());
        master.regionManager.incrementNumMetaRegions();
      }
    }
  }

  /*
   * Region server is reporting that a region is now opened
   * @param serverInfo
   * @param region
   * @param returnMsgs
   */
  private void processRegionOpen(HServerInfo serverInfo, 
      HRegionInfo region, ArrayList<HMsg> returnMsgs) {
    boolean duplicateAssignment = false;
    synchronized (master.regionManager) {
      if (!master.regionManager.isUnassigned(region) &&
          !master.regionManager.isPendingOpen(region.getRegionNameAsString())) {
        if (region.isRootRegion()) {
          // Root region
          HServerAddress rootServer = master.getRootRegionLocation();
          if (rootServer != null) {
            if (rootServer.compareTo(serverInfo.getServerAddress()) == 0) {
              // A duplicate open report from the correct server
              return;
            }
            // We received an open report on the root region, but it is
            // assigned to a different server
            duplicateAssignment = true;
          }
        } else {
          // Not root region. If it is not a pending region, then we are
          // going to treat it as a duplicate assignment, although we can't 
          // tell for certain that's the case.
          if (master.regionManager.isPendingOpen(
              region.getRegionNameAsString())) {
            // A duplicate report from the correct server
            return;
          }
          duplicateAssignment = true;
        }
      }
    
      if (duplicateAssignment) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("region server " + serverInfo.getServerAddress().toString()
              + " should not have opened region " + Bytes.toString(region.getRegionName()));
        }

        // This Region should not have been opened.
        // Ask the server to shut it down, but don't report it as closed.  
        // Otherwise the HMaster will think the Region was closed on purpose, 
        // and then try to reopen it elsewhere; that's not what we want.
        returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE_WITHOUT_REPORT,
            region, "Duplicate assignment".getBytes()));
      } else {
        if (region.isRootRegion()) {
          // it was assigned, and it's not a duplicate assignment, so take it out 
          // of the unassigned list.
          master.regionManager.removeRegion(region);

          // Store the Root Region location (in memory)
          HServerAddress rootServer = serverInfo.getServerAddress();
          if (master.regionManager.inSafeMode()) {
            master.connection.setRootRegionLocation(
                new HRegionLocation(region, rootServer));
          }
          master.regionManager.setRootRegionLocation(rootServer);
        } else {
          // Note that the table has been assigned and is waiting for the
          // meta table to be updated.
          master.regionManager.setOpen(region.getRegionNameAsString());
          // Queue up an update to note the region location.  Do inside
          // a retry loop in case interrupted.
          boolean succeeded = false;
          for (int i = 0; i < 10; i++) {
            try {
              master.toDoQueue.
                put(new ProcessRegionOpen(master, serverInfo, region));
              succeeded = true;
            } catch (InterruptedException e) {
              LOG.warn("Putting into toDoQueue was interrupted.", e);
            }
          }
          if (!succeeded) {
            LOG.warn("FAILED ADDING OPEN TO TODO QUEUE: " + serverInfo);
          }
        }
      }
    }
  }

  /*
   * @param region
   * @throws Exception
   */
  private void processRegionClose(HRegionInfo region) {
    synchronized (master.regionManager) {
      if (region.isRootRegion()) {
        // Root region
        master.regionManager.unsetRootRegion();
        if (region.isOffline()) {
          // Can't proceed without root region. Shutdown.
          LOG.fatal("root region is marked offline");
          master.shutdown();
          return;
        }

      } else if (region.isMetaTable()) {
        // Region is part of the meta table. Remove it from onlineMetaRegions
        master.regionManager.offlineMetaRegion(region.getStartKey());
      }

      boolean offlineRegion =
        master.regionManager.isOfflined(region.getRegionNameAsString());
      boolean reassignRegion = !region.isOffline() && !offlineRegion;

      // NOTE: If the region was just being closed and not offlined, we cannot
      //       mark the region unassignedRegions as that changes the ordering of
      //       the messages we've received. In this case, a close could be
      //       processed before an open resulting in the master not agreeing on
      //       the region's state.
      master.regionManager.setClosed(region.getRegionNameAsString());
      try {
        master.toDoQueue.put(new ProcessRegionClose(master, region,
            offlineRegion, reassignRegion));
      } catch (InterruptedException e) {
        throw new RuntimeException("Putting into toDoQueue was interrupted.", e);
      }
    }
  }
  
  /** Update a server load information because it's shutting down*/
  private boolean removeServerInfo(final String serverName,
                                   final HServerAddress serverAddress) {
    boolean infoUpdated = false;
    serverAddressToServerInfo.remove(serverAddress);
    HServerInfo info = serversToServerInfo.remove(serverName);
    // Only update load information once.
    // This method can be called a couple of times during shutdown.
    if (info != null) {
      LOG.info("Removing server's info " + serverName);
      if (master.getRootRegionLocation() != null &&
        info.getServerAddress().equals(master.getRootRegionLocation())) {
        master.regionManager.unsetRootRegion();
      }
      infoUpdated = true;

      // update load information
      HServerLoad load = serversToLoad.remove(serverName);
      if (load != null) {
        synchronized (loadToServers) {
          Set<String> servers = loadToServers.get(load);
          if (servers != null) {
            servers.remove(serverName);
            loadToServers.put(load, servers);
          }
        }
      }
    }
    return infoUpdated;
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
    synchronized (serversToLoad) {
      numServers = serversToLoad.size();
      for (HServerLoad load : serversToLoad.values()) {
        totalLoad += load.getNumberOfRegions();
      }
      averageLoad = Math.ceil((double)totalLoad / (double)numServers);
    }
    return averageLoad;
  }

  /** @return the number of active servers */
  public int numServers() {
    return serversToServerInfo.size();
  }
  
  /**
   * @param name server name
   * @return HServerInfo for the given server address
   */
  public HServerInfo getServerInfo(String name) {
    return serversToServerInfo.get(name);
  }
  
  /**
   * @return Read-only map of servers to serverinfo.
   */
  public Map<String, HServerInfo> getServersToServerInfo() {
    synchronized (serversToServerInfo) {
      return Collections.unmodifiableMap(serversToServerInfo);
    }
  }

  public Map<HServerAddress, HServerInfo> getServerAddressToServerInfo() {
    // we use this one because all the puts to this map are parallel/synced with the other map.
    synchronized (serversToServerInfo) {
      return Collections.unmodifiableMap(serverAddressToServerInfo);
    }
  }

  /**
   * @return Read-only map of servers to load.
   */
  public Map<String, HServerLoad> getServersToLoad() {
    synchronized (serversToLoad) {
      return Collections.unmodifiableMap(serversToLoad);
    }
  }

  /**
   * Wakes up threads waiting on serversToServerInfo
   */
  public void notifyServers() {
    synchronized (serversToServerInfo) {
      serversToServerInfo.notifyAll();
    }
  }
  
  /*
   * Wait on regionservers to report in
   * with {@link #regionServerReport(HServerInfo, HMsg[])} so they get notice
   * the master is going down.  Waits until all region servers come back with
   * a MSG_REGIONSERVER_STOP.
   */
  void letRegionServersShutdown() {
    if (!master.fsOk) {
      // Forget waiting for the region servers if the file system has gone
      // away. Just exit as quickly as possible.
      return;
    }
    synchronized (serversToServerInfo) {
      while (serversToServerInfo.size() > 0) {
        LOG.info("Waiting on following regionserver(s) to go down " +
          serversToServerInfo.values());
        try {
          serversToServerInfo.wait(master.threadWakeFrequency);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }
  
  /** Watcher triggered when a RS znode is deleted */
  private class ServerExpirer implements Watcher {
    private String server;
    private HServerAddress serverAddress;

    ServerExpirer(String server, HServerAddress serverAddress) {
      this.server = server;
      this.serverAddress = serverAddress;
    }

    public void process(WatchedEvent event) {
      if(event.getType().equals(EventType.NodeDeleted)) {
        LOG.info(server + " znode expired");
        // Remove the server from the known servers list and update load info
        serverAddressToServerInfo.remove(serverAddress);
        HServerInfo info = serversToServerInfo.remove(server);
        boolean rootServer = false;
        if (info != null) {
          HServerAddress root = master.getRootRegionLocation();
          if (root != null && root.equals(info.getServerAddress())) {
            // NOTE: If the server was serving the root region, we cannot
            // reassign
            // it here because the new server will start serving the root region
            // before ProcessServerShutdown has a chance to split the log file.
            master.regionManager.unsetRootRegion();
            rootServer = true;
          }
          String serverName = HServerInfo.getServerName(info);
          HServerLoad load = serversToLoad.remove(serverName);
          if (load != null) {
            synchronized (loadToServers) {
              Set<String> servers = loadToServers.get(load);
              if (servers != null) {
                servers.remove(serverName);
                loadToServers.put(load, servers);
              }
            }
          }
          deadServers.add(server);
          try {
            master.toDoQueue.put(new ProcessServerShutdown(master, info,
                rootServer));
          } catch (InterruptedException e) {
            LOG.error("insert into toDoQueue was interrupted", e);
          }
        }
        synchronized (serversToServerInfo) {
          serversToServerInfo.notifyAll();
        }
      }
    }
  }
  
  /**
   * @param serverName
   */
  public void removeDeadServer(String serverName) {
    deadServers.remove(serverName);
  }
  
  /**
   * @param serverName
   * @return true if server is dead
   */
  public boolean isDead(String serverName) {
    return deadServers.contains(serverName);
  }
}
