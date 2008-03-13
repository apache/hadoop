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
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.Text;

/**
 * The ServerManager class manages info about region servers - HServerInfo, 
 * load numbers, dying servers, etc.
 */
class ServerManager implements HConstants {
  static final Log LOG = LogFactory.getLog(ServerManager.class.getName());
  
  /** The map of known server names to server info */
  final Map<String, HServerInfo> serversToServerInfo =
    new ConcurrentHashMap<String, HServerInfo>();
  
  /** Set of known dead servers */
  final Set<String> deadServers =
    Collections.synchronizedSet(new HashSet<String>());

  /** SortedMap server load -> Set of server names */
  final SortedMap<HServerLoad, Set<String>> loadToServers =
    Collections.synchronizedSortedMap(new TreeMap<HServerLoad, Set<String>>());

  /** Map of server names -> server load */
  final Map<String, HServerLoad> serversToLoad =
    new ConcurrentHashMap<String, HServerLoad>();  
  
  HMaster master;
  
  private final Leases serverLeases;
    
  /**
   * @param master
   */
  public ServerManager(HMaster master) {
    this.master = master;
    serverLeases = new Leases(master.leaseTimeout, 
      master.conf.getInt("hbase.master.lease.thread.wakefrequency", 15 * 1000));
  }
  
  /**
   * Let the server manager know a new regionserver has come online
   * 
   * @param serverInfo
   */
  public void regionServerStartup(HServerInfo serverInfo) {
    String s = serverInfo.getServerAddress().toString().trim();
    LOG.info("received start message from: " + s);
    // Do the lease check up here. There might already be one out on this
    // server expecially if it just shutdown and came back up near-immediately
    // after.
    if (!master.closed.get()) {
      serverLeases.createLease(s, new ServerExpirer(s));
    }
    HServerLoad load = serversToLoad.remove(s);
    if (load != null) {
      // The startup message was from a known server.
      // Remove stale information about the server's load.
      Set<String> servers = loadToServers.get(load);
      if (servers != null) {
        servers.remove(s);
        loadToServers.put(load, servers);
      }
    }

    HServerInfo storedInfo = serversToServerInfo.remove(s);
    if (storedInfo != null && !master.closed.get()) {
      // The startup message was from a known server with the same name.
      // Timeout the old one right away.
      HServerAddress root = master.getRootRegionLocation();
      if (root != null && root.equals(storedInfo.getServerAddress())) {
        master.regionManager.unassignRootRegion();
      }
      master.delayedToDoQueue.put(new ProcessServerShutdown(master, storedInfo));
    }

    // record new server
    load = new HServerLoad();
    serverInfo.setLoad(load);
    serversToServerInfo.put(s, serverInfo);
    serversToLoad.put(s, load);
    Set<String> servers = loadToServers.get(load);
    if (servers == null) {
      servers = new HashSet<String>();
    }
    servers.add(s);
    loadToServers.put(load, servers);
  }
  
  /**
   * @param serverInfo
   * @param msgs
   * @return messages from master to region server indicating what region
   * server should do.
   * 
   * @throws IOException
   */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[])
  throws IOException {
    String serverName = serverInfo.getServerAddress().toString().trim();

    if (msgs.length > 0) {
      if (msgs[0].getMsg() == HMsg.MSG_REPORT_EXITING) {
        processRegionServerExit(serverName, msgs);
        return new HMsg[]{msgs[0]};
      } else if (msgs[0].getMsg() == HMsg.MSG_REPORT_QUIESCED) {
        LOG.info("Region server " + serverName + " quiesced");
        master.quiescedMetaServers.incrementAndGet();
      }
    }

    if(master.quiescedMetaServers.get() >= serversToServerInfo.size()) {
      // If the only servers we know about are meta servers, then we can
      // proceed with shutdown
      LOG.info("All user tables quiesced. Proceeding with shutdown");
      master.startShutdown();
    }

    if (master.shutdownRequested && !master.closed.get()) {
      // Tell the server to stop serving any user regions
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_QUIESCE)};
    }

    if (master.closed.get()) {
      // Tell server to shut down if we are shutting down.  This should
      // happen after check of MSG_REPORT_EXITING above, since region server
      // will send us one of these messages after it gets MSG_REGIONSERVER_STOP
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};
    }

    HServerInfo storedInfo = serversToServerInfo.get(serverName);
    if (storedInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("received server report from unknown server: " + serverName);
      }

      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to start over and call regionServerStartup()
      return new HMsg[]{new HMsg(HMsg.MSG_CALL_SERVER_STARTUP)};
    } else if (storedInfo.getStartCode() != serverInfo.getStartCode()) {
      // This state is reachable if:
      //
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then 
      //    clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      //
      // The answer is to ask A to shut down for good.
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("region server race condition detected: " + serverName);
      }

      synchronized (serversToServerInfo) {
        cancelLease(serverName);
        serversToServerInfo.notifyAll();
      }
      
      return new HMsg[]{new HMsg(HMsg.MSG_REGIONSERVER_STOP)};
    } else {
      return processRegionServerAllsWell(serverName, serverInfo, msgs);
    }
  }

  /** Region server is exiting */
  private void processRegionServerExit(String serverName, HMsg[] msgs) {
    synchronized (serversToServerInfo) {
      try {
        // HRegionServer is shutting down. Cancel the server's lease.
        // Note that canceling the server's lease takes care of updating
        // serversToServerInfo, etc.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Region server " + serverName +
          ": MSG_REPORT_EXITING -- cancelling lease");
        }

        if (cancelLease(serverName)) {
          // Only process the exit message if the server still has a lease.
          // Otherwise we could end up processing the server exit twice.
          LOG.info("Region server " + serverName +
          ": MSG_REPORT_EXITING -- lease cancelled");
          // Get all the regions the server was serving reassigned
          // (if we are not shutting down).
          if (!master.closed.get()) {
            for (int i = 1; i < msgs.length; i++) {
              HRegionInfo info = msgs[i].getRegionInfo();
              if (info.isRootRegion()) {
                master.regionManager.unassignRootRegion();
              } else if (info.isMetaTable()) {
                master.regionManager.offlineMetaRegion(info.getStartKey());
              }

              master.regionManager.setUnassigned(info);
            }
          }
        }

        // We don't need to return anything to the server because it isn't
        // going to do any more work.
/*        return new HMsg[0];*/
      } finally {
        serversToServerInfo.notifyAll();
      }
    }    
  }

  /** RegionServer is checking in, no exceptional circumstances */
  private HMsg[] processRegionServerAllsWell(String serverName, 
    HServerInfo serverInfo, HMsg[] msgs) 
  throws IOException {
    // All's well.  Renew the server's lease.
    // This will always succeed; otherwise, the fetch of serversToServerInfo
    // would have failed above.
    serverLeases.renewLease(serverName);

    // Refresh the info object and the load information
    serversToServerInfo.put(serverName, serverInfo);

    HServerLoad load = serversToLoad.get(serverName);
    if (load != null && !load.equals(serverInfo.getLoad())) {
      // We have previous information about the load on this server
      // and the load on this server has changed
      Set<String> servers = loadToServers.get(load);

      // Note that servers should never be null because loadToServers
      // and serversToLoad are manipulated in pairs
      servers.remove(serverName);
      loadToServers.put(load, servers);
    }

    // Set the current load information
    load = serverInfo.getLoad();
    serversToLoad.put(serverName, load);
    Set<String> servers = loadToServers.get(load);
    if (servers == null) {
      servers = new HashSet<String>();
    }
    servers.add(serverName);
    loadToServers.put(load, servers);

    // Next, process messages for this server
    return processMsgs(serverName, serverInfo, msgs);
  }

  /** 
   * Process all the incoming messages from a server that's contacted us.
   * 
   * Note that we never need to update the server's load information because
   * that has already been done in regionServerReport.
   */
  private HMsg[] processMsgs(String serverName, HServerInfo serverInfo, 
    HMsg incomingMsgs[])
  throws IOException { 
    ArrayList<HMsg> returnMsgs = new ArrayList<HMsg>();
    Map<Text, HRegionInfo> regionsToKill = 
      master.regionManager.getMarkedToClose(serverName);

    // Get reports on what the RegionServer did.
    for (int i = 0; i < incomingMsgs.length; i++) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received " + incomingMsgs[i].toString() + " from " +
            serverName);
      }
      HRegionInfo region = incomingMsgs[i].getRegionInfo();

      switch (incomingMsgs[i].getMsg()) {
        case HMsg.MSG_REPORT_PROCESS_OPEN:
          master.regionManager.updateAssignmentDeadline(region);
          break;
        
        case HMsg.MSG_REPORT_OPEN:
          processRegionOpen(serverName, serverInfo, 
            incomingMsgs[i].getRegionInfo(), returnMsgs);
          break;

        case HMsg.MSG_REPORT_CLOSE:
          LOG.info(serverInfo.getServerAddress().toString() + " no longer serving " +
              region.getRegionName());

          if (region.isRootRegion()) {
            // Root region
            if (region.isOffline()) {
              // Can't proceed without root region. Shutdown.
              LOG.fatal("root region is marked offline");
              master.shutdown();
            }
            master.regionManager.unassignRootRegion();

          } else {
            boolean reassignRegion = !region.isOffline();
            boolean deleteRegion = false;

            if (master.regionManager.isClosing(region.getRegionName())) {
              master.regionManager.noLongerClosing(region.getRegionName());
              reassignRegion = false;
            }

            if (master.regionManager.isMarkedForDeletion(region.getRegionName())) {
              master.regionManager.regionDeleted(region.getRegionName());
              reassignRegion = false;
              deleteRegion = true;
            }

            if (region.isMetaTable()) {
              // Region is part of the meta table. Remove it from onlineMetaRegions
              master.regionManager.offlineMetaRegion(region.getStartKey());
            }

            // NOTE: we cannot put the region into unassignedRegions as that
            //       could create a race with the pending close if it gets 
            //       reassigned before the close is processed.

            master.regionManager.noLongerUnassigned(region);

            try {
              master.toDoQueue.put(new ProcessRegionClose(master, region, 
                reassignRegion, deleteRegion));

            } catch (InterruptedException e) {
              throw new RuntimeException(
                "Putting into toDoQueue was interrupted.", e);
            }
          }
          break;

        case HMsg.MSG_REPORT_SPLIT:
          processSplitRegion(serverName, serverInfo, region, incomingMsgs[++i], 
            incomingMsgs[++i], returnMsgs);
          break;

        default:
          throw new IOException(
            "Impossible state during msg processing.  Instruction: " +
            incomingMsgs[i].getMsg());
      }
    }

    // Tell the region server to close regions that we have marked for closing.
    if (regionsToKill != null) {
      for (HRegionInfo i: regionsToKill.values()) {
        returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE, i));
        // Transition the region from toClose to closing state
        master.regionManager.setClosing(i.getRegionName());
        master.regionManager.noLongerMarkedToClose(serverName, i.getRegionName());
      }
    }

    // Figure out what the RegionServer ought to do, and write back.
    master.regionManager.assignRegions(serverInfo, serverName, returnMsgs);
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }
  
  /**
   * A region has split.
   *
   * @param serverName
   * @param serverInfo
   * @param region
   * @param splitA
   * @param splitB
   * @param returnMsgs
   */
  private void processSplitRegion(String serverName, HServerInfo serverInfo, 
    HRegionInfo region, HMsg splitA, HMsg splitB, ArrayList<HMsg> returnMsgs) {
    
    HRegionInfo newRegionA = splitA.getRegionInfo();
    master.regionManager.setUnassigned(newRegionA);

    HRegionInfo newRegionB = splitB.getRegionInfo();
    master.regionManager.setUnassigned(newRegionB);

    LOG.info("region " + region.getRegionName() + " split. New regions are: " +
      newRegionA.getRegionName() + ", " + newRegionB.getRegionName());

    if (region.isMetaTable()) {
      // A meta region has split.
      master.regionManager.offlineMetaRegion(region.getStartKey());
      master.regionManager.incrementNumMetaRegions();
    }
  }
  
  /** Region server is reporting that a region is now opened */
  private void processRegionOpen(String serverName, HServerInfo serverInfo, 
    HRegionInfo region, ArrayList<HMsg> returnMsgs) 
  throws IOException {
    boolean duplicateAssignment = false;
    
    if (!master.regionManager.isUnassigned(region)) {
      if (region.isRootRegion()) {
        // Root region
        HServerAddress rootServer = master.getRootRegionLocation();
        if (rootServer != null) {
          if (rootServer.toString().compareTo(serverName) == 0) {
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
        if (master.regionManager.isPending(region.getRegionName())) {
          // A duplicate report from the correct server
          return;
        }
        duplicateAssignment = true;
      }
    }
    
    if (duplicateAssignment) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("region server " + serverInfo.getServerAddress().toString()
          + " should not have opened region " + region.getRegionName());
      }

      // This Region should not have been opened.
      // Ask the server to shut it down, but don't report it as closed.  
      // Otherwise the HMaster will think the Region was closed on purpose, 
      // and then try to reopen it elsewhere; that's not what we want.
      returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT, region));
    } else {
      LOG.info(serverInfo.getServerAddress().toString() + " serving " +
        region.getRegionName());

      // it was assigned, and it's not a duplicate assignment, so take it out 
      // of the unassigned list.
      master.regionManager.noLongerUnassigned(region);

      if (region.isRootRegion()) {
        // Store the Root Region location (in memory)
        master.regionManager.setRootRegionLocation(serverInfo.getServerAddress());
      } else {
        // Note that the table has been assigned and is waiting for the
        // meta table to be updated.
        master.regionManager.setPending(region.getRegionName());

        // Queue up an update to note the region location.
        try {
          master.toDoQueue.put(
            new ProcessRegionOpen(master, serverInfo, region));
        } catch (InterruptedException e) {
          throw new RuntimeException(
            "Putting into toDoQueue was interrupted.", e);
        }
      } 
    }
  }
  
  /** Cancel a server's lease and update its load information */
  private boolean cancelLease(final String serverName) {
    boolean leaseCancelled = false;
    HServerInfo info = serversToServerInfo.remove(serverName);
    if (info != null) {
      // Only cancel lease and update load information once.
      // This method can be called a couple of times during shutdown.
      if (master.getRootRegionLocation() != null &&
        info.getServerAddress().equals(master.getRootRegionLocation())) {
        master.regionManager.unassignRootRegion();
      }
      LOG.info("Cancelling lease for " + serverName);
      serverLeases.cancelLease(serverName);
      leaseCancelled = true;

      // update load information
      HServerLoad load = serversToLoad.remove(serverName);
      if (load != null) {
        Set<String> servers = loadToServers.get(load);
        if (servers != null) {
          servers.remove(serverName);
          loadToServers.put(load, servers);
        }
      }
    }
    return leaseCancelled;
  }
  
  
  /** @return the average load across all region servers */
  public int averageLoad() {
    return 0;
  }
  
  /** @return the number of active servers */
  public int numServers() {
    return serversToServerInfo.size();
  }
  
  /**
   * @param address server address
   * @return HServerInfo for the given server address
   */
  public HServerInfo getServerInfo(String address) {
    return serversToServerInfo.get(address);
  }
  
  /**
   * @return Read-only map of servers to serverinfo.
   */
  public Map<String, HServerInfo> getServersToServerInfo() {
    return Collections.unmodifiableMap(serversToServerInfo);
  }

  /**
   * @return Read-only map of servers to load.
   */
  public Map<String, HServerLoad> getServersToLoad() {
    return Collections.unmodifiableMap(serversToLoad);
  }

  /**
   * @return Read-only map of load to servers.
   */
  public Map<HServerLoad, Set<String>> getLoadToServers() {
    return Collections.unmodifiableMap(loadToServers);
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
   * a MSG_REGIONSERVER_STOP which will cancel their lease or until leases held
   * by remote region servers have expired.
   */
  void letRegionServersShutdown() {
    if (!master.fsOk) {
      // Forget waiting for the region servers if the file system has gone
      // away. Just exit as quickly as possible.
      return;
    }
    synchronized (serversToServerInfo) {
      while (serversToServerInfo.size() > 0) {
        LOG.info("Waiting on following regionserver(s) to go down (or " +
          "region server lease expiration, whichever happens first): " +
          serversToServerInfo.values());
        try {
          serversToServerInfo.wait(master.threadWakeFrequency);
        } catch (InterruptedException e) {}
      }
    }
  }
  
  /** Instantiated to monitor the health of a region server */
  private class ServerExpirer implements LeaseListener {
    @SuppressWarnings("hiding")
    private String server;

    ServerExpirer(String server) {
      this.server = server;
    }

    /** {@inheritDoc} */
    public void leaseExpired() {
      LOG.info(server + " lease expired");
      // Remove the server from the known servers list and update load info
      HServerInfo info = serversToServerInfo.remove(server);
      if (info != null) {
        HServerAddress root = master.getRootRegionLocation();
        if (root != null && root.equals(info.getServerAddress())) {
          master.regionManager.unassignRootRegion();
        }
        String serverName = info.getServerAddress().toString();
        HServerLoad load = serversToLoad.remove(serverName);
        if (load != null) {
          Set<String> servers = loadToServers.get(load);
          if (servers != null) {
            servers.remove(serverName);
            loadToServers.put(load, servers);
          }
        }
        deadServers.add(server);
      }
      synchronized (serversToServerInfo) {
        serversToServerInfo.notifyAll();
      }

      // NOTE: If the server was serving the root region, we cannot reassign it
      // here because the new server will start serving the root region before
      // the ProcessServerShutdown operation has a chance to split the log file.
      if (info != null) {
        master.delayedToDoQueue.put(new ProcessServerShutdown(master, info));
      }
    }
  }

  /** Start up the server manager */
  public void start() {
    // Leases are not the same as Chore threads. Set name differently.
    this.serverLeases.setName("ServerManager.leaseChecker");
    this.serverLeases.start();
  }
  
  /** Shut down the server manager */
  public void stop() {
    // stop monitor lease monitor
    serverLeases.close();
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
