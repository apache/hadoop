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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.master.handler.SplitRegionHandler;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

/**
 * Manages and performs region assignment.
 * <p>
 * Monitors ZooKeeper for events related to regions in transition.
 * <p>
 * Handles existing regions in transition during master failover.
 */
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  protected Server master;

  private ServerManager serverManager;

  private CatalogTracker catalogTracker;

  private TimeoutMonitor timeoutMonitor;

  private LoadBalancer balancer;

  /**
   * Map of regions to reopen after the schema of a table is changed. Key -
   * encoded region name, value - HRegionInfo
   */
  private final Map <String, HRegionInfo> regionsToReopen;

  /*
   * Maximum times we recurse an assignment.  See below in {@link #assign()}.
   */
  private final int maximumAssignmentAttempts;

  /**
   * Regions currently in transition.  Map of encoded region names to the master
   * in-memory state for that region.
   */
  final ConcurrentSkipListMap<String, RegionState> regionsInTransition =
    new ConcurrentSkipListMap<String, RegionState>();

  /** Plans for region movement. Key is the encoded version of a region name*/
  // TODO: When do plans get cleaned out?  Ever? In server open and in server
  // shutdown processing -- St.Ack
  // All access to this Map must be synchronized.
  final NavigableMap<String, RegionPlan> regionPlans =
    new TreeMap<String, RegionPlan>();

  private final ZKTable zkTable;

  // store all the table names in disabling state
  Set<String> disablingTables = new HashSet<String>(1);
  // store all the enabling state tablenames.
  Set<String> enablingTables = new HashSet<String>(1);

  /**
   * Server to regions assignment map.
   * Contains the set of regions currently assigned to a given server.
   * This Map and {@link #regions} are tied.  Always update this in tandem
   * with the other under a lock on {@link #regions}
   * @see #regions
   */
  private final NavigableMap<ServerName, Set<HRegionInfo>> servers =
    new TreeMap<ServerName, Set<HRegionInfo>>();

  /**
   * Region to server assignment map.
   * Contains the server a given region is currently assigned to.
   * This Map and {@link #servers} are tied.  Always update this in tandem
   * with the other under a lock on {@link #regions}
   * @see #servers
   */
  private final SortedMap<HRegionInfo, ServerName> regions =
    new TreeMap<HRegionInfo, ServerName>();

  private final ExecutorService executorService;

  /**
   * Constructs a new assignment manager.
   *
   * @param master
   * @param serverManager
   * @param catalogTracker
   * @param service
   * @throws KeeperException
   * @throws IOException 
   */
  public AssignmentManager(Server master, ServerManager serverManager,
      CatalogTracker catalogTracker, final ExecutorService service)
  throws KeeperException, IOException {
    super(master.getZooKeeper());
    this.master = master;
    this.serverManager = serverManager;
    this.catalogTracker = catalogTracker;
    this.executorService = service;
    this.regionsToReopen = Collections.synchronizedMap
                           (new HashMap<String, HRegionInfo> ());
    Configuration conf = master.getConfiguration();
    this.timeoutMonitor = new TimeoutMonitor(
      conf.getInt("hbase.master.assignment.timeoutmonitor.period", 10000),
      master,
      conf.getInt("hbase.master.assignment.timeoutmonitor.timeout", 1800000));
    Threads.setDaemonThreadRunning(timeoutMonitor,
      master.getServerName() + ".timeoutMonitor");
    this.zkTable = new ZKTable(this.master.getZooKeeper());
    this.maximumAssignmentAttempts =
      this.master.getConfiguration().getInt("hbase.assignment.maximum.attempts", 10);
    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  double getAverageLoad() {
    int totalLoad = 0;
    int numServers = 0;
    // Sync on this.regions because access to this.servers always synchronizes
    // in this order.
    synchronized (this.regions) {
      for (Map.Entry<ServerName, Set<HRegionInfo>> e: servers.entrySet()) {
        numServers++;
        totalLoad += e.getValue().size();
      }
    }
    return (double)totalLoad / (double)numServers;
  }

  /**
   * @return Instance of ZKTable.
   */
  public ZKTable getZKTable() {
    // These are 'expensive' to make involving trip to zk ensemble so allow
    // sharing.
    return this.zkTable;
  }
  /**
   * Returns the RegionServer to which hri is assigned.
   *
   * @param hri
   *          HRegion for which this function returns the region server
   * @return HServerInfo The region server to which hri belongs
   */
  public ServerName getRegionServerOfRegion(HRegionInfo hri) {
    synchronized (this.regions ) {
      return regions.get(hri);
    }
  }

  /**
   * Add a regionPlan for the specified region.
   */
  public void addPlan(String encodedName, RegionPlan plan) {
    synchronized (regionPlans) {
      regionPlans.put(encodedName, plan);
    }
  }

  /**
   * Set the list of regions that will be reopened
   * because of an update in table schema
   *
   * @param regions
   *          list of regions that should be tracked for reopen
   */
  public void setRegionsToReopen(List <HRegionInfo> regions) {
    for(HRegionInfo hri : regions) {
      regionsToReopen.put(hri.getEncodedName(), hri);
    }
  }

  /**
   * Used by the client to identify if all regions have the schema updates
   *
   * @param tableName
   * @return Pair indicating the status of the alter command
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(byte[] tableName)
      throws IOException {
    List <HRegionInfo> hris = MetaReader.getTableRegions(
                              this.master.getCatalogTracker(), tableName);
    Integer pending = 0;
    for(HRegionInfo hri : hris) {
      if(regionsToReopen.get(hri.getEncodedName()) != null) {
        pending++;
      }
    }
    return new Pair<Integer, Integer>(pending, hris.size());
  }
  /**
   * Reset all unassigned znodes.  Called on startup of master.
   * Call {@link #assignAllUserRegions()} after root and meta have been assigned.
   * @throws IOException
   * @throws KeeperException
   */
  void cleanoutUnassigned() throws IOException, KeeperException {
    // Cleanup any existing ZK nodes and start watching
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
      this.watcher.assignmentZNode);
  }

  /**
   * Called on startup.
   * Figures whether a fresh cluster start of we are joining extant running cluster.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  void joinCluster() throws IOException, KeeperException, InterruptedException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: Regions that have a null location and are not in regionsInTransitions
    // need to be handled.

    // Scan META to build list of existing regions, servers, and assignment
    // Returns servers who have not checked in (assumed dead) and their regions
    Map<ServerName,List<Pair<HRegionInfo,Result>>> deadServers =
      rebuildUserRegions();
    // Process list of dead servers; note this will add regions to the RIT.
    // processRegionsInTransition will read them and assign them out.
    processDeadServers(deadServers);
    // Check existing regions in transition
    processRegionsInTransition(deadServers);

    // Recover the tables that were not fully moved to DISABLED state.
    // These tables are in DISABLING state when the master restarted/switched.
    boolean isWatcherCreated = recoverTableInDisablingState(this.disablingTables);
    recoverTableInEnablingState(this.enablingTables, isWatcherCreated);
  }

  /**
   * Process all regions that are in transition up in zookeeper.  Used by
   * master joining an already running cluster.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  void processRegionsInTransition()
  throws KeeperException, IOException, InterruptedException {
    // Pass null to signify no dead servers in this context.
    processRegionsInTransition(null);
  }

  /**
   * Process all regions that are in transition up in zookeeper.  Used by
   * master joining an already running cluster.
   * @param deadServers Map of dead servers and their regions.  Can be null.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  void processRegionsInTransition(
      final Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers)
  throws KeeperException, IOException, InterruptedException {
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(watcher,
      watcher.assignmentZNode);
    // Run through all regions.  If they are not assigned and not in RIT, then
    // its a clean cluster startup, else its a failover.
    boolean regionsToProcess = false;
    for (Map.Entry<HRegionInfo, ServerName> e: this.regions.entrySet()) {
      if (!e.getKey().isMetaRegion()
          && !e.getKey().isRootRegion()
          && e.getValue() != null) {
        LOG.debug("Found " + e + " out on cluster");
        regionsToProcess = true;
        break;
      }
      if (nodes.contains(e.getKey().getEncodedName())) {
        LOG.debug("Found " + e.getKey().getRegionNameAsString() + " in RITs");
        // Could be a meta region.
        regionsToProcess = true;
        break;
      }
    }

    // If we found user regions out on cluster, its a failover.
    if (regionsToProcess) {
      LOG.info("Found regions out on cluster or in RIT; failover");
      if (!nodes.isEmpty()) {
        for (String encodedRegionName: nodes) {
          processRegionInTransition(encodedRegionName, null, deadServers);
        }
      }
    } else {
      // Fresh cluster startup.
      LOG.info("Clean cluster startup. Assigning userregions");
      cleanoutUnassigned();
      assignAllUserRegions();
    }
  }

  /**
   * If region is up in zk in transition, then do fixup and block and wait until
   * the region is assigned and out of transition.  Used on startup for
   * catalog regions.
   * @param hri Region to look for.
   * @return True if we processed a region in transition else false if region
   * was not up in zk in transition.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransitionAndBlockUntilAssigned(final HRegionInfo hri)
  throws InterruptedException, KeeperException, IOException {
    boolean intransistion =
      processRegionInTransition(hri.getEncodedName(), hri, null);
    if (!intransistion) return intransistion;
    debugLog(hri, "Waiting on " + HRegionInfo.prettyPrint(hri.getEncodedName()));
    synchronized(this.regionsInTransition) {
      while (!this.master.isStopped() &&
          this.regionsInTransition.containsKey(hri.getEncodedName())) {
        this.regionsInTransition.wait();
      }
    }
    return intransistion;
  }

  /**
   * Process failover of new master for region <code>encodedRegionName</code>
   * up in zookeeper.
   * @param encodedRegionName Region to process failover for.
   * @param regionInfo If null we'll go get it from meta table.
   * @param deadServers Can be null 
   * @return True if we processed <code>regionInfo</code> as a RIT.
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransition(final String encodedRegionName,
      final HRegionInfo regionInfo,
      final Map<ServerName,List<Pair<HRegionInfo,Result>>> deadServers)
  throws KeeperException, IOException {
    RegionTransitionData data = ZKAssign.getData(watcher, encodedRegionName);
    if (data == null) return false;
    HRegionInfo hri = regionInfo;
    if (hri == null) {
      Pair<HRegionInfo, ServerName> p =
        MetaReader.getRegion(catalogTracker, data.getRegionName());
      if (p == null) return false;
      hri = p.getFirst();
    }
    processRegionsInTransition(data, hri, deadServers);
    return true;
  }

  void processRegionsInTransition(final RegionTransitionData data,
      final HRegionInfo regionInfo,
      final Map<ServerName,List<Pair<HRegionInfo,Result>>> deadServers)
  throws KeeperException {
    String encodedRegionName = regionInfo.getEncodedName();
    LOG.info("Processing region " + regionInfo.getRegionNameAsString() +
      " in state " + data.getEventType());
    synchronized (regionsInTransition) {
      switch (data.getEventType()) {
      case RS_ZK_REGION_CLOSING:
        // If zk node of the region was updated by a live server skip this
        // region and just add it into RIT.
        if (isOnDeadServer(regionInfo, deadServers) &&
            (data.getOrigin() == null || !serverManager.isServerOnline(data.getOrigin()))) {
          // If was on dead server, its closed now. Force to OFFLINE and this
          // will get it reassigned if appropriate
          forceOffline(regionInfo, data);
        } else {
          // Just insert region into RIT.
          // If this never updates the timeout will trigger new assignment
          regionsInTransition.put(encodedRegionName, new RegionState(
            regionInfo, RegionState.State.CLOSING,
            data.getStamp(), data.getOrigin()));
        }
        break;

      case RS_ZK_REGION_CLOSED:
        // Region is closed, insert into RIT and handle it
        addToRITandCallClose(regionInfo, RegionState.State.CLOSED, data);
        break;

      case M_ZK_REGION_OFFLINE:
        // Region is offline, insert into RIT and handle it like a closed
        addToRITandCallClose(regionInfo, RegionState.State.OFFLINE, data);
        break;

      case RS_ZK_REGION_OPENING:
        // TODO: Could check if it was on deadServers.  If it was, then we could
        // do what happens in TimeoutMonitor when it sees this condition.

        // Just insert region into RIT
        // If this never updates the timeout will trigger new assignment
        regionsInTransition.put(encodedRegionName, new RegionState(
            regionInfo, RegionState.State.OPENING,
            data.getStamp(), data.getOrigin()));
        break;

      case RS_ZK_REGION_OPENED:
        // Region is opened, insert into RIT and handle it
        regionsInTransition.put(encodedRegionName, new RegionState(
            regionInfo, RegionState.State.OPEN,
            data.getStamp(), data.getOrigin()));
        ServerName sn = data.getOrigin() == null? null: data.getOrigin();
        // sn could be null if this server is no longer online.  If
        // that is the case, just let this RIT timeout; it'll be assigned
        // to new server then.
        if (sn == null) {
          LOG.warn("Region in transition " + regionInfo.getEncodedName() +
            " references a null server; letting RIT timeout so will be " +
            "assigned elsewhere");
        } else if (isOnDeadServer(regionInfo, deadServers) &&
            !serverManager.isServerOnline(sn)) {
          // If was on a dead server, then its not open any more; needs
          // handling.
          forceOffline(regionInfo, data);
        } else {
          new OpenedRegionHandler(master, this, regionInfo, sn).process();
        }
        break;
      }
    }
  }

  /**
   * Put the region <code>hri</code> into an offline state up in zk.
   * @param hri
   * @param oldData
   * @throws KeeperException
   */
  private void forceOffline(final HRegionInfo hri,
      final RegionTransitionData oldData)
  throws KeeperException {
    // If was on dead server, its closed now.  Force to OFFLINE and then
    // handle it like a close; this will get it reassigned if appropriate
    debugLog(hri, "RIT " + hri.getEncodedName() + " in state=" +
      oldData.getEventType() + " was on deadserver; forcing offline");
    ZKAssign.createOrForceNodeOffline(this.watcher, hri,
      this.master.getServerName());
    addToRITandCallClose(hri, RegionState.State.OFFLINE, oldData);
  }

  /**
   * Add to the in-memory copy of regions in transition and then call close
   * handler on passed region <code>hri</code>
   * @param hri
   * @param state
   * @param oldData
   */
  private void addToRITandCallClose(final HRegionInfo hri,
      final RegionState.State state, final RegionTransitionData oldData) {
    this.regionsInTransition.put(hri.getEncodedName(),
      new RegionState(hri, state, oldData.getStamp(), oldData.getOrigin()));
    new ClosedRegionHandler(this.master, this, hri).process();
  }

  /**
   * When a region is closed, it should be removed from the regionsToReopen
   * @param hri HRegionInfo of the region which was closed
   */
  public void removeClosedRegion(HRegionInfo hri) {
    if (!regionsToReopen.isEmpty()) {
      if (regionsToReopen.remove(hri.getEncodedName()) != null) {
          LOG.debug("Removed region from reopening regions because it was closed");
      }
    }
  }

  /**
   * @param regionInfo
   * @param deadServers Map of deadServers and the regions they were carrying;
   * can be null.
   * @return True if the passed regionInfo in the passed map of deadServers?
   */
  private boolean isOnDeadServer(final HRegionInfo regionInfo,
      final Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers) {
    if (deadServers == null) return false;
    for (Map.Entry<ServerName, List<Pair<HRegionInfo, Result>>> deadServer:
        deadServers.entrySet()) {
      for (Pair<HRegionInfo, Result> e: deadServer.getValue()) {
        if (e.getFirst().equals(regionInfo)) return true;
      }
    }
    return false;
  }

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet).
   * @param data
   */
  private void handleRegion(final RegionTransitionData data) {
    synchronized(regionsInTransition) {
      if (data == null || data.getOrigin() == null) {
        LOG.warn("Unexpected NULL input " + data);
        return;
      }
      ServerName sn = data.getOrigin();
      // Check if this is a special HBCK transition
      if (sn.equals(HConstants.HBCK_CODE_SERVERNAME)) {
        handleHBCK(data);
        return;
      }
      // Verify this is a known server
      if (!serverManager.isServerOnline(sn) &&
          !this.master.getServerName().equals(sn)) {
        LOG.warn("Attempted to handle region transition for server but " +
          "server is not online: " + Bytes.toString(data.getRegionName()));
        return;
      }
      String encodedName = HRegionInfo.encodeRegionName(data.getRegionName());
      String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
      // Printing if the event was created a long time ago helps debugging
      boolean lateEvent = data.getStamp() <
          (System.currentTimeMillis() - 15000);
      LOG.debug("Handling transition=" + data.getEventType() +
        ", server=" + data.getOrigin() + ", region=" +
          prettyPrintedRegionName +
          (lateEvent? ", which is more than 15 seconds late" : ""));
      RegionState regionState = regionsInTransition.get(encodedName);
      switch (data.getEventType()) {
        case M_ZK_REGION_OFFLINE:
          // Nothing to do.
          break;

        case RS_ZK_REGION_SPLITTING:
          if (!isInStateForSplitting(regionState)) break;
          addSplittingToRIT(sn, encodedName);
          break;

        case RS_ZK_REGION_SPLIT:
          // RegionState must be null, or SPLITTING or PENDING_CLOSE.
          if (!isInStateForSplitting(regionState)) break;
          // If null, add SPLITTING state before going to SPLIT
          if (regionState == null) {
            regionState = addSplittingToRIT(sn, encodedName);
            String message = "Received SPLIT for region " + prettyPrintedRegionName +
              " from server " + sn;
            // If still null, it means we cannot find it and it was already processed
            if (regionState == null) {
              LOG.warn(message + " but it doesn't exist anymore," +
                  " probably already processed its split");
              break;
            }
            LOG.info(message +
                " but region was not first in SPLITTING state; continuing");
          }
          // Check it has daughters.
          byte [] payload = data.getPayload();
          List<HRegionInfo> daughters = null;
          try {
            daughters = Writables.getHRegionInfos(payload, 0, payload.length);
          } catch (IOException e) {
            LOG.error("Dropped split! Failed reading split payload for " +
              prettyPrintedRegionName);
            break;
          }
          assert daughters.size() == 2;
          // Assert that we can get a serverinfo for this server.
          if (!this.serverManager.isServerOnline(sn)) {
            LOG.error("Dropped split! ServerName=" + sn + " unknown.");
            break;
          }
          // Run handler to do the rest of the SPLIT handling.
          this.executorService.submit(new SplitRegionHandler(master, this,
            regionState.getRegion(), sn, daughters));
          break;

        case RS_ZK_REGION_CLOSING:
          // Should see CLOSING after we have asked it to CLOSE or additional
          // times after already being in state of CLOSING
          if (regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSING for region " + prettyPrintedRegionName +
              " from server " + data.getOrigin() + " but region was in " +
              " the state " + regionState + " and not " +
              "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Transition to CLOSING (or update stamp if already CLOSING)
          regionState.update(RegionState.State.CLOSING,
              data.getStamp(), data.getOrigin());
          break;

        case RS_ZK_REGION_CLOSED:
          // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
          if (regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSED for region " + prettyPrintedRegionName +
                " from server " + data.getOrigin() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Handle CLOSED by assigning elsewhere or stopping if a disable
          // If we got here all is good.  Need to update RegionState -- else
          // what follows will fail because not in expected state.
          regionState.update(RegionState.State.CLOSED,
              data.getStamp(), data.getOrigin());
	  removeClosedRegion(regionState.getRegion());
          this.executorService.submit(new ClosedRegionHandler(master,
            this, regionState.getRegion()));
          break;

        case RS_ZK_REGION_OPENING:
          // Should see OPENING after we have asked it to OPEN or additional
          // times after already being in state of OPENING
          if(regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENING for region " +
                prettyPrintedRegionName +
                " from server " + data.getOrigin() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Transition to OPENING (or update stamp if already OPENING)
          regionState.update(RegionState.State.OPENING,
              data.getStamp(), data.getOrigin());
          break;

        case RS_ZK_REGION_OPENED:
          // Should see OPENED after OPENING but possible after PENDING_OPEN
          if (regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENED for region " +
                prettyPrintedRegionName +
                " from server " + data.getOrigin() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Handle OPENED by removing from transition and deleted zk node
          regionState.update(RegionState.State.OPEN,
              data.getStamp(), data.getOrigin());
          this.executorService.submit(
            new OpenedRegionHandler(master, this, regionState.getRegion(),
              data.getOrigin()));
          break;
      }
    }
  }

  /**
   * @return Returns true if this RegionState is splittable; i.e. the
   * RegionState is currently in splitting state or pending_close or
   * null (Anything else will return false). (Anything else will return false).
   */
  private boolean isInStateForSplitting(final RegionState rs) {
    if (rs == null) return true;
    if (rs.isSplitting()) return true;
    if (convertPendingCloseToSplitting(rs)) return true;
    LOG.warn("Dropped region split! Not in state good for SPLITTING; rs=" + rs);
    return false;
  }

  /**
   * If the passed regionState is in PENDING_CLOSE, clean up PENDING_CLOSE
   * state and convert it to SPLITTING instead.
   * This can happen in case where master wants to close a region at same time
   * a regionserver starts a split.  The split won.  Clean out old PENDING_CLOSE
   * state.
   * @param rs
   * @return True if we converted from PENDING_CLOSE to SPLITTING
   */
  private boolean convertPendingCloseToSplitting(final RegionState rs) {
    if (!rs.isPendingClose()) return false;
    LOG.debug("Converting PENDING_CLOSE to SPLITING; rs=" + rs);
    rs.update(RegionState.State.SPLITTING);
    // Clean up existing state.  Clear from region plans seems all we
    // have to do here by way of clean up of PENDING_CLOSE.
    clearRegionPlan(rs.getRegion());
    return true;
  }

  /**
   * @param serverName
   * @param encodedName
   * @return The SPLITTING RegionState we added to RIT for the passed region
   * <code>encodedName</code>
   */
  private RegionState addSplittingToRIT(final ServerName serverName,
      final String encodedName) {
    RegionState regionState = null;
    synchronized (this.regions) {
      regionState = findHRegionInfoThenAddToRIT(serverName, encodedName);
      if (regionState != null) {
        regionState.update(RegionState.State.SPLITTING,
          System.currentTimeMillis(), serverName);
      }
    }
    return regionState;
  }

  /**
   * Caller must hold lock on <code>this.regions</code>.
   * @param serverName
   * @param encodedName
   * @return The instance of RegionState that was added to RIT or null if error.
   */
  private RegionState findHRegionInfoThenAddToRIT(final ServerName serverName,
      final String encodedName) {
    HRegionInfo hri = findHRegionInfo(serverName, encodedName);
    if (hri == null) {
      LOG.warn("Region " + encodedName + " not found on server " + serverName +
        "; failed processing");
      return null;
    }
    // Add to regions in transition, then update state to SPLITTING.
    return addToRegionsInTransition(hri);
  }

  /**
   * Caller must hold lock on <code>this.regions</code>.
   * @param serverName
   * @param encodedName
   * @return Found HRegionInfo or null.
   */
  private HRegionInfo findHRegionInfo(final ServerName sn,
      final String encodedName) {
    if (!this.serverManager.isServerOnline(sn)) return null;
    Set<HRegionInfo> hris = this.servers.get(sn);
    HRegionInfo foundHri = null;
    for (HRegionInfo hri: hris) {
      if (hri.getEncodedName().equals(encodedName)) {
        foundHri = hri;
        break;
      }
    }
    return foundHri;
  }

  /**
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * @param data
   */
  private void handleHBCK(RegionTransitionData data) {
    String encodedName = HRegionInfo.encodeRegionName(data.getRegionName());
    LOG.info("Handling HBCK triggered transition=" + data.getEventType() +
      ", server=" + data.getOrigin() + ", region=" +
      HRegionInfo.prettyPrint(encodedName));
    RegionState regionState = regionsInTransition.get(encodedName);
    switch (data.getEventType()) {
      case M_ZK_REGION_OFFLINE:
        HRegionInfo regionInfo = null;
        if (regionState != null) {
          regionInfo = regionState.getRegion();
        } else {
          try {
            regionInfo = MetaReader.getRegion(catalogTracker,
                data.getRegionName()).getFirst();
          } catch (IOException e) {
            LOG.info("Exception reading META doing HBCK repair operation", e);
            return;
          }
        }
        LOG.info("HBCK repair is triggering assignment of region=" +
            regionInfo.getRegionNameAsString());
        // trigger assign, node is already in OFFLINE so don't need to update ZK
        assign(regionInfo, false);
        break;

      default:
        LOG.warn("Received unexpected region state from HBCK (" +
            data.getEventType() + ")");
        break;
    }
  }

  // ZooKeeper events

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeCreated(String path) {
    if(path.startsWith(watcher.assignmentZNode)) {
      try {
        RegionTransitionData data = ZKAssign.getData(watcher, path);
        if (data == null) {
          return;
        }
        handleRegion(data);
      } catch (KeeperException e) {
        master.abort("Unexpected ZK exception reading unassigned node data", e);
      }
    }
  }

  /**
   * Existing unassigned node has had data changed.
   *
   * <p>This happens when an RS transitions from OFFLINE to OPENING, or between
   * OPENING/OPENED and CLOSING/CLOSED.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeDataChanged(String path) {
    if(path.startsWith(watcher.assignmentZNode)) {
      try {
        RegionTransitionData data = ZKAssign.getData(watcher, path);
        if (data == null) {
          return;
        }
        handleRegion(data);
      } catch (KeeperException e) {
        master.abort("Unexpected ZK exception reading unassigned node data", e);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    // Added so we notice when ephemeral nodes go away; in particular,
    // SPLITTING or SPLIT nodes added by a regionserver splitting.
    if (path.startsWith(this.watcher.assignmentZNode)) {
      String regionName =
        ZKAssign.getRegionName(this.master.getZooKeeper(), path);
      RegionState rs = this.regionsInTransition.get(regionName);
      if (rs != null) {
        if (rs.isSplitting() || rs.isSplit()) {
          LOG.debug("Ephemeral node deleted, regionserver crashed?, " +
            "clearing from RIT; rs=" + rs);
          clearRegionFromTransition(rs.getRegion());
        } else {
          LOG.warn("Node deleted but still in RIT: " + rs);
        }
      }
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING, SPLITTING or CLOSING of a
   * region by creating a znode.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.assignmentZNode)) {
      try {
        // Just make sure we see the changes for the new znodes
        ZKUtil.listChildrenAndWatchThem(watcher,
            watcher.assignmentZNode);
      } catch(KeeperException e) {
        master.abort("Unexpected ZK exception reading unassigned children", e);
      }
    }
  }

  /**
   * Marks the region as online.  Removes it from regions in transition and
   * updates the in-memory assignment information.
   * <p>
   * Used when a region has been successfully opened on a region server.
   * @param regionInfo
   * @param sn
   */
  public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
    synchronized (this.regionsInTransition) {
      RegionState rs =
        this.regionsInTransition.remove(regionInfo.getEncodedName());
      if (rs != null) {
        this.regionsInTransition.notifyAll();
      }
    }
    synchronized (this.regions) {
      // Add check
      ServerName oldSn = this.regions.get(regionInfo);
      if (oldSn != null) LOG.warn("Overwriting " + regionInfo.getEncodedName() +
        " on " + oldSn + " with " + sn);
      
      if (isServerOnline(sn)) {
        this.regions.put(regionInfo, sn);
        addToServers(sn, regionInfo);
        this.regions.notifyAll();
      } else {
        LOG.info("The server is not in online servers, ServerName=" + 
          sn.getServerName() + ", region=" + regionInfo.getEncodedName());
      }
    }
    // Remove plan if one.
    clearRegionPlan(regionInfo);
    // Update timers for all regions in transition going against this server.
    updateTimers(sn);
  }

  /**
   * Touch timers for all regions in transition that have the passed
   * <code>sn</code> in common.
   * Call this method whenever a server checks in.  Doing so helps the case where
   * a new regionserver has joined the cluster and its been given 1k regions to
   * open.  If this method is tickled every time the region reports in a
   * successful open then the 1k-th region won't be timed out just because its
   * sitting behind the open of 999 other regions.  This method is NOT used
   * as part of bulk assign -- there we have a different mechanism for extending
   * the regions in transition timer (we turn it off temporarily -- because
   * there is no regionplan involved when bulk assigning.
   * @param sn
   */
  private void updateTimers(final ServerName sn) {
    // This loop could be expensive.
    // First make a copy of current regionPlan rather than hold sync while
    // looping because holding sync can cause deadlock.  Its ok in this loop
    // if the Map we're going against is a little stale
    Map<String, RegionPlan> copy = new HashMap<String, RegionPlan>();
    synchronized(this.regionPlans) {
      copy.putAll(this.regionPlans);
    }
    for (Map.Entry<String, RegionPlan> e: copy.entrySet()) {
      if (!e.getValue().getDestination().equals(sn)) continue;
      RegionState rs = null;
      synchronized (this.regionsInTransition) {
        rs = this.regionsInTransition.get(e.getKey());
      }
      if (rs == null) continue;
      rs.updateTimestampToNow();
    }
  }

  /**
   * Marks the region as offline.  Removes it from regions in transition and
   * removes in-memory assignment information.
   * <p>
   * Used when a region has been closed and should remain closed.
   * @param regionInfo
   */
  public void regionOffline(final HRegionInfo regionInfo) {
    synchronized(this.regionsInTransition) {
      if (this.regionsInTransition.remove(regionInfo.getEncodedName()) != null) {
        this.regionsInTransition.notifyAll();
      }
    }
    // remove the region plan as well just in case.
    clearRegionPlan(regionInfo);
    setOffline(regionInfo);
  }

  /**
   * Sets the region as offline by removing in-memory assignment information but
   * retaining transition information.
   * <p>
   * Used when a region has been closed but should be reassigned.
   * @param regionInfo
   */
  public void setOffline(HRegionInfo regionInfo) {
    synchronized (this.regions) {
      ServerName sn = this.regions.remove(regionInfo);
      if (sn == null) return;
      Set<HRegionInfo> serverRegions = this.servers.get(sn);
      if (!serverRegions.remove(regionInfo)) {
        LOG.warn("No " + regionInfo + " on " + sn);
      }
    }
  }

  public void offlineDisabledRegion(HRegionInfo regionInfo) {
    // Disabling so should not be reassigned, just delete the CLOSED node
    LOG.debug("Table being disabled so deleting ZK node and removing from " +
        "regions in transition, skipping assignment of region " +
          regionInfo.getRegionNameAsString());
    try {
      if (!ZKAssign.deleteClosedNode(watcher, regionInfo.getEncodedName())) {
        // Could also be in OFFLINE mode
        ZKAssign.deleteOfflineNode(watcher, regionInfo.getEncodedName());
      }
    } catch (KeeperException.NoNodeException nne) {
      LOG.debug("Tried to delete closed node for " + regionInfo + " but it " +
          "does not exist so just offlining");
    } catch (KeeperException e) {
      this.master.abort("Error deleting CLOSED node in ZK", e);
    }
    regionOffline(regionInfo);
  }

  // Assignment methods

  /**
   * Assigns the specified region.
   * <p>
   * If a RegionPlan is available with a valid destination then it will be used
   * to determine what server region is assigned to.  If no RegionPlan is
   * available, region will be assigned to a random available server.
   * <p>
   * Updates the RegionState and sends the OPEN RPC.
   * <p>
   * This will only succeed if the region is in transition and in a CLOSED or
   * OFFLINE state or not in transition (in-memory not zk), and of course, the
   * chosen server is up and running (It may have just crashed!).  If the
   * in-memory checks pass, the zk node is forced to OFFLINE before assigning.
   *
   * @param region server to be assigned
   * @param setOfflineInZK whether ZK node should be created/transitioned to an
   *                       OFFLINE state before assigning the region
   */
  public void assign(HRegionInfo region, boolean setOfflineInZK) {
    assign(region, setOfflineInZK, false);
  }

  public void assign(HRegionInfo region, boolean setOfflineInZK,
      boolean forceNewPlan) {
    String tableName = region.getTableNameAsString();
    boolean disabled = this.zkTable.isDisabledTable(tableName);
    if (disabled || this.zkTable.isDisablingTable(tableName)) {
      LOG.info("Table " + tableName + (disabled? " disabled;": " disabling;") +
        " skipping assign of " + region.getRegionNameAsString());
      offlineDisabledRegion(region);
      return;
    }
    if (this.serverManager.isClusterShutdown()) {
      LOG.info("Cluster shutdown is set; skipping assign of " +
        region.getRegionNameAsString());
      return;
    }
    RegionState state = addToRegionsInTransition(region);
    synchronized (state) {
      assign(state, setOfflineInZK, forceNewPlan);
    }
  }

  /**
   * Bulk assign regions to <code>destination</code>.
   * @param destination
   * @param regions Regions to assign.
   */
  void assign(final ServerName destination,
      final List<HRegionInfo> regions) {
    if (regions.size() == 0) {
      return;
    }
    LOG.debug("Bulk assigning " + regions.size() + " region(s) to " +
      destination.toString());

    List<RegionState> states = new ArrayList<RegionState>(regions.size());
    synchronized (this.regionsInTransition) {
      for (HRegionInfo region: regions) {
        states.add(forceRegionStateToOffline(region));
      }
    }
    // Presumption is that only this thread will be updating the state at this
    // time; i.e. handlers on backend won't be trying to set it to OPEN, etc.
    AtomicInteger counter = new AtomicInteger(0);
    CreateUnassignedAsyncCallback cb =
      new CreateUnassignedAsyncCallback(this.watcher, destination, counter);
    for (RegionState state: states) {
      if (!asyncSetOfflineInZooKeeper(state, cb, state)) {
        return;
      }
    }
    // Wait until all unassigned nodes have been put up and watchers set.
    int total = regions.size();
    for (int oldCounter = 0; true;) {
      int count = counter.get();
      if (oldCounter != count) {
        LOG.info(destination.toString() + " unassigned znodes=" + count +
          " of total=" + total);
        oldCounter = count;
      }
      if (count == total) break;
      Threads.sleep(1);
    }
    // Move on to open regions.
    try {
      // Send OPEN RPC. If it fails on a IOE or RemoteException, the
      // TimeoutMonitor will pick up the pieces.
      long maxWaitTime = System.currentTimeMillis() +
        this.master.getConfiguration().
          getLong("hbase.regionserver.rpc.startup.waittime", 60000);
      while (!this.master.isStopped()) {
        try {
          this.serverManager.sendRegionOpen(destination, regions);
          break;
        } catch (RemoteException e) {
          IOException decodedException = e.unwrapRemoteException();
          if (decodedException instanceof RegionServerStoppedException) {
            LOG.warn("The region server was shut down, ", decodedException);
            // No need to retry, the region server is a goner.
            return;
          } else if (decodedException instanceof ServerNotRunningYetException) {
            // This is the one exception to retry.  For all else we should just fail
            // the startup.
            long now = System.currentTimeMillis();
            if (now > maxWaitTime) throw e;
            LOG.debug("Server is not yet up; waiting up to " +
                (maxWaitTime - now) + "ms", e);
            Thread.sleep(1000);
          }

          throw decodedException;
        }
      }
    } catch (IOException e) {
      // Can be a socket timeout, EOF, NoRouteToHost, etc
      LOG.info("Unable to communicate with the region server in order" +
          " to assign regions", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Bulk assigning done for " + destination.toString());
  }

  /**
   * Callback handler for create unassigned znodes used during bulk assign.
   */
  static class CreateUnassignedAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateUnassignedAsyncCallback.class);
    private final ZooKeeperWatcher zkw;
    private final ServerName destination;
    private final AtomicInteger counter;

    CreateUnassignedAsyncCallback(final ZooKeeperWatcher zkw,
        final ServerName destination, final AtomicInteger counter) {
      this.zkw = zkw;
      this.destination = destination;
      this.counter = counter;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc != 0) {
        // Thisis resultcode.  If non-zero, need to resubmit.
        LOG.warn("rc != 0 for " + path + " -- retryable connectionloss -- " +
          "FIX see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A2");
        this.zkw.abort("Connectionloss writing unassigned at " + path +
          ", rc=" + rc, null);
        return;
      }
      LOG.debug("rs=" + (RegionState)ctx + ", server=" + this.destination.toString());
      // Async exists to set a watcher so we'll get triggered when
      // unassigned node changes.
      this.zkw.getRecoverableZooKeeper().getZooKeeper().exists(path, this.zkw,
        new ExistsUnassignedAsyncCallback(this.counter, destination), ctx);
    }
  }

  /**
   * Callback handler for the exists call that sets watcher on unassigned znodes.
   * Used during bulk assign on startup.
   */
  static class ExistsUnassignedAsyncCallback implements AsyncCallback.StatCallback {
    private final Log LOG = LogFactory.getLog(ExistsUnassignedAsyncCallback.class);
    private final AtomicInteger counter;
    private ServerName destination;

    ExistsUnassignedAsyncCallback(final AtomicInteger counter, ServerName destination) {
      this.counter = counter;
      this.destination = destination;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc != 0) {
        // Thisis resultcode.  If non-zero, need to resubmit.
        LOG.warn("rc != 0 for " + path + " -- retryable connectionloss -- " +
          "FIX see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A2");
        return;
      }
      RegionState state = (RegionState)ctx;
      LOG.debug("rs=" + state);
      // Transition RegionState to PENDING_OPEN here in master; means we've
      // sent the open.  We're a little ahead of ourselves here since we've not
      // yet sent out the actual open but putting this state change after the
      // call to open risks our writing PENDING_OPEN after state has been moved
      // to OPENING by the regionserver.
      state.update(RegionState.State.PENDING_OPEN, System.currentTimeMillis(), destination);
      this.counter.addAndGet(1);
    }
  }

  /**
   * @param region
   * @return The current RegionState
   */
  private RegionState addToRegionsInTransition(final HRegionInfo region) {
    synchronized (regionsInTransition) {
      return forceRegionStateToOffline(region);
    }
  }

  /**
   * Sets regions {@link RegionState} to {@link RegionState.State#OFFLINE}.
   * Caller must hold lock on this.regionsInTransition.
   * @param region
   * @return Amended RegionState.
   */
  private RegionState forceRegionStateToOffline(final HRegionInfo region) {
    String encodedName = region.getEncodedName();
    RegionState state = this.regionsInTransition.get(encodedName);
    if (state == null) {
      state = new RegionState(region, RegionState.State.OFFLINE);
      this.regionsInTransition.put(encodedName, state);
    } else {
      LOG.debug("Forcing OFFLINE; was=" + state);
      state.update(RegionState.State.OFFLINE);
    }
    return state;
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state
   * @param setOfflineInZK
   * @param forceNewPlan
   */
  private void assign(final RegionState state, final boolean setOfflineInZK,
      final boolean forceNewPlan) {
    for (int i = 0; i < this.maximumAssignmentAttempts; i++) {
      if (setOfflineInZK && !setOfflineInZooKeeper(state)) return;
      if (this.master.isStopped()) {
        LOG.debug("Server stopped; skipping assign of " + state);
        return;
      }
      RegionPlan plan = getRegionPlan(state, forceNewPlan);
      if (plan == null) {
        debugLog(state.getRegion(),
            "Unable to determine a plan to assign " + state);
        return; // Should get reassigned later when RIT times out.
      }
      try {
        debugLog(state.getRegion(),
          "Assigning region " + state.getRegion().getRegionNameAsString() +
          " to " + plan.getDestination().toString());
        // Transition RegionState to PENDING_OPEN
        state.update(RegionState.State.PENDING_OPEN, System.currentTimeMillis(),
            plan.getDestination());
        // Send OPEN RPC. This can fail if the server on other end is is not up.
        RegionOpeningState regionOpenState = serverManager.sendRegionOpen(plan
            .getDestination(), state.getRegion());
        if (regionOpenState == RegionOpeningState.ALREADY_OPENED) {
          // Remove region from in-memory transition and unassigned node from ZK
          // While trying to enable the table the regions of the table were
          // already enabled.
          String encodedRegionName = state.getRegion()
              .getEncodedName();
          try {
            ZKAssign.deleteOfflineNode(master.getZooKeeper(), encodedRegionName);
          } catch (KeeperException.NoNodeException e) {
            if(LOG.isDebugEnabled()){
              LOG.debug("The unassigned node "+encodedRegionName+" doesnot exist.");
            }
          } catch (KeeperException e) {
            master.abort(
                "Error deleting OFFLINED node in ZK for transition ZK node ("
                    + encodedRegionName + ")", e);
          }
          synchronized (this.regionsInTransition) {
            this.regionsInTransition.remove(plan.getRegionInfo()
                .getEncodedName());
          }
          synchronized (this.regions) {
            this.regions.put(plan.getRegionInfo(), plan.getDestination());
          }
        }
        break;
      } catch (Throwable t) {
        LOG.warn("Failed assignment of " +
          state.getRegion().getRegionNameAsString() + " to " +
          plan.getDestination() + ", trying to assign elsewhere instead; " +
          "retry=" + i, t);
        // Clean out plan we failed execute and one that doesn't look like it'll
        // succeed anyways; we need a new plan!
        // Transition back to OFFLINE
        state.update(RegionState.State.OFFLINE);
        // Force a new plan and reassign.  Will return null if no servers.
        if (getRegionPlan(state, plan.getDestination(), true) == null) {
          LOG.warn("Unable to find a viable location to assign region " +
            state.getRegion().getRegionNameAsString());
          return;
        }
      }
    }
  }

  private void debugLog(HRegionInfo region, String string) {
    if (region.isMetaTable() || region.isRootRegion()) {
      LOG.info(string);
    } else {
      LOG.debug(string);
    }
  }

  /**
   * Set region as OFFLINED up in zookeeper
   * @param state
   * @return True if we succeeded, false otherwise (State was incorrect or failed
   * updating zk).
   */
  boolean setOfflineInZooKeeper(final RegionState state) {
    if (!state.isClosed() && !state.isOffline()) {
      this.master.abort("Unexpected state trying to OFFLINE; " + state,
        new IllegalStateException());
      return false;
    }
    state.update(RegionState.State.OFFLINE);
    try {
      if(!ZKAssign.createOrForceNodeOffline(master.getZooKeeper(),
          state.getRegion(), this.master.getServerName())) {
        LOG.warn("Attempted to create/force node into OFFLINE state before " +
          "completing assignment but failed to do so for " + state);
        return false;
      }
    } catch (KeeperException e) {
      master.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return false;
    }
    return true;
  }

  /**
   * Set region as OFFLINED up in zookeeper asynchronously.
   * @param state
   * @return True if we succeeded, false otherwise (State was incorrect or failed
   * updating zk).
   */
  boolean asyncSetOfflineInZooKeeper(final RegionState state,
      final AsyncCallback.StringCallback cb, final Object ctx) {
    if (!state.isClosed() && !state.isOffline()) {
        new RuntimeException("Unexpected state trying to OFFLINE; " + state);
      this.master.abort("Unexpected state trying to OFFLINE; " + state,
        new IllegalStateException());
      return false;
    }
    state.update(RegionState.State.OFFLINE);
    try {
      ZKAssign.asyncCreateNodeOffline(master.getZooKeeper(), state.getRegion(),
        this.master.getServerName(), cb, ctx);
    } catch (KeeperException e) {
      master.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return false;
    }
    return true;
  }

  /**
   * @param state
   * @return Plan for passed <code>state</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  RegionPlan getRegionPlan(final RegionState state,
      final boolean forceNewPlan) {
    return getRegionPlan(state, null, forceNewPlan);
  }

  /**
   * @param state
   * @param serverToExclude Server to exclude (we know its bad). Pass null if
   * all servers are thought to be assignable.
   * @param forceNewPlan If true, then if an existing plan exists, a new plan
   * will be generated.
   * @return Plan for passed <code>state</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  RegionPlan getRegionPlan(final RegionState state,
      final ServerName serverToExclude, final boolean forceNewPlan) {
    // Pickup existing plan or make a new one
    String encodedName = state.getRegion().getEncodedName();
    List<ServerName> servers = this.serverManager.getOnlineServersList();
    // The remove below hinges on the fact that the call to
    // serverManager.getOnlineServersList() returns a copy
    if (serverToExclude != null) servers.remove(serverToExclude);
    if (servers.isEmpty()) return null;
    RegionPlan randomPlan = new RegionPlan(state.getRegion(), null,
      balancer.randomAssignment(servers));
    boolean newPlan = false;
    RegionPlan existingPlan = null;
    synchronized (this.regionPlans) {
      existingPlan = this.regionPlans.get(encodedName);
      if (existingPlan != null && existingPlan.getDestination() != null) {
        LOG.debug("Found an existing plan for " +
            state.getRegion().getRegionNameAsString() +
       " destination server is + " + existingPlan.getDestination().toString());
      }
      if (forceNewPlan || existingPlan == null 
              || existingPlan.getDestination() == null 
              || existingPlan.getDestination().equals(serverToExclude)) {
        newPlan = true;
        this.regionPlans.put(encodedName, randomPlan);
      }
    }
    if (newPlan) {
      debugLog(state.getRegion(), "No previous transition plan was found (or we are ignoring " +
        "an existing plan) for " + state.getRegion().getRegionNameAsString() +
        " so generated a random one; " + randomPlan + "; " +
        serverManager.countOfRegionServers() +
        " (online=" + serverManager.getOnlineServers().size() +
        ", exclude=" + serverToExclude + ") available servers");
        return randomPlan;
      }
      debugLog(state.getRegion(), "Using pre-existing plan for region " +
        state.getRegion().getRegionNameAsString() + "; plan=" + existingPlan);
      return existingPlan;
  }

  /**
   * Unassign the list of regions. Configuration knobs:
   * hbase.bulk.waitbetween.reopen indicates the number of milliseconds to
   * wait before unassigning another region from this region server
   *
   * @param regions
   * @throws InterruptedException
   */
  public void unassign(List<HRegionInfo> regions) {
    int waitTime = this.master.getConfiguration().getInt(
        "hbase.bulk.waitbetween.reopen", 0);
    for (HRegionInfo region : regions) {
      if (isRegionInTransition(region) != null)
        continue;
      unassign(region, false);
      while (isRegionInTransition(region) != null) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
      }
      if (waitTime > 0)
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
    }
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC.
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   */
  public void unassign(HRegionInfo region) {
    unassign(region, false);
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC.
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   * @param force if region should be closed even if already closing
   */
  public void unassign(HRegionInfo region, boolean force) {
    debugLog(region, "Starting unassignment of region " +
      region.getRegionNameAsString() + " (offlining)");
    synchronized (this.regions) {
      // Check if this region is currently assigned
      if (!regions.containsKey(region)) {
        debugLog(region, "Attempted to unassign region " +
          region.getRegionNameAsString() + " but it is not " +
          "currently assigned anywhere");
        return;
      }
    }
    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    RegionState state;
    synchronized (regionsInTransition) {
      state = regionsInTransition.get(encodedName);
      if (state == null) {

         // Create the znode in CLOSING state
        try {
          ZKAssign.createNodeClosing(
            master.getZooKeeper(), region, master.getServerName());
        } catch (KeeperException e) {
          master.abort("Unexpected ZK exception creating node CLOSING", e);
          return;
        }
        state = new RegionState(region, RegionState.State.PENDING_CLOSE);
        regionsInTransition.put(encodedName, state);
      } else if (force && state.isPendingClose()) {
        // JD 05/25/11
        // in my experience this is useless, when this happens it just spins
        debugLog(region, "Attempting to unassign region " +
            region.getRegionNameAsString() + " which is already pending close "
            + "but forcing an additional close");
        state.update(RegionState.State.PENDING_CLOSE);
      } else {
        debugLog(region, "Attempting to unassign region " +
          region.getRegionNameAsString() + " but it is " +
          "already in transition (" + state.getState() + ")");
        return;
      }
    }
    // Send CLOSE RPC
    ServerName server = null;
    synchronized (this.regions) {
      server = regions.get(region);
    }
    try {
      // TODO: We should consider making this look more like it does for the
      // region open where we catch all throwables and never abort
      if (serverManager.sendRegionClose(server, state.getRegion())) {
        debugLog(region, "Sent CLOSE to " + server + " for region " +
          region.getRegionNameAsString());
        return;
      }
      // This never happens. Currently regionserver close always return true.
      LOG.warn("Server " + server + " region CLOSE RPC returned false for " +
        region.getEncodedName());
    } catch (NotServingRegionException nsre) {
      LOG.info("Server " + server + " returned " + nsre + " for " +
        region.getEncodedName());
      // Presume that master has stale data.  Presume remote side just split.
      // Presume that the split message when it comes in will fix up the master's
      // in memory cluster state.
      return;
    } catch (Throwable t) {
      if (t instanceof RemoteException) {
        t = ((RemoteException)t).unwrapRemoteException();
        if (t instanceof NotServingRegionException) {
          if (checkIfRegionBelongsToDisabling(region)) {
            // Remove from the regionsinTransition map
            LOG.info("While trying to recover the table "
                + region.getTableNameAsString()
                + " to DISABLED state the region " + region
                + " was offlined but the table was in DISABLING state");
            synchronized (this.regionsInTransition) {
              this.regionsInTransition.remove(region.getEncodedName());
            }
            // Remove from the regionsMap
            synchronized (this.regions) {
              this.regions.remove(region);
            }
          }
        }
      }
      LOG.info("Server " + server + " returned " + t + " for " +
        region.getEncodedName());
      // Presume retry or server will expire.
    }
  }

  /**
   * Waits until the specified region has completed assignment.
   * <p>
   * If the region is already assigned, returns immediately.  Otherwise, method
   * blocks until the region is assigned.
   * @param regionInfo region to wait on assignment for
   * @throws InterruptedException
   */
  public void waitForAssignment(HRegionInfo regionInfo)
  throws InterruptedException {
    synchronized(regions) {
      while(!regions.containsKey(regionInfo)) {
        regions.wait();
      }
    }
  }

  /**
   * Assigns the ROOT region.
   * <p>
   * Assumes that ROOT is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly unsets the current root region location in ZooKeeper and assigns
   * ROOT to a random RegionServer.
   * @throws KeeperException
   */
  public void assignRoot() throws KeeperException {
    RootLocationEditor.deleteRootLocation(this.master.getZooKeeper());
    assign(HRegionInfo.ROOT_REGIONINFO, true);
  }

  /**
   * Assigns the META region.
   * <p>
   * Assumes that META is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly assigns META to a random RegionServer.
   */
  public void assignMeta() {
    // Force assignment to a random server
    assign(HRegionInfo.FIRST_META_REGIONINFO, true);
  }

  /**
   * Assigns all user regions, if any.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown
   * @throws InterruptedException
   * @throws IOException
   */
  public void assignUserRegions(List<HRegionInfo> regions, List<ServerName> servers)
  throws IOException, InterruptedException {
    if (regions == null)
      return;
    Map<ServerName, List<HRegionInfo>> bulkPlan = null;
    // Generate a round-robin bulk assignment plan
    bulkPlan = balancer.roundRobinAssignment(regions, servers);
    LOG.info("Bulk assigning " + regions.size() + " region(s) round-robin across " +
               servers.size() + " server(s)");
    // Use fixed count thread pool assigning.
    BulkAssigner ba = new StartupBulkAssigner(this.master, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Assigns all user regions, if any exist.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown and the cluster
   * should be shutdown.
   * @throws InterruptedException
   * @throws IOException
   */
  public void assignAllUserRegions() throws IOException, InterruptedException {
    // Get all available servers
    List<ServerName> servers = serverManager.getOnlineServersList();

    // Scan META for all user regions, skipping any disabled tables
    Map<HRegionInfo, ServerName> allRegions =
      MetaReader.fullScan(catalogTracker, this.zkTable.getDisabledTables(), true);
    if (allRegions == null || allRegions.isEmpty()) return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = master.getConfiguration().
      getBoolean("hbase.master.startup.retainassign", true);

    Map<ServerName, List<HRegionInfo>> bulkPlan = null;
    if (retainAssignment) {
      // Reuse existing assignment info
      bulkPlan = balancer.retainAssignment(allRegions, servers);
    } else {
      // assign regions in round-robin fashion
      assignUserRegions(new ArrayList<HRegionInfo>(allRegions.keySet()), servers);
      return;
    }
    LOG.info("Bulk assigning " + allRegions.size() + " region(s) across " +
      servers.size() + " server(s), retainAssignment=" + retainAssignment);

    // Use fixed count thread pool assigning.
    BulkAssigner ba = new StartupBulkAssigner(this.master, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Run bulk assign on startup.  Does one RCP per regionserver passing a
   * batch of reginons using {@link SingleServerBulkAssigner}.
   * Uses default {@link #getUncaughtExceptionHandler()}
   * which will abort the Server if exception.
   */
  static class StartupBulkAssigner extends BulkAssigner {
    final Map<ServerName, List<HRegionInfo>> bulkPlan;
    final AssignmentManager assignmentManager;

    StartupBulkAssigner(final Server server,
        final Map<ServerName, List<HRegionInfo>> bulkPlan,
        final AssignmentManager am) {
      super(server);
      this.bulkPlan = bulkPlan;
      this.assignmentManager = am;
    }

    @Override
    public boolean bulkAssign(boolean sync) throws InterruptedException {
      // Disable timing out regions in transition up in zk while bulk assigning.
      this.assignmentManager.timeoutMonitor.bulkAssign(true);
      try {
        return super.bulkAssign(sync);
      } finally {
        // Reenable timing out regions in transition up in zi.
        this.assignmentManager.timeoutMonitor.bulkAssign(false);
      }
    }

    @Override
    protected String getThreadNamePrefix() {
      return this.server.getServerName() + "-StartupBulkAssigner";
    }

    @Override
    protected void populatePool(java.util.concurrent.ExecutorService pool) {
      for (Map.Entry<ServerName, List<HRegionInfo>> e: this.bulkPlan.entrySet()) {
        pool.execute(new SingleServerBulkAssigner(e.getKey(), e.getValue(),
          this.assignmentManager));
      }
    }

    protected boolean waitUntilDone(final long timeout)
    throws InterruptedException {
      Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
      for (List<HRegionInfo> regionList : bulkPlan.values()) {
        regionSet.addAll(regionList);
      }
      return this.assignmentManager.waitUntilNoRegionsInTransition(timeout, regionSet);
    }

    @Override
    protected long getTimeoutOnRIT() {
      // Guess timeout.  Multiply the number of regions on a random server
      // by how long we thing one region takes opening.
      long perRegionOpenTimeGuesstimate =
        this.server.getConfiguration().getLong("hbase.bulk.assignment.perregion.open.time", 1000);
      int regionsPerServer =
        this.bulkPlan.entrySet().iterator().next().getValue().size();
      long timeout = perRegionOpenTimeGuesstimate * regionsPerServer;
      LOG.debug("Timeout-on-RIT=" + timeout);
      return timeout;
    }
  }

  /**
   * Bulk user region assigner.
   * If failed assign, lets timeout in RIT do cleanup.
   */
  static class GeneralBulkAssigner extends StartupBulkAssigner {
    GeneralBulkAssigner(final Server server,
        final Map<ServerName, List<HRegionInfo>> bulkPlan,
        final AssignmentManager am) {
      super(server, bulkPlan, am);
    }

    @Override
    protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
      return new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.warn("Assigning regions in " + t.getName(), e);
        }
      };
    }
  }

  /**
   * Manage bulk assigning to a server.
   */
  static class SingleServerBulkAssigner implements Runnable {
    private final ServerName regionserver;
    private final List<HRegionInfo> regions;
    private final AssignmentManager assignmentManager;

    SingleServerBulkAssigner(final ServerName regionserver,
        final List<HRegionInfo> regions, final AssignmentManager am) {
      this.regionserver = regionserver;
      this.regions = regions;
      this.assignmentManager = am;
    }
    @Override
    public void run() {
      this.assignmentManager.assign(this.regionserver, this.regions);
    }
  }

  /**
   * Wait until no regions in transition.
   * @param timeout How long to wait.
   * @return True if nothing in regions in transition.
   * @throws InterruptedException
   */
  boolean waitUntilNoRegionsInTransition(final long timeout)
  throws InterruptedException {
    // Blocks until there are no regions in transition. It is possible that
    // there
    // are regions in transition immediately after this returns but guarantees
    // that if it returns without an exception that there was a period of time
    // with no regions in transition from the point-of-view of the in-memory
    // state of the Master.
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    synchronized (regionsInTransition) {
      while (regionsInTransition.size() > 0 && !this.master.isStopped()
          && remaining > 0) {
        regionsInTransition.wait(remaining);
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
    }
    return regionsInTransition.isEmpty();
  }

  /**
   * Wait until no regions from set regions are in transition.
   * @param timeout How long to wait.
   * @param regions set of regions to wait for
   * @return True if nothing in regions in transition.
   * @throws InterruptedException
   */
  boolean waitUntilNoRegionsInTransition(final long timeout, Set<HRegionInfo> regions)
  throws InterruptedException {
    // Blocks until there are no regions in transition.
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    boolean stillInTransition = true;
    synchronized (regionsInTransition) {
      while (regionsInTransition.size() > 0 && !this.master.isStopped() &&
          remaining > 0 && stillInTransition) {
        int count = 0;
        for (RegionState rs : regionsInTransition.values()) {
          if (regions.contains(rs.getRegion())) {
            count++;
            break;
          }
        }
        if (count == 0) {
          stillInTransition = false;
          break;
        }
        regionsInTransition.wait(remaining);
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
    }
    return stillInTransition;
  }

  /**
   * Rebuild the list of user regions and assignment information.
   * <p>
   * Returns a map of servers that are not found to be online and the regions
   * they were hosting.
   * @return map of servers not online to their assigned regions, as stored
   *         in META
   * @throws IOException
   */
  Map<ServerName, List<Pair<HRegionInfo, Result>>> rebuildUserRegions()
  throws IOException, KeeperException {
    // Region assignment from META
    List<Result> results = MetaReader.fullScanOfResults(this.catalogTracker);
    // Map of offline servers and their regions to be returned
    Map<ServerName, List<Pair<HRegionInfo,Result>>> offlineServers =
      new TreeMap<ServerName, List<Pair<HRegionInfo, Result>>>();
    // Iterate regions in META
    for (Result result : results) {
      Pair<HRegionInfo, ServerName> region = MetaReader.metaRowToRegionPair(result);
      if (region == null) continue;
      HRegionInfo regionInfo = region.getFirst();
      ServerName regionLocation = region.getSecond();
      String tableName = regionInfo.getTableNameAsString();
      if (regionLocation == null) {
        // regionLocation could be null if createTable didn't finish properly.
        // When createTable is in progress, HMaster restarts.
        // Some regions have been added to .META., but have not been assigned.
        // When this happens, the region's table must be in ENABLING state.
        // It can't be in ENABLED state as that is set when all regions are
        // assigned.
        // It can't be in DISABLING state, because DISABLING state transitions
        // from ENABLED state when application calls disableTable.
        // It can't be in DISABLED state, because DISABLED states transitions
        // from DISABLING state.
        if (false == checkIfRegionsBelongsToEnabling(regionInfo)) {
          LOG.warn("Region " + regionInfo.getEncodedName() +
            " has null regionLocation." + " But its table " + tableName +
            " isn't in ENABLING state.");
        }
        addTheTablesInPartialState(this.disablingTables, this.enablingTables, regionInfo,
            tableName);
      } else if (!this.serverManager.isServerOnline(regionLocation)) {
        // Region is located on a server that isn't online
        List<Pair<HRegionInfo, Result>> offlineRegions =
          offlineServers.get(regionLocation);
        if (offlineRegions == null) {
          offlineRegions = new ArrayList<Pair<HRegionInfo,Result>>(1);
          offlineServers.put(regionLocation, offlineRegions);
        }
        offlineRegions.add(new Pair<HRegionInfo,Result>(regionInfo, result));
      } else {
        // Region is being served and on an active server
        // add only if region not in disabled and enabling table
        if (false == checkIfRegionBelongsToDisabled(regionInfo)
            && false == checkIfRegionsBelongsToEnabling(regionInfo)) {
          regions.put(regionInfo, regionLocation);
          addToServers(regionLocation, regionInfo);
        }
        addTheTablesInPartialState(this.disablingTables, this.enablingTables, regionInfo,
            tableName);
      }
    }
    return offlineServers;
  }

  private void addTheTablesInPartialState(Set<String> disablingTables,
      Set<String> enablingTables, HRegionInfo regionInfo,
      String disablingTableName) {
    if (checkIfRegionBelongsToDisabling(regionInfo)) {
      disablingTables.add(disablingTableName);
    } else if (checkIfRegionsBelongsToEnabling(regionInfo)) {
      enablingTables.add(disablingTableName);
    }
  }

  /**
   * Recover the tables that were not fully moved to DISABLED state. These
   * tables are in DISABLING state when the master restarted/switched.
   * 
   * @param disablingTables
   * @return
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private boolean recoverTableInDisablingState(Set<String> disablingTables)
      throws KeeperException, TableNotFoundException, IOException {
    boolean isWatcherCreated = false;
    if (disablingTables.size() != 0) {
      // Create a watcher on the zookeeper node
      ZKUtil.listChildrenAndWatchForNewChildren(watcher,
          watcher.assignmentZNode);
      isWatcherCreated = true;
      for (String tableName : disablingTables) {
        // Recover by calling DisableTableHandler
        LOG.info("The table " + tableName
            + " is in DISABLING state.  Hence recovering by moving the table"
            + " to DISABLED state.");
        new DisableTableHandler(this.master, tableName.getBytes(),
            catalogTracker, this, true).process();
      }
    }
    return isWatcherCreated;
  }

  /**
   * Recover the tables that are not fully moved to ENABLED state. These tables
   * are in ENABLING state when the master restarted/switched
   * 
   * @param enablingTables
   * @param isWatcherCreated
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInEnablingState(Set<String> enablingTables,
      boolean isWatcherCreated) throws KeeperException, TableNotFoundException,
      IOException {
    if (enablingTables.size() != 0) {
      if (false == isWatcherCreated) {
        ZKUtil.listChildrenAndWatchForNewChildren(watcher,
            watcher.assignmentZNode);
      }
      for (String tableName : enablingTables) {
        // Recover by calling EnableTableHandler
        LOG.info("The table " + tableName
            + " is in ENABLING state.  Hence recovering by moving the table"
            + " to ENABLED state.");
        // enableTable in sync way during master startup,
        // no need to invoke coprocessor
        new EnableTableHandler(this.master, tableName.getBytes(),
            catalogTracker, this, true).process();
      }
    }
  }

  private boolean checkIfRegionsBelongsToEnabling(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    return getZKTable().isEnablingTable(tableName);
  }

  private boolean checkIfRegionBelongsToDisabled(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    return getZKTable().isDisabledTable(tableName);
  }

  private boolean checkIfRegionBelongsToDisabling(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    return getZKTable().isDisablingTable(tableName);
  }

  /**
   * Processes list of dead servers from result of META scan.
   * <p>
   * This is used as part of failover to handle RegionServers which failed
   * while there was no active master.
   * <p>
   * Method stubs in-memory data to be as expected by the normal server shutdown
   * handler.
   *
   * @param deadServers
   * @throws IOException
   * @throws KeeperException
   */
  private void processDeadServers(
      Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers)
  throws IOException, KeeperException {
    for (Map.Entry<ServerName, List<Pair<HRegionInfo,Result>>> deadServer:
        deadServers.entrySet()) {
      List<Pair<HRegionInfo,Result>> regions = deadServer.getValue();
      for (Pair<HRegionInfo,Result> region : regions) {
        HRegionInfo regionInfo = region.getFirst();
        Result result = region.getSecond();
        // If region was in transition (was in zk) force it offline for reassign
        try {
          RegionTransitionData data = ZKAssign.getData(watcher,
              regionInfo.getEncodedName());

          // If zk node of this region has been updated by a live server,
          // we consider that this region is being handled.
          // So we should skip it and process it in processRegionsInTransition.
          if (data != null && data.getOrigin() != null &&
	      serverManager.isServerOnline(data.getOrigin())) {
            LOG.info("The region " + regionInfo.getEncodedName()
                + "is being handled on " + data.getOrigin());
            continue;
          }
          // Process with existing RS shutdown code
          boolean assign =
            ServerShutdownHandler.processDeadRegion(regionInfo, result, this,
            this.catalogTracker);
          if (assign) {
            ZKAssign.createOrForceNodeOffline(watcher, regionInfo,
              master.getServerName());
          }
        } catch (KeeperException.NoNodeException nne) {
          // This is fine
        }
      }
    }
  }

  /*
   * Presumes caller has taken care of necessary locking modifying servers Map.
   * @param hsi
   * @param hri
   */
  private void addToServers(final ServerName sn, final HRegionInfo hri) {
    Set<HRegionInfo> hris = servers.get(sn);
    if (hris == null) {
      hris = new ConcurrentSkipListSet<HRegionInfo>();
      servers.put(sn, hris);
    }
    if (!hris.contains(hri)) hris.add(hri);
  }

  /**
   * @return A copy of the Map of regions currently in transition.
   */
  public NavigableMap<String, RegionState> getRegionsInTransition() {
    synchronized (this.regionsInTransition) {
      return new TreeMap<String, RegionState>(this.regionsInTransition);
    }
  }

  /**
   * @return True if regions in transition.
   */
  public boolean isRegionsInTransition() {
    synchronized (this.regionsInTransition) {
      return !this.regionsInTransition.isEmpty();
    }
  }

  /**
   * @param hri Region to check.
   * @return Returns null if passed region is not in transition else the current
   * RegionState
   */
  public RegionState isRegionInTransition(final HRegionInfo hri) {
    synchronized (this.regionsInTransition) {
      return this.regionsInTransition.get(hri.getEncodedName());
    }
  }

  /**
   * Clears the specified region from being in transition.
   * @param hri Region to remove.
   */
  public void clearRegionFromTransition(HRegionInfo hri) {
    synchronized (this.regionsInTransition) {
      this.regionsInTransition.remove(hri.getEncodedName());
    }
    synchronized (this.regions) {
      this.regions.remove(hri);
      for (Set<HRegionInfo> regions : this.servers.values()) {
        regions.remove(hri);
      }
    }
    clearRegionPlan(hri);
  }

  /**
   * @param region Region whose plan we are to clear.
   */
  void clearRegionPlan(final HRegionInfo region) {
    synchronized (this.regionPlans) {
      this.regionPlans.remove(region.getEncodedName());
    }
  }

  /**
   * Wait on region to clear regions-in-transition.
   * @param hri Region to wait on.
   * @throws IOException
   */
  public void waitOnRegionToClearRegionsInTransition(final HRegionInfo hri)
  throws IOException {
    if (isRegionInTransition(hri) == null) return;
    RegionState rs = null;
    // There is already a timeout monitor on regions in transition so I
    // should not have to have one here too?
    while(!this.master.isStopped() && (rs = isRegionInTransition(hri)) != null) {
      Threads.sleep(1000);
      LOG.info("Waiting on " + rs + " to clear regions-in-transition");
    }
    if (this.master.isStopped()) {
      LOG.info("Giving up wait on regions in " +
        "transition because stoppable.isStopped is set");
    }
  }


  /**
   * Gets the online regions of the specified table.
   * This method looks at the in-memory state.  It does not go to <code>.META.</code>.
   * Only returns <em>online</em> regions.  If a region on this table has been
   * closed during a disable, etc., it will be included in the returned list.
   * So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName
   * @return Online regions from <code>tableName</code>
   */
  public List<HRegionInfo> getRegionsOfTable(byte[] tableName) {
    List<HRegionInfo> tableRegions = new ArrayList<HRegionInfo>();
    HRegionInfo boundary =
      new HRegionInfo(tableName, null, null);
    synchronized (this.regions) {
      for (HRegionInfo regionInfo: this.regions.tailMap(boundary).keySet()) {
        if(Bytes.equals(regionInfo.getTableName(), tableName)) {
          tableRegions.add(regionInfo);
        } else {
          break;
        }
      }
    }
    return tableRegions;
  }

  /**
   * Monitor to check for time outs on region transition operations
   */
  public class TimeoutMonitor extends Chore {
    private final int timeout;
    private boolean bulkAssign = false;

    /**
     * Creates a periodic monitor to check for time outs on region transition
     * operations.  This will deal with retries if for some reason something
     * doesn't happen within the specified timeout.
     * @param period
   * @param stopper When {@link Stoppable#isStopped()} is true, this thread will
   * cleanup and exit cleanly.
     * @param timeout
     */
    public TimeoutMonitor(final int period, final Stoppable stopper,
        final int timeout) {
      super("AssignmentTimeoutMonitor", period, stopper);
      this.timeout = timeout;
    }

    /**
     * @param bulkAssign If true, we'll suspend checking regions in transition
     * up in zookeeper.  If false, will reenable check.
     * @return Old setting for bulkAssign.
     */
    public boolean bulkAssign(final boolean bulkAssign) {
      boolean result = this.bulkAssign;
      this.bulkAssign = bulkAssign;
      return result;
    }

    @Override
    protected void chore() {
      // If bulkAssign in progress, suspend checks
      if (this.bulkAssign) return;
      List<HRegionInfo> unassigns = new ArrayList<HRegionInfo>();
      Map<HRegionInfo, Boolean> assigns =
        new HashMap<HRegionInfo, Boolean>();
      synchronized (regionsInTransition) {
        // Iterate all regions in transition checking for time outs
        long now = System.currentTimeMillis();
        for (RegionState regionState : regionsInTransition.values()) {
          if (regionState.getStamp() + timeout <= now) {
            HRegionInfo regionInfo = regionState.getRegion();
            LOG.info("Regions in transition timed out:  " + regionState);
            // Expired!  Do a retry.
            switch (regionState.getState()) {
              case CLOSED:
                LOG.info("Region " + regionInfo.getEncodedName() +
                  " has been CLOSED for too long, waiting on queued " +
                  "ClosedRegionHandler to run or server shutdown");
                // Update our timestamp.
                regionState.updateTimestampToNow();
                break;
              case OFFLINE:
                LOG.info("Region has been OFFLINE for too long, " +
                  "reassigning " + regionInfo.getRegionNameAsString() +
                  " to a random server");
                assigns.put(regionState.getRegion(), Boolean.FALSE);
                break;
              case PENDING_OPEN:
                LOG.info("Region has been PENDING_OPEN for too " +
                    "long, reassigning region=" +
                    regionInfo.getRegionNameAsString());
                assigns.put(regionState.getRegion(), Boolean.TRUE);
                break;
              case OPENING:
                LOG.info("Region has been OPENING for too " +
                  "long, reassigning region=" +
                  regionInfo.getRegionNameAsString());
                // Should have a ZK node in OPENING state
                try {
                  String node = ZKAssign.getNodeName(watcher,
                      regionInfo.getEncodedName());
                  Stat stat = new Stat();
                  RegionTransitionData data = ZKAssign.getDataNoWatch(watcher,
                      node, stat);
                  if (data == null) {
                    LOG.warn("Data is null, node " + node + " no longer exists");
                    break;
                  }
                  if (data.getEventType() == EventType.RS_ZK_REGION_OPENED) {
                    LOG.debug("Region has transitioned to OPENED, allowing " +
                        "watched event handlers to process");
                    break;
                  } else if (data.getEventType() !=
                      EventType.RS_ZK_REGION_OPENING) {
                    LOG.warn("While timing out a region in state OPENING, " +
                        "found ZK node in unexpected state: " +
                        data.getEventType());
                    break;
                  }
                  // Attempt to transition node into OFFLINE
                  try {
                    data = new RegionTransitionData(
                      EventType.M_ZK_REGION_OFFLINE, regionInfo.getRegionName(),
                        master.getServerName());
                    if (ZKUtil.setData(watcher, node, data.getBytes(),
                        stat.getVersion())) {
                      // Node is now OFFLINE, let's trigger another assignment
                      ZKUtil.getDataAndWatch(watcher, node); // re-set the watch
                      LOG.info("Successfully transitioned region=" +
                          regionInfo.getRegionNameAsString() + " into OFFLINE" +
                          " and forcing a new assignment");
                      assigns.put(regionState.getRegion(), Boolean.TRUE);
                    }
                  } catch (KeeperException.NoNodeException nne) {
                    // Node did not exist, can't time this out
                  }
                } catch (KeeperException ke) {
                  LOG.error("Unexpected ZK exception timing out CLOSING region",
                      ke);
                  break;
                }
                break;
              case OPEN:
                LOG.error("Region has been OPEN for too long, " +
                "we don't know where region was opened so can't do anything");
                synchronized(regionState) {
                  regionState.updateTimestampToNow();
                }
                break;

              case PENDING_CLOSE:
                LOG.info("Region has been PENDING_CLOSE for too " +
                    "long, running forced unassign again on region=" +
                    regionInfo.getRegionNameAsString());
                  try {
                    // If the server got the RPC, it will transition the node
                    // to CLOSING, so only do something here if no node exists
                    if (!ZKUtil.watchAndCheckExists(watcher,
                      ZKAssign.getNodeName(watcher, regionInfo.getEncodedName()))) {
                      // Queue running of an unassign -- do actual unassign
                      // outside of the regionsInTransition lock.
                      unassigns.add(regionInfo);
                    }
                  } catch (NoNodeException e) {
                    LOG.debug("Node no longer existed so not forcing another " +
                      "unassignment");
                  } catch (KeeperException e) {
                    LOG.warn("Unexpected ZK exception timing out a region " +
                      "close", e);
                  }
                  break;
              case CLOSING:
                LOG.info("Region has been CLOSING for too " +
                  "long, this should eventually complete or the server will " +
                  "expire, doing nothing");
                break;
            }
          }
        }
      }
      // Finish the work for regions in PENDING_CLOSE state
      for (HRegionInfo hri: unassigns) {
        unassign(hri, true);
      }
      for (Map.Entry<HRegionInfo, Boolean> e: assigns.entrySet()){
        assign(e.getKey(), false, e.getValue());
      }
    }
  }

  /**
   * Process shutdown server removing any assignments.
   * @param sn Server that went down.
   * @return list of regions in transition on this server
   */
  public List<RegionState> processServerShutdown(final ServerName sn) {
    // Clean out any existing assignment plans for this server
    synchronized (this.regionPlans) {
      for (Iterator <Map.Entry<String, RegionPlan>> i =
          this.regionPlans.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, RegionPlan> e = i.next();
        ServerName otherSn = e.getValue().getDestination();
        // The name will be null if the region is planned for a random assign.
        if (otherSn != null && otherSn.equals(sn)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
      }
    }
    // TODO: Do we want to sync on RIT here?
    // Remove this server from map of servers to regions, and remove all regions
    // of this server from online map of regions.
    Set<HRegionInfo> deadRegions = null;
    List<RegionState> rits = new ArrayList<RegionState>();
    synchronized (this.regions) {
      Set<HRegionInfo> assignedRegions = this.servers.remove(sn);
      if (assignedRegions == null || assignedRegions.isEmpty()) {
        // No regions on this server, we are done, return empty list of RITs
        return rits;
      }
      deadRegions = new TreeSet<HRegionInfo>(assignedRegions);
      for (HRegionInfo region : deadRegions) {
        this.regions.remove(region);
      }
    }
    // See if any of the regions that were online on this server were in RIT
    // If they are, normal timeouts will deal with them appropriately so
    // let's skip a manual re-assignment.
    synchronized (regionsInTransition) {
      for (RegionState region : this.regionsInTransition.values()) {
        if (deadRegions.remove(region.getRegion())) {
          rits.add(region);
        }
      }
    }
    return rits;
  }

  /**
   * Update inmemory structures.
   * @param sn Server that reported the split
   * @param parent Parent region that was split
   * @param a Daughter region A
   * @param b Daughter region B
   */
  public void handleSplitReport(final ServerName sn, final HRegionInfo parent,
      final HRegionInfo a, final HRegionInfo b) {
    regionOffline(parent);
    regionOnline(a, sn);
    regionOnline(b, sn);

    // There's a possibility that the region was splitting while a user asked
    // the master to disable, we need to make sure we close those regions in
    // that case. This is not racing with the region server itself since RS
    // report is done after the split transaction completed.
    if (this.zkTable.isDisablingOrDisabledTable(
        parent.getTableNameAsString())) {
      unassign(a);
      unassign(b);
    }
  }

  /**
   * @return A clone of current assignments. Note, this is assignments only.
   * If a new server has come in and it has no regions, it will not be included
   * in the returned Map.
   */
  Map<ServerName, List<HRegionInfo>> getAssignments() {
    // This is an EXPENSIVE clone.  Cloning though is the safest thing to do.
    // Can't let out original since it can change and at least the loadbalancer
    // wants to iterate this exported list.  We need to synchronize on regions
    // since all access to this.servers is under a lock on this.regions.
    Map<ServerName, List<HRegionInfo>> result = null;
    synchronized (this.regions) {
      result = new HashMap<ServerName, List<HRegionInfo>>(this.servers.size());
      for (Map.Entry<ServerName, Set<HRegionInfo>> e: this.servers.entrySet()) {
        result.put(e.getKey(), new ArrayList<HRegionInfo>(e.getValue()));
      }
    }
    return result;
  }

  /**
   * @param encodedRegionName Region encoded name.
   * @return Null or a {@link Pair} instance that holds the full {@link HRegionInfo}
   * and the hosting servers {@link ServerName}.
   */
  Pair<HRegionInfo, ServerName> getAssignment(final byte [] encodedRegionName) {
    String name = Bytes.toString(encodedRegionName);
    synchronized(this.regions) {
      for (Map.Entry<HRegionInfo, ServerName> e: this.regions.entrySet()) {
        if (e.getKey().getEncodedName().equals(name)) {
          return new Pair<HRegionInfo, ServerName>(e.getKey(), e.getValue());
        }
      }
    }
    return null;
  }

  /**
   * @param plan Plan to execute.
   */
  void balance(final RegionPlan plan) {
    synchronized (this.regionPlans) {
      this.regionPlans.put(plan.getRegionName(), plan);
    }
    unassign(plan.getRegionInfo());
  }

  /**
   * Run through remaining regionservers and unassign all catalog regions.
   */
  void unassignCatalogRegions() {
    this.servers.entrySet();
    synchronized (this.regions) {
      for (Map.Entry<ServerName, Set<HRegionInfo>> e: this.servers.entrySet()) {
        Set<HRegionInfo> regions = e.getValue();
        if (regions == null || regions.isEmpty()) continue;
        for (HRegionInfo hri: regions) {
          if (hri.isMetaRegion()) {
            unassign(hri);
          }
        }
      }
    }
  }

  /**
   * State of a Region while undergoing transitions.
   */
  public static class RegionState implements Writable {
    private HRegionInfo region;

    public enum State {
      OFFLINE,        // region is in an offline state
      PENDING_OPEN,   // sent rpc to server to open but has not begun
      OPENING,        // server has begun to open but not yet done
      OPEN,           // server opened region and updated meta
      PENDING_CLOSE,  // sent rpc to server to close but has not begun
      CLOSING,        // server has begun to close but not yet done
      CLOSED,         // server closed region and updated meta
      SPLITTING,      // server started split of a region
      SPLIT           // server completed split of a region
    }

    private State state;
    // Many threads can update the state at the stamp at the same time
    private final AtomicLong stamp;
    private ServerName serverName;

    public RegionState() {
      this.stamp = new AtomicLong(System.currentTimeMillis());
    }

    RegionState(HRegionInfo region, State state) {
      this(region, state, System.currentTimeMillis(), null);
    }

    RegionState(HRegionInfo region, State state, long stamp, ServerName serverName) {
      this.region = region;
      this.state = state;
      this.stamp = new AtomicLong(stamp);
      this.serverName = serverName;
    }

    public void update(State state, long stamp, ServerName serverName) {
      this.state = state;
      updateTimestamp(stamp);
      this.serverName = serverName;
    }

    public void update(State state) {
      this.state = state;
      updateTimestampToNow();
      this.serverName = null;
    }

    public void updateTimestamp(long stamp) {
      this.stamp.set(stamp);
    }

    public void updateTimestampToNow() {
      this.stamp.set(System.currentTimeMillis());
    }

    public State getState() {
      return state;
    }

    public long getStamp() {
      return stamp.get();
    }

    public HRegionInfo getRegion() {
      return region;
    }

    public boolean isClosing() {
      return state == State.CLOSING;
    }

    public boolean isClosed() {
      return state == State.CLOSED;
    }

    public boolean isPendingClose() {
      return state == State.PENDING_CLOSE;
    }

    public boolean isOpening() {
      return state == State.OPENING;
    }

    public boolean isOpened() {
      return state == State.OPEN;
    }

    public boolean isPendingOpen() {
      return state == State.PENDING_OPEN;
    }

    public boolean isOffline() {
      return state == State.OFFLINE;
    }

    public boolean isSplitting() {
      return state == State.SPLITTING;
    }
 
    public boolean isSplit() {
      return state == State.SPLIT;
    }

    @Override
    public String toString() {
      return region.getRegionNameAsString()
        + " state=" + state
        + ", ts=" + stamp
        + ", server=" + serverName;
    }

    /**
     * A slower (but more easy-to-read) stringification 
     */
    public String toDescriptiveString() {
      long lstamp = stamp.get();
      long relTime = System.currentTimeMillis() - lstamp;
      
      return region.getRegionNameAsString()
        + " state=" + state
        + ", ts=" + new Date(lstamp) + " (" + (relTime/1000) + "s ago)"
        + ", server=" + serverName;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      region = new HRegionInfo();
      region.readFields(in);
      state = State.valueOf(in.readUTF());
      stamp.set(in.readLong());
    }

    @Override
    public void write(DataOutput out) throws IOException {
      region.write(out);
      out.writeUTF(state.name());
      out.writeLong(stamp.get());
    }
  }

  public void stop() {
    this.timeoutMonitor.interrupt();
  }
  
  /**
   * Check whether the RegionServer is online.
   */
  public boolean isServerOnline(ServerName serverName) {
    return this.serverManager.isServerOnline(serverName);
  }
}
