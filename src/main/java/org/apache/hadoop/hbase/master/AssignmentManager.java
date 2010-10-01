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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.master.LoadBalancer.RegionPlan;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTableDisable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.NodeAndData;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

  /** Regions currently in transition. */
  private final Map<String, RegionState> regionsInTransition =
    new TreeMap<String, RegionState>();

  /** Plans for region movement. Key is the encoded version of a region name*/
  // TODO: When do plans get cleaned out?  Ever? In server open and in server
  // shutdown processing -- St.Ack
  private final ConcurrentNavigableMap<String, RegionPlan> regionPlans =
    new ConcurrentSkipListMap<String, RegionPlan>();

  /** Set of tables that have been disabled. */
  private final Set<String> disabledTables =
    Collections.synchronizedSet(new HashSet<String>());

  /**
   * Server to regions assignment map.
   * Contains the set of regions currently assigned to a given server.
   * This Map and {@link #regions} are tied.  Always update this in tandem
   * with the other under a lock on {@link #regions}
   * @see #regions
   */
  private final NavigableMap<HServerInfo, List<HRegionInfo>> servers =
    new TreeMap<HServerInfo, List<HRegionInfo>>();

  /**
   * Region to server assignment map.
   * Contains the server a given region is currently assigned to.
   * This Map and {@link #servers} are tied.  Always update this in tandem
   * with the other under a lock on {@link #regions}
   * @see #servers
   */
  private final SortedMap<HRegionInfo,HServerInfo> regions =
    new TreeMap<HRegionInfo,HServerInfo>();

  private final ExecutorService executorService;

  /**
   * Constructs a new assignment manager.
   *
   * <p>This manager must be started with {@link #start()}.
   *
   * @param status master status
   * @param serverManager
   * @param catalogTracker
   * @param service
   */
  public AssignmentManager(Server master, ServerManager serverManager,
      CatalogTracker catalogTracker, final ExecutorService service) {
    super(master.getZooKeeper());
    this.master = master;
    this.serverManager = serverManager;
    this.catalogTracker = catalogTracker;
    this.executorService = service;
    Configuration conf = master.getConfiguration();
    this.timeoutMonitor = new TimeoutMonitor(
        conf.getInt("hbase.master.assignment.timeoutmonitor.period", 30000),
        master,
        conf.getInt("hbase.master.assignment.timeoutmonitor.timeout", 15000));
    Threads.setDaemonThreadRunning(timeoutMonitor,
        master.getServerName() + ".timeoutMonitor");
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
   * Handle failover.  Restore state from META and ZK.  Handle any regions in
   * transition.  Presumes <code>.META.</code> and <code>-ROOT-</code> deployed.
   * @throws KeeperException
   * @throws IOException
   */
  void processFailover() throws KeeperException, IOException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: Check list of user regions and their assignments against regionservers.
    // TODO: Regions that have a null location and are not in regionsInTransitions
    // need to be handled.
    // TODO: Regions that are on servers that are not in our online list need
    // reassigning.

    // Scan META to build list of existing regions, servers, and assignment
    rebuildUserRegions();
    // Pickup any disabled tables
    rebuildDisabledTables();
    // Check existing regions in transition
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(watcher,
        watcher.assignmentZNode);
    if (nodes.isEmpty()) {
      LOG.info("No regions in transition in ZK to process on failover");
      return;
    }
    LOG.info("Failed-over master needs to process " + nodes.size() +
        " regions in transition");
    for (String encodedRegionName: nodes) {
      processRegionInTransition(encodedRegionName, null);
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
    boolean intransistion = processRegionInTransition(hri.getEncodedName(), hri);
    if (!intransistion) return intransistion;
    synchronized(this.regionsInTransition) {
      while (!this.master.isStopped() &&
          this.regionsInTransition.containsKey(hri.getEncodedName())) {
        this.regionsInTransition.wait();
      }
    }
    return intransistion;
  }

  /**
   * Process failover of <code>encodedName</code>.  Look in 
   * @param encodedRegionName Region to process failover for.
   * @param encodedRegionName RegionInfo.  If null we'll go get it from meta table.
   * @return
   * @throws KeeperException 
   * @throws IOException 
   */
  boolean processRegionInTransition(final String encodedRegionName,
      final HRegionInfo regionInfo)
  throws KeeperException, IOException {
    RegionTransitionData data = ZKAssign.getData(watcher, encodedRegionName);
    if (data == null) return false;
    HRegionInfo hri = (regionInfo != null)? regionInfo:
      MetaReader.getRegion(catalogTracker, data.getRegionName()).getFirst();
    processRegionsInTransition(data, hri);
    return true;
  }

  void processRegionsInTransition(final RegionTransitionData data,
      final HRegionInfo regionInfo)
  throws KeeperException {
    String encodedRegionName = regionInfo.getEncodedName();
    LOG.info("Processing region " + regionInfo.getRegionNameAsString() +
      " in state " + data.getEventType());
    switch (data.getEventType()) {
    case RS_ZK_REGION_CLOSING:
      // Just insert region into RIT.
      // If this never updates the timeout will trigger new assignment
      regionsInTransition.put(encodedRegionName, new RegionState(
          regionInfo, RegionState.State.CLOSING, data.getStamp()));
      break;

    case RS_ZK_REGION_CLOSED:
      // Region is closed, insert into RIT and handle it
      regionsInTransition.put(encodedRegionName, new RegionState(
          regionInfo, RegionState.State.CLOSED, data.getStamp()));
      new ClosedRegionHandler(master, this, data, regionInfo).process();
      break;

    case RS_ZK_REGION_OPENING:
      // Just insert region into RIT
      // If this never updates the timeout will trigger new assignment
      regionsInTransition.put(encodedRegionName, new RegionState(
          regionInfo, RegionState.State.OPENING, data.getStamp()));
      break;

    case RS_ZK_REGION_OPENED:
      // Region is opened, insert into RIT and handle it
      regionsInTransition.put(encodedRegionName, new RegionState(
          regionInfo, RegionState.State.OPENING, data.getStamp()));
      new OpenedRegionHandler(master, this, data, regionInfo,
          serverManager.getServerInfo(data.getServerName())).process();
      break;
    }
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
      // Verify this is a known server
      if (!serverManager.isServerOnline(data.getServerName()) &&
          !this.master.getServerName().equals(data.getServerName())) {
        LOG.warn("Attempted to handle region transition for server but " +
          "server is not online: " + data);
        return;
      }
      String encodedName = HRegionInfo.encodeRegionName(data.getRegionName());
      String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
      LOG.debug("Handling transition=" + data.getEventType() +
        ", server=" + data.getServerName() + ", region=" + prettyPrintedRegionName);
      RegionState regionState = regionsInTransition.get(encodedName);
      switch (data.getEventType()) {
        case M_ZK_REGION_OFFLINE:
          // Nothing to do.
          break;

        case RS_ZK_REGION_CLOSING:
          // Should see CLOSING after we have asked it to CLOSE or additional
          // times after already being in state of CLOSING
          if (regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSING for region " + prettyPrintedRegionName +
              " from server " + data.getServerName() + " but region was in " +
              " the state " + regionState + " and not " +
              "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Transition to CLOSING (or update stamp if already CLOSING)
          regionState.update(RegionState.State.CLOSING, data.getStamp());
          break;

        case RS_ZK_REGION_CLOSED:
          // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
          if (regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSED for region " + prettyPrintedRegionName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Handle CLOSED by assigning elsewhere or stopping if a disable
          // If we got here all is good.  Need to update RegionState -- else
          // what follows will fail because not in expected state.
          regionState.update(RegionState.State.CLOSED, data.getStamp());
          this.executorService.submit(new ClosedRegionHandler(master,
            this, data, regionState.getRegion()));
          break;

        case RS_ZK_REGION_OPENING:
          // Should see OPENING after we have asked it to OPEN or additional
          // times after already being in state of OPENING
          if(regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENING for region " +
                prettyPrintedRegionName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Transition to OPENING (or update stamp if already OPENING)
          regionState.update(RegionState.State.OPENING, data.getStamp());
          break;

        case RS_ZK_REGION_OPENED:
          // Should see OPENED after OPENING but possible after PENDING_OPEN
          if(regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENED for region " +
                prettyPrintedRegionName +
                " from server " + data.getServerName() + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Handle OPENED by removing from transition and deleted zk node
          this.executorService.submit(
            new OpenedRegionHandler(master, this, data, regionState.getRegion(),
              this.serverManager.getServerInfo(data.getServerName())));
          break;
      }
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
      synchronized(regionsInTransition) {
        try {
          RegionTransitionData data = ZKAssign.getData(watcher, path);
          if(data == null) {
            return;
          }
          handleRegion(data);
        } catch (KeeperException e) {
          master.abort("Unexpected ZK exception reading unassigned node data", e);
        }
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
      synchronized(regionsInTransition) {
        try {
          RegionTransitionData data = ZKAssign.getData(watcher, path);
          if(data == null) {
            return;
          }
          handleRegion(data);
        } catch (KeeperException e) {
          master.abort("Unexpected ZK exception reading unassigned node data", e);
        }
      }
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   *   <li>Read all children and handle them</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.assignmentZNode)) {
      synchronized(regionsInTransition) {
        try {
          List<NodeAndData> newNodes = ZKUtil.watchAndGetNewChildren(watcher,
              watcher.assignmentZNode);
          for(NodeAndData newNode : newNodes) {
            LOG.debug("Handling new unassigned node: " + newNode);
            handleRegion(RegionTransitionData.fromBytes(newNode.getData()));
          }
        } catch(KeeperException e) {
          master.abort("Unexpected ZK exception reading unassigned children", e);
        }
      }
    }
  }

  /**
   * Marks the region as online.  Removes it from regions in transition and
   * updates the in-memory assignment information.
   * <p>
   * Used when a region has been successfully opened on a region server.
   * @param regionInfo
   * @param serverInfo
   */
  public void regionOnline(HRegionInfo regionInfo, HServerInfo serverInfo) {
    synchronized (this.regionsInTransition) {
      RegionState rs =
        this.regionsInTransition.remove(regionInfo.getEncodedName());
      if (rs != null) {
        this.regionsInTransition.notifyAll();
      } else {
        LOG.warn("Asked online a region that was not in " +
          "regionsInTransition: " + rs);
      }
    }
    synchronized (this.regions) {
      // Add check
      HServerInfo hsi = this.regions.get(regionInfo);
      if (hsi != null) LOG.warn("Overwriting " + regionInfo.getEncodedName() +
        " on " + hsi);
      this.regions.put(regionInfo, serverInfo);
      addToServers(serverInfo, regionInfo);
    }
    // Remove plan if one.
    this.regionPlans.remove(regionInfo.getEncodedName());
    // Update timers for all regions in transition going against this server.
    updateTimers(serverInfo);
  }

  /**
   * Touch timers for all regions in transition that have the passed
   * <code>hsi</code> in common.
   * Call this method whenever a server checks in.  Doing so helps the case where
   * a new regionserver has joined the cluster and its been given 1k regions to
   * open.  If this method is tickled every time the region reports in a
   * successful open then the 1k-th region won't be timed out just because its
   * sitting behind the open of 999 other regions.  This method is NOT used
   * as part of bulk assign -- there we have a different mechanism for extending
   * the regions in transition timer (we turn it off temporarily -- because
   * there is no regionplan involved when bulk assigning.
   * @param hsi
   */
  private void updateTimers(final HServerInfo hsi) {
    // This loop could be expensive
    for (Map.Entry<String, RegionPlan> e: this.regionPlans.entrySet()) {
      if (e.getValue().getDestination().equals(hsi)) {
        RegionState rs = null;
        synchronized (this.regionsInTransition) {
          rs = this.regionsInTransition.get(e.getKey());
        }
        if (rs != null) {
          synchronized (rs) {
            rs.update(rs.getState());
          }
        }
      }
    }
  }

  /**
   * Marks the region as offline.  Removes it from regions in transition and
   * removes in-memory assignment information.
   * <p>
   * Used when a region has been closed and should remain closed.
   * @param regionInfo
   * @param serverInfo
   */
  public void regionOffline(final HRegionInfo regionInfo) {
    synchronized(this.regionsInTransition) {
      if (this.regionsInTransition.remove(regionInfo.getEncodedName()) != null) {
        this.regionsInTransition.notifyAll();
      }
    }
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
      HServerInfo serverInfo = this.regions.remove(regionInfo);
      if (serverInfo != null) {
        List<HRegionInfo> serverRegions = this.servers.get(serverInfo);
        serverRegions.remove(regionInfo);
      } else {
        LOG.warn("Asked offline a region that was not online: " + regionInfo);
      }
    }
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
   * @param regionName server to be assigned
   */
  public void assign(HRegionInfo region) {
    RegionState state = addToRegionsInTransition(region);
    synchronized (state) {
      assign(state);
    }
  }

  /**
   * Bulk assign regions to <code>destination</code>.  If we fail in any way,
   * we'll abort the server.
   * @param destination
   * @param regions Regions to assign.
   */
  public void assign(final HServerInfo destination,
      final List<HRegionInfo> regions) {
    LOG.debug("Bulk assigning " + regions.size() + " region(s) to " +
      destination.getServerName());
    List<RegionState> states = new ArrayList<RegionState>(regions.size());
    synchronized (this.regionsInTransition) {
      for (HRegionInfo region: regions) {
        states.add(forceRegionStateToOffline(region));
      }
    }
    // Presumption is that only this thread will be updating the state at this
    // time; i.e. handlers on backend won't be trying to set it to OPEN, etc.
    for (RegionState state: states) {
      if (!setOfflineInZooKeeper(state)) {
        return;
      }
    }
    for (RegionState state: states) {
      // Transition RegionState to PENDING_OPEN here in master; means we've
      // sent the open.  We're a little ahead of ourselves here since we've not
      // yet sent out the actual open but putting this state change after the
      // call to open risks our writing PENDING_OPEN after state has been moved
      // to OPENING by the regionserver.
      state.update(RegionState.State.PENDING_OPEN);
    }
    try {
      // Send OPEN RPC. This can fail if the server on other end is is not up.
      this.serverManager.sendRegionOpen(destination, regions);
    } catch (Throwable t) {
      this.master.abort("Failed assignment of regions to " + destination, t);
      return;
    }
    LOG.debug("Bulk assigning done for " + destination.getServerName());
  }

  /**
   * @param region
   * @return
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
   */
  private void assign(final RegionState state) {
    if (!setOfflineInZooKeeper(state)) return;
    RegionPlan plan = getRegionPlan(state);
    try {
      LOG.debug("Assigning region " + state.getRegion().getRegionNameAsString() +
        " to " + plan.getDestination().getServerName());
      // Send OPEN RPC. This can fail if the server on other end is is not up.
      serverManager.sendRegionOpen(plan.getDestination(), state.getRegion());
      // Transition RegionState to PENDING_OPEN
      state.update(RegionState.State.PENDING_OPEN);
    } catch (Throwable t) {
      LOG.warn("Failed assignment of " +
        state.getRegion().getRegionNameAsString() + " to " +
        plan.getDestination(), t);
      // Clean out plan we failed execute and one that doesn't look like it'll
      // succeed anyways; we need a new plan!
      this.regionPlans.remove(state.getRegion().getEncodedName());
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
        new RuntimeException("Unexpected state trying to OFFLINE; " + state);
      this.master.abort("Unexpected state trying to OFFLINE; " + state,
        new IllegalStateException());
      return false;
    }
    state.update(RegionState.State.OFFLINE);
    try {
      if(!ZKAssign.createOrForceNodeOffline(master.getZooKeeper(),
          state.getRegion(), master.getServerName())) {
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
   * @param state
   * @return Plan for passed <code>state</code> (If none currently, it creates one)
   */
  RegionPlan getRegionPlan(final RegionState state) {
    // Pickup existing plan or make a new one
    String encodedName = state.getRegion().getEncodedName();
    RegionPlan newPlan = new RegionPlan(state.getRegion(), null,
      LoadBalancer.randomAssignment(serverManager.getOnlineServersList()));
    RegionPlan existingPlan = regionPlans.putIfAbsent(encodedName, newPlan);
    RegionPlan plan = null;
    if (existingPlan == null) {
      LOG.debug("No previous transition plan for " +
        state.getRegion().getRegionNameAsString() +
        " so generated a random one; " + newPlan + "; " +
        serverManager.countOfRegionServers() +
        " (online=" + serverManager.getOnlineServers().size() +
        ") available servers");
      plan = newPlan;
    } else {
      LOG.debug("Using preexisting plan=" + existingPlan);
      plan = existingPlan;
    }
    return plan;
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC.
   * <p>
   * If a RegionPlan is already set, it will remain.  If this is being used
   * to disable a table, be sure to use {@link #disableTable(String)} to ensure
   * regions are not onlined after being closed.
   *
   * @param regionName server to be unassigned
   */
  public void unassign(HRegionInfo region) {
    LOG.debug("Starting unassignment of region " +
      region.getRegionNameAsString() + " (offlining)");
    // Check if this region is currently assigned
    if (!regions.containsKey(region)) {
      LOG.debug("Attempted to unassign region " +
        region.getRegionNameAsString() + " but it is not " +
        "currently assigned anywhere");
      return;
    }
    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    RegionState state;
    synchronized (regionsInTransition) {
      state = regionsInTransition.get(encodedName);
      if (state == null) {
        state = new RegionState(region, RegionState.State.PENDING_CLOSE);
        regionsInTransition.put(encodedName, state);
      } else {
        LOG.debug("Attempting to unassign region " +
          region.getRegionNameAsString() + " but it is " +
          "already in transition (" + state.getState() + ")");
        return;
      }
    }
    // Send CLOSE RPC
    try {
      serverManager.sendRegionClose(regions.get(region), state.getRegion());
    } catch (NotServingRegionException e) {
      LOG.warn("Attempted to close region " + region.getRegionNameAsString() +
        " but got an NSRE", e);
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
    assign(HRegionInfo.ROOT_REGIONINFO);
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
    assign(HRegionInfo.FIRST_META_REGIONINFO);
  }

  /**
   * Assigns all user regions, if any exist.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown and the cluster
   * should be shutdown.
   */
  public void assignAllUserRegions() throws IOException {
    // First experiment at synchronous assignment
    // Simpler because just wait for no regions in transition

    // Scan META for all user regions
    List<HRegionInfo> allRegions =
      MetaScanner.listAllRegions(master.getConfiguration());
    if (allRegions == null || allRegions.isEmpty()) return;

    // Get all available servers
    List<HServerInfo> servers = serverManager.getOnlineServersList();
    LOG.info("Bulk assigning " + allRegions.size() + " region(s) across " +
      servers.size() + " server(s)");

    // Generate a cluster startup region placement plan
    Map<HServerInfo, List<HRegionInfo>> bulkPlan =
      LoadBalancer.bulkAssignment(allRegions, servers);

    // Make a fixed thread count pool to run bulk assignments.  Thought is that
    // if a 1k cluster, running 1k bulk concurrent assignment threads will kill
    // master, HDFS or ZK?
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setDaemon(true);
    builder.setNameFormat(this.master.getServerName() + "-BulkAssigner-%1$d");
    builder.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // Abort if exception of any kind.
        master.abort("Uncaught exception bulk assigning in " + t.getName(), e);
      }
    });
    int threadCount =
      this.master.getConfiguration().getInt("hbase.bulk.assignment.threadpool.size", 20);
    java.util.concurrent.ExecutorService pool =
      Executors.newFixedThreadPool(threadCount, builder.build());
    // Disable timing out regions in transition up in zk while bulk assigning.
    this.timeoutMonitor.bulkAssign(true);
    try {
      for (Map.Entry<HServerInfo, List<HRegionInfo>> e: bulkPlan.entrySet()) {
        pool.execute(new SingleServerBulkAssigner(e.getKey(), e.getValue()));
      }
      // Wait for no regions to be in transition
      try {
        // How long to wait on empty regions-in-transition.  When we timeout,
        // we'll put back in place the monitor of R-I-T.  It should do fixup
        // if server crashed during bulk assign, etc.
        long timeout =
          this.master.getConfiguration().getInt("hbase.bulk.assignment.waiton.empty.rit", 10 * 60 * 1000);
        waitUntilNoRegionsInTransition(timeout);
      } catch (InterruptedException e) {
        LOG.error("Interrupted waiting for regions to be assigned", e);
        throw new IOException(e);
      }
    } finally {
      // We're done with the pool.  It'll exit when its done all in queue.
      pool.shutdown();
      // Reenable timing out regions in transition up in zi.
      this.timeoutMonitor.bulkAssign(false);
    }
    LOG.info("Bulk assigning done");
  }

  /**
   * Manage bulk assigning to a server.
   */
  class SingleServerBulkAssigner implements Runnable {
    private final HServerInfo regionserver;
    private final List<HRegionInfo> regions;
    SingleServerBulkAssigner(final HServerInfo regionserver,
        final List<HRegionInfo> regions) {
      this.regionserver = regionserver;
      this.regions = regions;
    }
    @Override
    public void run() {
      assign(this.regionserver, this.regions);
    }
  }

  private void rebuildUserRegions() throws IOException {
    Map<HRegionInfo,HServerAddress> allRegions =
      MetaReader.fullScan(catalogTracker);
    for (Map.Entry<HRegionInfo,HServerAddress> region: allRegions.entrySet()) {
      HServerAddress regionLocation = region.getValue();
      HRegionInfo regionInfo = region.getKey();
      if (regionLocation == null) {
        this.regions.put(regionInfo, null);
        continue;
      }
      HServerInfo serverInfo = serverManager.getHServerInfo(regionLocation);
      regions.put(regionInfo, serverInfo);
      addToServers(serverInfo, regionInfo);
    }
  }

  /*
   * Presumes caller has taken care of necessary locking modifying servers Map.
   * @param hsi
   * @param hri
   */
  private void addToServers(final HServerInfo hsi, final HRegionInfo hri) {
    List<HRegionInfo> hris = servers.get(hsi);
    if (hris == null) {
      hris = new ArrayList<HRegionInfo>();
      servers.put(hsi, hris);
    }
    hris.add(hri);
  }

  /**
   * Blocks until there are no regions in transition.  It is possible that there
   * are regions in transition immediately after this returns but guarantees
   * that if it returns without an exception that there was a period of time
   * with no regions in transition from the point-of-view of the in-memory
   * state of the Master.
   * @param timeout How long to wait on empty regions-in-transition.
   * @throws InterruptedException
   */
  public void waitUntilNoRegionsInTransition(final long timeout)
  throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    synchronized (this.regionsInTransition) {
      while(this.regionsInTransition.size() > 0 &&
          !this.master.isStopped() && remaining > 0) {
        this.regionsInTransition.wait(remaining);
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
    }
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
   * Checks if the specified table has been disabled by the user.
   * @param tableName
   * @return
   */
  public boolean isTableDisabled(String tableName) {
    synchronized(disabledTables) {
      return disabledTables.contains(tableName);
    }
  }

  /**
   * Checks if the table of the specified region has been disabled by the user.
   * @param regionName
   * @return
   */
  public boolean isTableOfRegionDisabled(byte [] regionName) {
    return isTableDisabled(Bytes.toString(
        HRegionInfo.getTableName(regionName)));
  }

  /**
   * Sets the specified table to be disabled.
   * @param tableName table to be disabled
   */
  public void disableTable(String tableName) {
    synchronized(disabledTables) {
      if(!isTableDisabled(tableName)) {
        disabledTables.add(tableName);
        try {
          ZKTableDisable.disableTable(master.getZooKeeper(), tableName);
        } catch (KeeperException e) {
          LOG.warn("ZK error setting table as disabled", e);
        }
      }
    }
  }

  /**
   * Unsets the specified table from being disabled.
   * <p>
   * This operation only acts on the in-memory
   * @param tableName table to be undisabled
   */
  public void undisableTable(String tableName) {
    synchronized(disabledTables) {
      if(isTableDisabled(tableName)) {
        disabledTables.remove(tableName);
        try {
          ZKTableDisable.undisableTable(master.getZooKeeper(), tableName);
        } catch (KeeperException e) {
          LOG.warn("ZK error setting table as disabled", e);
        }
      }
    }
  }

  /**
   * Rebuild the set of disabled tables from zookeeper.  Used during master
   * failover.
   */
  private void rebuildDisabledTables() {
    synchronized(disabledTables) {
      List<String> disabledTables;
      try {
        disabledTables = ZKTableDisable.getDisabledTables(master.getZooKeeper());
      } catch (KeeperException e) {
        LOG.warn("ZK error getting list of disabled tables", e);
        return;
      }
      if(!disabledTables.isEmpty()) {
        LOG.info("Rebuilt list of " + disabledTables.size() + " disabled " +
            "tables from zookeeper");
        disabledTables.addAll(disabledTables);
      }
    }
  }

  /**
   * Gets the online regions of the specified table.
   * @param tableName
   * @return
   */
  public List<HRegionInfo> getRegionsOfTable(byte[] tableName) {
    List<HRegionInfo> tableRegions = new ArrayList<HRegionInfo>();
    for(HRegionInfo regionInfo : regions.tailMap(new HRegionInfo(
        new HTableDescriptor(tableName), null, null)).keySet()) {
      if(Bytes.equals(regionInfo.getTableDesc().getName(), tableName)) {
        tableRegions.add(regionInfo);
      } else {
        break;
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
      synchronized (regionsInTransition) {
        // Iterate all regions in transition checking for time outs
        long now = System.currentTimeMillis();
        for (RegionState regionState : regionsInTransition.values()) {
          if(regionState.getStamp() + timeout <= now) {
            HRegionInfo regionInfo = regionState.getRegion();
            LOG.info("Regions in transition timed out:  " + regionState);
            // Expired!  Do a retry.
            switch (regionState.getState()) {
              case OFFLINE:
              case CLOSED:
                LOG.info("Region has been OFFLINE or CLOSED for too long, " +
                  "reassigning " + regionInfo.getRegionNameAsString());
                assign(regionState.getRegion());
                break;
              case PENDING_OPEN:
              case OPENING:
                LOG.info("Region has been PENDING_OPEN  or OPENING for too " +
                  "long, reassigning region=" +
                  regionInfo.getRegionNameAsString());
                // TODO: Possible RACE in here if RS is right now sending us an
                // OPENED to handle.  Otherwise, after our call to assign, which
                // forces zk state to OFFLINE, any actions by RS should cause
                // it abort its open w/ accompanying LOG.warns coming out of the
                // handleRegion method below.
                AssignmentManager.this.setOffline(regionState.getRegion());
                regionState.update(RegionState.State.OFFLINE);
                assign(regionState.getRegion());
                break;
              case OPEN:
                LOG.warn("Long-running region in OPEN state?  This should " +
                  "not happen; region=" + regionInfo.getRegionNameAsString());
                break;
              case PENDING_CLOSE:
              case CLOSING:
                LOG.info("Region has been PENDING_CLOSE or CLOSING for too " +
                  "long, running unassign again on region=" +
                  regionInfo.getRegionNameAsString());
                unassign(regionInfo);
                break;
            }
          }
        }
      }
    }
  }

  /**
   * Process shutdown server removing any assignments.
   * @param hsi Server that went down.
   */
  public void processServerShutdown(final HServerInfo hsi) {
    // Clean out any exisiting assignment plans for this server
    for (Iterator <Map.Entry<String, RegionPlan>> i =
        this.regionPlans.entrySet().iterator(); i.hasNext();) {
      Map.Entry<String, RegionPlan> e = i.next();
      if (e.getValue().getDestination().equals(hsi)) {
        // Use iterator's remove else we'll get CME
        i.remove();
      }
    }
    // Remove assignment info related to the downed server.  Remove the downed
    // server from list of servers else it looks like a server w/ no load.
    synchronized (this.regions) {
      Set<HRegionInfo> hris = new HashSet<HRegionInfo>();
      for (Map.Entry<HRegionInfo, HServerInfo> e: this.regions.entrySet()) {
        // Add to a Set -- don't call setOffline in here else we get a CME.
        if (e.getValue().equals(hsi)) hris.add(e.getKey());
      }
      for (HRegionInfo hri: hris) setOffline(hri);
      this.servers.remove(hsi);
    }
    // If anything in transition related to the server, clean it up.
    synchronized (regionsInTransition) {
      // Iterate all regions in transition checking if were on this server
      final String serverName = hsi.getServerName();
      for (Map.Entry<String, RegionState> e: this.regionsInTransition.entrySet()) {
        if (!e.getKey().equals(serverName)) continue;
        RegionState regionState = e.getValue();
        switch(regionState.getState()) {
          case PENDING_OPEN:
          case OPENING:
          case OFFLINE:
          case CLOSED:
          case PENDING_CLOSE:
          case CLOSING:
            LOG.info("Region " + regionState.getRegion().getRegionNameAsString() +
              " was in state=" + regionState.getState() + " on shutdown server=" +
              serverName + ", reassigning");
            assign(regionState.getRegion());
            break;

          case OPEN:
            LOG.warn("Long-running region in OPEN state?  Should not happen");
            break;
        }
      }
    }
  }

  /**
   * Update inmemory structures.
   * @param hsi Server that reported the split
   * @param parent Parent region that was split
   * @param a Daughter region A
   * @param b Daughter region B
   */
  public void handleSplitReport(final HServerInfo hsi, final HRegionInfo parent,
      final HRegionInfo a, final HRegionInfo b) {
    regionOffline(parent);
    regionOnline(a, hsi);
    regionOnline(b, hsi);
  }

  /**
   * @return A clone of current assignments. Note, this is assignments only.
   * If a new server has come in and it has no regions, it will not be included
   * in the returned Map.
   */
  Map<HServerInfo, List<HRegionInfo>> getAssignments() {
    // This is an EXPENSIVE clone.  Cloning though is the safest thing to do.
    // Can't let out original since it can change and at least the loadbalancer
    // wants to iterate this exported list.  We need to synchronize on regions
    // since all access to this.servers is under a lock on this.regions.
    Map<HServerInfo, List<HRegionInfo>> result = null;
    synchronized (this.regions) {
      result = new HashMap<HServerInfo, List<HRegionInfo>>(this.servers.size());
      for (Map.Entry<HServerInfo, List<HRegionInfo>> e: this.servers.entrySet()) {
        List<HRegionInfo> shallowCopy = new ArrayList<HRegionInfo>(e.getValue());
        HServerInfo clone = new HServerInfo(e.getKey());
        // Set into server load the number of regions this server is carrying
        // The load balancer calculation needs it at least and its handy.
        clone.getLoad().setNumberOfRegions(e.getValue().size());
        result.put(clone, shallowCopy);
      }
    }
    return result;
  }

  /**
   * @param encodedRegionName Region encoded name.
   * @return Null or a {@link Pair} instance that holds the full {@link HRegionInfo}
   * and the hosting servers {@link HServerInfo}.
   */
  Pair<HRegionInfo, HServerInfo> getAssignment(final byte [] encodedRegionName) {
    String name = Bytes.toString(encodedRegionName);
    synchronized(this.regions) {
      for (Map.Entry<HRegionInfo, HServerInfo> e: this.regions.entrySet()) {
        if (e.getKey().getEncodedName().equals(name)) {
          return new Pair<HRegionInfo, HServerInfo>(e.getKey(), e.getValue());
        }
      }
    }
    return null;
  }

  /**
   * @param plan Plan to execute.
   */
  void balance(final RegionPlan plan) {
    this.regionPlans.put(plan.getRegionName(), plan);
    unassign(plan.getRegionInfo());
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
      CLOSED          // server closed region and updated meta
    }

    private State state;
    private long stamp;

    public RegionState() {}

    RegionState(HRegionInfo region, State state) {
      this(region, state, System.currentTimeMillis());
    }

    RegionState(HRegionInfo region, State state, long stamp) {
      this.region = region;
      this.state = state;
      this.stamp = stamp;
    }

    public void update(State state, long stamp) {
      this.state = state;
      this.stamp = stamp;
    }

    public void update(State state) {
      this.state = state;
      this.stamp = System.currentTimeMillis();
    }

    public State getState() {
      return state;
    }

    public long getStamp() {
      return stamp;
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

    @Override
    public String toString() {
      return region.getRegionNameAsString() + " state=" + state +
        ", ts=" + stamp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      region = new HRegionInfo();
      region.readFields(in);
      state = State.valueOf(in.readUTF());
      stamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      region.write(out);
      out.writeUTF(state.name());
      out.writeLong(stamp);
    }
  }
}
