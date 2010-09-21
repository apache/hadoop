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
import org.apache.hadoop.hbase.zookeeper.ZKUtil.NodeAndData;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.KeeperException;

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
  // TODO: When do plans get cleaned out?  Ever?
  // Its cleaned on server shutdown processing -- St.Ack
  private final Map<String, RegionPlan> regionPlans =
    new TreeMap<String, RegionPlan>();

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
   * transition.
   * @throws KeeperException
   * @throws IOException
   */
  void processFailover() throws KeeperException, IOException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // Scan META to build list of existing regions, servers, and assignment
    rebuildUserRegions();
    // Pickup any disabled tables
    rebuildDisabledTables();
    // Check existing regions in transition
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(watcher,
        watcher.assignmentZNode);
    if(nodes.isEmpty()) {
      LOG.info("No regions in transition in ZK to process on failover");
      return;
    }
    LOG.info("Failed-over master needs to process " + nodes.size() +
        " regions in transition");
    for(String regionName : nodes) {
      RegionTransitionData data = ZKAssign.getData(watcher, regionName);
      HRegionInfo regionInfo =
        MetaReader.getRegion(catalogTracker, data.getRegionName()).getFirst();
      String encodedName = regionInfo.getEncodedName();
      switch(data.getEventType()) {
        case RS_ZK_REGION_CLOSING:
          // Just insert region into RIT.
          // If this never updates the timeout will trigger new assignment
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.CLOSING,
                  data.getStamp()));
          break;

        case RS_ZK_REGION_CLOSED:
          // Region is closed, insert into RIT and handle it
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.CLOSED,
                  data.getStamp()));
          new ClosedRegionHandler(master, this, data, regionInfo).process();
          break;

        case RS_ZK_REGION_OPENING:
          // Just insert region into RIT
          // If this never updates the timeout will trigger new assignment
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.OPENING,
                  data.getStamp()));
          break;

        case RS_ZK_REGION_OPENED:
          // Region is opened, insert into RIT and handle it
          regionsInTransition.put(encodedName,
              new RegionState(regionInfo, RegionState.State.OPENING,
                  data.getStamp()));
          new OpenedRegionHandler(master, this, data, regionInfo,
              serverManager.getServerInfo(data.getServerName())).process();
          break;
      }
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
  private void handleRegion(RegionTransitionData data) {
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
      switch(data.getEventType()) {
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
    synchronized(regionsInTransition) {
      regionsInTransition.remove(regionInfo.getEncodedName());
      regionsInTransition.notifyAll();
    }
    synchronized(regions) {
      regions.put(regionInfo, serverInfo);
      addToServers(serverInfo, regionInfo);
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
  public void regionOffline(HRegionInfo regionInfo) {
    synchronized(regionsInTransition) {
      regionsInTransition.remove(regionInfo.getEncodedName());
      regionsInTransition.notifyAll();
    }
    synchronized(regions) {
      HServerInfo serverInfo = regions.remove(regionInfo);
      List<HRegionInfo> serverRegions = servers.get(serverInfo);
      serverRegions.remove(regionInfo);
    }
  }

  /**
   * Sets the region as offline by removing in-memory assignment information but
   * retaining transition information.
   * <p>
   * Used when a region has been closed but should be reassigned.
   * @param regionInfo
   */
  public void setOffline(HRegionInfo regionInfo) {
    synchronized(regions) {
      HServerInfo serverInfo = regions.remove(regionInfo);
      List<HRegionInfo> serverRegions = servers.get(serverInfo);
      serverRegions.remove(regionInfo);
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
    LOG.debug("Starting assignment for region " + region.getRegionNameAsString());
    // Grab the state of this region and synchronize on it
    String encodedName = region.getEncodedName();
    RegionState state;
    synchronized (regionsInTransition) {
      state = regionsInTransition.get(encodedName);
      if (state == null) {
        state = new RegionState(region, RegionState.State.OFFLINE);
        regionsInTransition.put(encodedName, state);
      }
    }
    // This here gap between synchronizations looks like a hole but it should
    // be ok because the assign below would protect against being called with
    // a state instance that is not in the right 'state' -- St.Ack 20100920.
    synchronized (state) {
      assign(state);
    }
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state 
   */
  private void assign(final RegionState state) {
    if (!state.isClosed() && !state.isOffline()) {
      LOG.info("Attempting to assign region but it is in transition and in " +
        "an unexpected state:" + state);
      return;
    } else {
      state.update(RegionState.State.OFFLINE);
    }
    try {
      if(!ZKAssign.createOrForceNodeOffline(master.getZooKeeper(),
          state.getRegion(), master.getServerName())) {
        LOG.warn("Attempted to create/force node into OFFLINE state before " +
            "completing assignment but failed to do so");
        return;
      }
    } catch (KeeperException e) {
      master.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return;
    }
    // Pickup existing plan or make a new one
    String encodedName = state.getRegion().getEncodedName();
    RegionPlan plan;
    synchronized(regionPlans) {
      plan = regionPlans.get(encodedName);
      if (plan == null) {
        LOG.debug("No previous transition plan for " +
            state.getRegion().getRegionNameAsString() +
            " so generating a random one; " + serverManager.countOfRegionServers() +
            " (online=" + serverManager.getOnlineServers().size() + ") available servers");
        plan = new RegionPlan(state.getRegion(), null,
          LoadBalancer.randomAssignment(serverManager.getOnlineServersList()));
        regionPlans.put(encodedName, plan);
      } else {
        LOG.debug("Using preexisting plan=" + plan);
      }
    }
    try {
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
      synchronized(regionPlans) {
        this.regionPlans.remove(encodedName);
      }
    }
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
      LOG.debug("Attempted to unassign region " + region.getRegionNameAsString() +
        " but it is not " +
        "currently assigned anywhere");
      return;
    }
    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    RegionState state;
    synchronized(regionsInTransition) {
      state = regionsInTransition.get(encodedName);
      if(state == null) {
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
    LOG.info("Assigning " + allRegions.size() + " region(s) across " +
      servers.size() + " server(s)");

    // Generate a cluster startup region placement plan
    Map<HServerInfo, List<HRegionInfo>> bulkPlan =
      LoadBalancer.bulkAssignment(allRegions, servers);

    // Now start a thread per server to run assignment.
    for (Map.Entry<HServerInfo,List<HRegionInfo>> entry: bulkPlan.entrySet()) {
      Thread t = new BulkAssignServer(entry.getKey(), entry.getValue(), this.master);
      t.start();
    }

    // Wait for no regions to be in transition
    try {
      waitUntilNoRegionsInTransition();
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for regions to be assigned", e);
      throw new IOException(e);
    }

    LOG.info("All user regions have been assigned");
  }

  /**
   * Class to run bulk assign to a single server.
   */
  class BulkAssignServer extends Thread {
    private final List<HRegionInfo> regions;
    private final HServerInfo server;
    private final Stoppable stopper;

    BulkAssignServer(final HServerInfo server,
        final List<HRegionInfo> regions, final Stoppable stopper) {
      super("serverassign-" + server.getServerName());
      setDaemon(true);
      this.server = server;
      this.regions = regions;
      this.stopper = stopper;
    }

    @Override
    public void run() {
      // Insert a plan for each region with 'server' as the target regionserver.
      // Below, we run through regions one at a time.  The call to assign will
      // move the region into the regionsInTransition which starts up a timer.
      // if the region is not out of the regionsInTransition by a certain time,
      // it will be reassigned.  We don't want that to happen.  So, do it this
      // way a region at a time for now.  Presumably the regionserver will put
      // up a back pressure if opening a region takes time which is good since
      // this will block our adding new regions to regionsInTransition.  Later
      // make it so we can send over a lump of regions in one rpc with the
      // regionserver on remote side tickling zk on a period to prevent our
      // regionsInTransition timing out.  Currently its not possible given the
      // Executor architecture on the regionserver side.  St.Ack 20100920.
      for (HRegionInfo region : regions) {
        LOG.debug("Assigning " + region.getRegionNameAsString() + " to " + this.server);
        regionPlans.put(region.getEncodedName(), new RegionPlan(region, null, server));
        assign(region);
        if (this.stopper.isStopped()) break;
      }
    }
  }

  private void rebuildUserRegions() throws IOException {
    Map<HRegionInfo,HServerAddress> allRegions =
      MetaReader.fullScan(catalogTracker);
    for(Map.Entry<HRegionInfo,HServerAddress> region : allRegions.entrySet()) {
      HServerAddress regionLocation = region.getValue();
      HRegionInfo regionInfo = region.getKey();
      if(regionLocation == null) {
        regions.put(regionInfo, null);
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
   * @throws InterruptedException
   */
  public void waitUntilNoRegionsInTransition() throws InterruptedException {
    synchronized(regionsInTransition) {
      while(regionsInTransition.size() > 0) {
        regionsInTransition.wait();
      }
    }
  }

  /**
   * @return A copy of the Map of regions currently in transition.
   */
  public NavigableMap<String, RegionState> getRegionsInTransition() {
    return new TreeMap<String, RegionState>(this.regionsInTransition);
  }

  /**
   * @return True if regions in transition.
   */
  public boolean isRegionsInTransition() {
    return !this.regionsInTransition.isEmpty();
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
   * Unsets the specified table as disabled (enables it).
   */
  public class TimeoutMonitor extends Chore {

    private final int timeout;

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

    @Override
    protected void chore() {
      synchronized (regionsInTransition) {
        // Iterate all regions in transition checking for time outs
        long now = System.currentTimeMillis();
        for (RegionState regionState : regionsInTransition.values()) {
          if(regionState.getStamp() + timeout <= now) {
            HRegionInfo regionInfo = regionState.getRegion();
            LOG.info("Regions in transition timed out:  " + regionState);
            // Expired!  Do a retry.
            switch(regionState.getState()) {
              case OFFLINE:
              case CLOSED:
                LOG.info("Region has been OFFLINE or CLOSED for too long, " +
                    "reassigning " + regionInfo.getRegionNameAsString());
                assign(regionState.getRegion());
                break;
              case PENDING_OPEN:
              case OPENING:
                LOG.info("Region has been PENDING_OPEN or OPENING for too " +
                    "long, reassigning " + regionInfo.getRegionNameAsString());
                assign(regionState.getRegion());
                break;
              case OPEN:
                LOG.warn("Long-running region in OPEN state?  This should " +
                    "not happen");
                break;
              case PENDING_CLOSE:
              case CLOSING:
                LOG.info("Region has been PENDING_CLOSE or CLOSING for too " +
                    "long, resending close rpc");
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
    synchronized (this.regionPlans) {
      for (Iterator <Map.Entry<String, RegionPlan>> i =
          this.regionPlans.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, RegionPlan> e = i.next();
        if (e.getValue().getDestination().equals(hsi)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
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
    synchronized (this.regions) {
      checkRegion(hsi, parent, true);
      checkRegion(hsi, a, false);
      this.regions.put(a, hsi);
      this.regions.put(b, hsi);
      removeFromServers(hsi, parent, true);
      removeFromServers(hsi, a, false);
      removeFromServers(hsi, b, false);
      addToServers(hsi, a);
      addToServers(hsi, b);
    }
  }

  /*
   * Caller must hold locks on regions Map.
   * @param hsi
   * @param hri
   * @param expected
   */
  private void checkRegion(final HServerInfo hsi, final HRegionInfo hri,
      final boolean expected) {
    HServerInfo serverInfo = regions.remove(hri);
    if (expected) {
      if (serverInfo == null) {
        LOG.info("Region not on a server: " + hri.getRegionNameAsString());
      }
    } else {
      if (serverInfo != null) {
        LOG.warn("Region present on " + hsi + "; unexpected");
      }
    }
  }

  /*
   * Caller must hold locks on servers Map.
   * @param hsi
   * @param hri
   * @param expected
   */
  private void removeFromServers(final HServerInfo hsi, final HRegionInfo hri,
      final boolean expected) {
    List<HRegionInfo> serverRegions = this.servers.get(hsi);
    boolean removed = serverRegions.remove(hri);
    if (expected) {
      if (!removed) {
        LOG.warn(hri.getRegionNameAsString() + " not found on " + hsi +
          "; unexpected");
      }
    } else {
      if (removed) {
        LOG.warn(hri.getRegionNameAsString() + " found on " + hsi +
        "; unexpected");
      }
    }
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
    synchronized (this.regionPlans) {
      this.regionPlans.put(plan.getRegionName(), plan);
    }
    unassign(plan.getRegionInfo());
  }

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
