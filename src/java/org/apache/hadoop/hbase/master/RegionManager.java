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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Class to manage assigning regions to servers, state of root and meta, etc.
 */ 
class RegionManager implements HConstants {
  protected static final Log LOG = LogFactory.getLog(RegionManager.class);
  
  private AtomicReference<HServerAddress> rootRegionLocation =
    new AtomicReference<HServerAddress>(null);
  
  private volatile boolean safeMode = true;
  
  final Lock splitLogLock = new ReentrantLock();
  
  private final RootScanner rootScannerThread;
  final MetaScanner metaScannerThread;
  
  /** Set by root scanner to indicate the number of meta regions */
  private final AtomicInteger numberOfMetaRegions = new AtomicInteger();

  /** These are the online meta regions */
  private final NavigableMap<byte [], MetaRegion> onlineMetaRegions =
    new ConcurrentSkipListMap<byte [], MetaRegion>(Bytes.BYTES_COMPARATOR);

  private static final byte[] OVERLOADED = Bytes.toBytes("Overloaded");

  /*
   * Map of region name to RegionState for regions that are in transition such as
   * 
   * unassigned -> assigned -> pending -> open
   * closing -> closed -> offline
   * closing -> closed -> unassigned -> assigned -> pending -> open
   * 
   * At the end of a transition, removeRegion is used to remove the region from
   * the map (since it is no longer in transition)
   * 
   * Note: Needs to be SortedMap so we can specify a comparator
   * 
   */
  private final SortedMap<byte[], RegionState> regionsInTransition =
    Collections.synchronizedSortedMap(
        new TreeMap<byte[], RegionState>(Bytes.BYTES_COMPARATOR));

  // How many regions to assign a server at a time.
  private final int maxAssignInOneGo;

  private final HMaster master;
  private final RegionHistorian historian;
  private final float slop;

  /** Set of regions to split. */
  private final SortedMap<byte[],Pair<HRegionInfo,HServerAddress>> regionsToSplit = 
    Collections.synchronizedSortedMap(
      new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
      (Bytes.BYTES_COMPARATOR));
  /** Set of regions to compact. */
  private final SortedMap<byte[],Pair<HRegionInfo,HServerAddress>> regionsToCompact =
    Collections.synchronizedSortedMap(
      new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
      (Bytes.BYTES_COMPARATOR));

  RegionManager(HMaster master) {
    this.master = master;
    this.historian = RegionHistorian.getInstance();
    this.maxAssignInOneGo = this.master.getConfiguration().
      getInt("hbase.regions.percheckin", 10);
    this.slop = this.master.getConfiguration().getFloat("hbase.regions.slop",
      (float)0.1);

    // The root region
    rootScannerThread = new RootScanner(master, this);

    // Scans the meta table
    metaScannerThread = new MetaScanner(master, this);
    
    reassignRootRegion();
  }
  
  void start() {
    Threads.setDaemonThreadRunning(rootScannerThread,
      "RegionManager.rootScanner");
    Threads.setDaemonThreadRunning(metaScannerThread,
      "RegionManager.metaScanner");    
  }

  void unsetRootRegion() {
    synchronized (regionsInTransition) {
      rootRegionLocation.set(null);
      regionsInTransition.remove(HRegionInfo.ROOT_REGIONINFO.getRegionName());
    }
  }
  
  void reassignRootRegion() {
    unsetRootRegion();
    if (!master.shutdownRequested) {
      synchronized (regionsInTransition) {
        RegionState s = new RegionState(HRegionInfo.ROOT_REGIONINFO);
        s.setUnassigned();
        regionsInTransition.put(HRegionInfo.ROOT_REGIONINFO.getRegionName(), s);
      }
    }
  }
  
  /*
   * Assigns regions to region servers attempting to balance the load across
   * all region servers
   *
   * Note that no synchronization is necessary as the caller 
   * (ServerManager.processMsgs) already owns the monitor for the RegionManager.
   * 
   * @param info
   * @param serverName
   * @param returnMsgs
   */
  void assignRegions(HServerInfo info, String serverName,
    HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
    HServerLoad thisServersLoad = info.getLoad();
    // figure out what regions need to be assigned and aren't currently being
    // worked on elsewhere.
    Set<RegionState> regionsToAssign = regionsAwaitingAssignment();
    if (regionsToAssign.size() == 0) {
      // There are no regions waiting to be assigned.
      if (!inSafeMode()) {
        // We only do load balancing once all regions are assigned.
        // This prevents churn while the cluster is starting up.
        double avgLoad = master.serverManager.getAverageLoad();
        double avgLoadWithSlop = avgLoad +
        ((this.slop != 0)? avgLoad * this.slop: avgLoad);
        if (avgLoad > 2.0 &&
            thisServersLoad.getNumberOfRegions() > avgLoadWithSlop) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Server " + serverName +
                " is overloaded. Server load: " + 
                thisServersLoad.getNumberOfRegions() + " avg: " + avgLoad +
                ", slop: " + this.slop);
          }
          unassignSomeRegions(serverName, thisServersLoad,
              avgLoad, mostLoadedRegions, returnMsgs);
        }
      }
    } else {
      // if there's only one server, just give it all the regions
      if (master.serverManager.numServers() == 1) {
        assignRegionsToOneServer(regionsToAssign, serverName, returnMsgs);
      } else {
        // otherwise, give this server a few regions taking into account the 
        // load of all the other servers.
        assignRegionsToMultipleServers(thisServersLoad, regionsToAssign, 
            serverName, returnMsgs);
      }
    }
  }
  
  /*
   * Make region assignments taking into account multiple servers' loads.
   *
   * Note that no synchronization is needed while we iterate over
   * regionsInTransition because this method is only called by assignRegions
   * whose caller owns the monitor for RegionManager
   */ 
  private void assignRegionsToMultipleServers(final HServerLoad thisServersLoad,
    final Set<RegionState> regionsToAssign, final String serverName, 
    final ArrayList<HMsg> returnMsgs) {
    
    int nRegionsToAssign = regionsToAssign.size();
    int nregions = regionsPerServer(nRegionsToAssign, thisServersLoad);
    nRegionsToAssign -= nregions;
    if (nRegionsToAssign > 0) {
      // We still have more regions to assign. See how many we can assign
      // before this server becomes more heavily loaded than the next
      // most heavily loaded server.
      HServerLoad heavierLoad = new HServerLoad();
      int nservers = computeNextHeaviestLoad(thisServersLoad, heavierLoad);

      nregions = 0;
      
      // Advance past any less-loaded servers
      for (HServerLoad load = new HServerLoad(thisServersLoad);
        load.compareTo(heavierLoad) <= 0 && nregions < nRegionsToAssign;
        load.setNumberOfRegions(load.getNumberOfRegions() + 1), nregions++) {
        // continue;
      }

      if (nregions < nRegionsToAssign) {
        // There are some more heavily loaded servers
        // but we can't assign all the regions to this server.
        if (nservers > 0) {
          // There are other servers that can share the load.
          // Split regions that need assignment across the servers.
          nregions = (int) Math.ceil((1.0 * nRegionsToAssign)
              / (1.0 * nservers));
        } else {
          // No other servers with same load.
          // Split regions over all available servers
          nregions = (int) Math.ceil((1.0 * nRegionsToAssign)
              / (1.0 * master.serverManager.numServers()));
        }
      } else {
        // Assign all regions to this server
        nregions = nRegionsToAssign;
      }

      if (nregions > this.maxAssignInOneGo) {
        nregions = this.maxAssignInOneGo;
      }
      
      for (RegionState s: regionsToAssign) {
        LOG.info("assigning region " + Bytes.toString(s.getRegionName())+
          " to server " + serverName);
        s.setAssigned(serverName);
        this.historian.addRegionAssignment(s.getRegionInfo(), serverName);
        returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, s.getRegionInfo()));
        if (--nregions <= 0) {
          break;
        }
      }
    }
  }
  
  /*
   * @param nRegionsToAssign
   * @param thisServersLoad
   * @return How many regions we can assign to more lightly loaded servers
   */
  private int regionsPerServer(final int numUnassignedRegions,
    final HServerLoad thisServersLoad) {
    
    SortedMap<HServerLoad, Set<String>> lightServers =
      new TreeMap<HServerLoad, Set<String>>();

    // Get all the servers who are more lightly loaded than this one.
    synchronized (master.serverManager.loadToServers) {
      lightServers.putAll(master.serverManager.loadToServers.headMap(thisServersLoad));
    }

    // Examine the list of servers that are more lightly loaded than this one.
    // Pretend that we will assign regions to these more lightly loaded servers
    // until they reach load equal with ours. Then, see how many regions are left
    // unassigned. That is how many regions we should assign to this server.
    int nRegions = 0;
    for (Map.Entry<HServerLoad, Set<String>> e : lightServers.entrySet()) {
      HServerLoad lightLoad = new HServerLoad(e.getKey());
      do {
        lightLoad.setNumberOfRegions(lightLoad.getNumberOfRegions() + 1);
        nRegions += 1;
      } while (lightLoad.compareTo(thisServersLoad) <= 0
          && nRegions < numUnassignedRegions);

      nRegions *= e.getValue().size();
      if (nRegions >= numUnassignedRegions) {
        break;
      }
    }
    return nRegions;
  }
  
  /*
   * Get the set of regions that should be assignable in this pass.
   * 
   * Note that no synchronization on regionsInTransition is needed because the
   * only caller (assignRegions) whose caller owns the monitor for RegionManager
   */ 
  private Set<RegionState> regionsAwaitingAssignment() {
    // set of regions we want to assign to this server
    Set<RegionState> regionsToAssign = new HashSet<RegionState>();
    
    // Look over the set of regions that aren't currently assigned to 
    // determine which we should assign to this server.
    for (RegionState s: regionsInTransition.values()) {
      HRegionInfo i = s.getRegionInfo();
      if (i == null) {
        continue;
      }
      if (numberOfMetaRegions.get() != onlineMetaRegions.size() &&
          !i.isMetaRegion()) {
        // Can't assign user regions until all meta regions have been assigned
        // and are on-line
        continue;
      }
      if (!s.isAssigned() && !s.isClosing() && !s.isPending()) {
        s.setUnassigned();
        regionsToAssign.add(s);
      }
    }
    return regionsToAssign;
  }
  
  /*
   * Figure out the load that is next highest amongst all regionservers. Also,
   * return how many servers exist at that load. 
   */
  private int computeNextHeaviestLoad(HServerLoad referenceLoad, 
    HServerLoad heavierLoad) {
    
    SortedMap<HServerLoad, Set<String>> heavyServers =
      new TreeMap<HServerLoad, Set<String>>();
    synchronized (master.serverManager.loadToServers) {
      heavyServers.putAll(
        master.serverManager.loadToServers.tailMap(referenceLoad));
    }
    int nservers = 0;
    for (Map.Entry<HServerLoad, Set<String>> e : heavyServers.entrySet()) {
      Set<String> servers = e.getValue();
      nservers += servers.size();
      if (e.getKey().compareTo(referenceLoad) == 0) {
        // This is the load factor of the server we are considering
        nservers -= 1;
        continue;
      }

      // If we get here, we are at the first load entry that is a
      // heavier load than the server we are considering
      heavierLoad.setNumberOfRequests(e.getKey().getNumberOfRequests());
      heavierLoad.setNumberOfRegions(e.getKey().getNumberOfRegions());
      break;
    }
    return nservers;
  }
  
  
  /*
   * Assign all to the only server. An unlikely case but still possible.
   * 
   * Note that no synchronization is needed on regionsInTransition while
   * iterating on it because the only caller is assignRegions whose caller owns 
   * the monitor for RegionManager
   * 
   * @param regionsToAssign
   * @param serverName
   * @param returnMsgs
   */
  private void assignRegionsToOneServer(final Set<RegionState> regionsToAssign,
      final String serverName, final ArrayList<HMsg> returnMsgs) {
    for (RegionState s: regionsToAssign) {
      LOG.info("assigning region " + Bytes.toString(s.getRegionName()) +
          " to the only server " + serverName);
      s.setAssigned(serverName);
      this.historian.addRegionAssignment(s.getRegionInfo(), serverName);
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, s.getRegionInfo()));
    }
  }
  
  /*
   * The server checking in right now is overloaded. We will tell it to close
   * some or all of its most loaded regions, allowing it to reduce its load.
   * The closed regions will then get picked up by other underloaded machines.
   *
   * Note that no synchronization is needed because the only caller 
   * (assignRegions) whose caller owns the monitor for RegionManager
   */
  private void unassignSomeRegions(final String serverName,
      final HServerLoad load, final double avgLoad,
      final HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
    
    int numRegionsToClose = load.getNumberOfRegions() - (int)Math.ceil(avgLoad);
    LOG.debug("Choosing to reassign " + numRegionsToClose 
      + " regions. mostLoadedRegions has " + mostLoadedRegions.length 
      + " regions in it.");
    
    int regionIdx = 0;
    int regionsClosed = 0;
    int skipped = 0;
    while (regionsClosed < numRegionsToClose &&
        regionIdx < mostLoadedRegions.length) {
      HRegionInfo currentRegion = mostLoadedRegions[regionIdx];
      regionIdx++;
      // skip the region if it's meta or root
      if (currentRegion.isRootRegion() || currentRegion.isMetaTable()) {
        continue;
      }
      
      byte[] regionName = currentRegion.getRegionName();
      if (isClosing(regionName) || isUnassigned(currentRegion) ||
          isAssigned(regionName) || isPending(regionName)) {
        skipped++;
        continue;
      }
      
      LOG.debug("Going to close region " +
        currentRegion.getRegionNameAsString());
      // make a message to close the region
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, currentRegion,
        OVERLOADED));
      // mark the region as closing
      setClosing(serverName, currentRegion, false);
      // increment the count of regions we've marked
      regionsClosed++;
    }
    LOG.info("Skipped " + skipped + " region(s) that are in transition states");
  }
  
  /**
   * @return Read-only map of online regions.
   */
  public Map<byte [], MetaRegion> getOnlineMetaRegions() {
    synchronized (onlineMetaRegions) {
      return Collections.unmodifiableMap(onlineMetaRegions);
    }
  }
  
  /**
   * Stop the root and meta scanners so that the region servers serving meta
   * regions can shut down.
   */
  public void stopScanners() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("telling root scanner to stop");
    }
    rootScannerThread.interruptIfAlive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("telling meta scanner to stop");
    }
    metaScannerThread.interruptIfAlive();
    if (LOG.isDebugEnabled()) {
      LOG.debug("meta and root scanners notified");
    }
  }
  
  /** Stop the region assigner */
  public void stop() {  
    try {
      if (rootScannerThread.isAlive()) {
        rootScannerThread.join();       // Wait for the root scanner to finish.
      }
    } catch (Exception iex) {
      LOG.warn("root scanner", iex);
    }
    try {
      if (metaScannerThread.isAlive()) {
        metaScannerThread.join();       // Wait for meta scanner to finish.
      }
    } catch(Exception iex) {
      LOG.warn("meta scanner", iex);
    }
  }
  
  /**
   * Block until meta regions are online or we're shutting down.
   * @return true if we found meta regions, false if we're closing.
   */
  public boolean areAllMetaRegionsOnline() {
    synchronized (onlineMetaRegions) {
      return (rootRegionLocation.get() != null &&
          numberOfMetaRegions.get() == onlineMetaRegions.size());
    }
  }
  
  /**
   * Search our map of online meta regions to find the first meta region that 
   * should contain a pointer to <i>newRegion</i>.
   * @param newRegion
   * @return MetaRegion where the newRegion should live
   */
  public MetaRegion getFirstMetaRegionForRegion(HRegionInfo newRegion) {
    synchronized (onlineMetaRegions) {
      if (onlineMetaRegions.size() == 0) {
        return null;
      } else if (onlineMetaRegions.size() == 1) {
        return onlineMetaRegions.get(onlineMetaRegions.firstKey());
      } else {
        if (onlineMetaRegions.containsKey(newRegion.getRegionName())) {
          return onlineMetaRegions.get(newRegion.getRegionName());
        } 
        return onlineMetaRegions.get(onlineMetaRegions.headMap(
            newRegion.getTableDesc().getName()).lastKey());
      }
    }
  }
  
  /**
   * Get a set of all the meta regions that contain info about a given table.
   * @param tableName Table you need to know all the meta regions for
   * @return set of MetaRegion objects that contain the table
   * @throws NotAllMetaRegionsOnlineException
   */
  public Set<MetaRegion> getMetaRegionsForTable(byte [] tableName)
  throws NotAllMetaRegionsOnlineException {
    byte [] firstMetaRegion = null;
    Set<MetaRegion> metaRegions = new HashSet<MetaRegion>();
    if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      if (rootRegionLocation.get() == null) {
        throw new NotAllMetaRegionsOnlineException(
            Bytes.toString(HConstants.ROOT_TABLE_NAME));
      }
      metaRegions.add(new MetaRegion(rootRegionLocation.get(),
          HRegionInfo.ROOT_REGIONINFO.getRegionName()));
    } else {
      if (!areAllMetaRegionsOnline()) {
        throw new NotAllMetaRegionsOnlineException();
      }
      synchronized (onlineMetaRegions) {
        if (onlineMetaRegions.size() == 1) {
          firstMetaRegion = onlineMetaRegions.firstKey();
        } else if (onlineMetaRegions.containsKey(tableName)) {
          firstMetaRegion = tableName;
        } else {
          firstMetaRegion = onlineMetaRegions.headMap(tableName).lastKey();
        }
        metaRegions.addAll(onlineMetaRegions.tailMap(firstMetaRegion).values());
      }
    }
    return metaRegions;
  }

  /**
   * Remove a region from the region state map.
   * 
   * @param info
   */
  public void removeRegion(HRegionInfo info) {
    regionsInTransition.remove(info.getRegionName());
  }
  
  /**
   * Get metaregion that would host passed in row.
   * @param row Row need to know all the meta regions for
   * @return set of MetaRegion objects that contain the table
   * @throws NotAllMetaRegionsOnlineException
   */
  public MetaRegion getMetaRegionForRow(final byte [] row)
  throws NotAllMetaRegionsOnlineException {
    if (!areAllMetaRegionsOnline()) {
      throw new NotAllMetaRegionsOnlineException();
    }
    return this.onlineMetaRegions.floorEntry(row).getValue();
  }

  /**
   * Create a new HRegion, put a row for it into META (or ROOT), and mark the
   * new region unassigned so that it will get assigned to a region server.
   * @param newRegion HRegionInfo for the region to create
   * @param server server hosting the META (or ROOT) region where the new
   * region needs to be noted
   * @param metaRegionName name of the meta region where new region is to be
   * written
   * @throws IOException
   */
  public void createRegion(HRegionInfo newRegion, HRegionInterface server, 
      byte [] metaRegionName) 
  throws IOException {
    // 2. Create the HRegion
    HRegion region = HRegion.createHRegion(newRegion, master.rootdir,
      master.getConfiguration());

    // 3. Insert into meta
    HRegionInfo info = region.getRegionInfo();
    byte [] regionName = region.getRegionName();
    BatchUpdate b = new BatchUpdate(regionName);
    b.put(COL_REGIONINFO, Writables.getBytes(info));
    server.batchUpdate(metaRegionName, b, -1L);
    
    // 4. Close the new region to flush it to disk.  Close its log file too.
    region.close();
    region.getLog().closeAndDelete();

    // 5. Get it assigned to a server
    setUnassigned(info, true);
  }
  
  /** 
   * Set a MetaRegion as online. 
   * @param metaRegion 
   */
  public void putMetaRegionOnline(MetaRegion metaRegion) {
    onlineMetaRegions.put(metaRegion.getStartKey(), metaRegion);
  }

  /** 
   * Get a list of online MetaRegions 
   * @return list of MetaRegion objects
   */
  public List<MetaRegion> getListOfOnlineMetaRegions() {
    List<MetaRegion> regions = null;
    synchronized(onlineMetaRegions) {
      regions = new ArrayList<MetaRegion>(onlineMetaRegions.values());
    }
    return regions;
  }
  
  /** 
   * Count of online meta regions 
   * @return count of online meta regions
   */
  public int numOnlineMetaRegions() {
    return onlineMetaRegions.size();
  }
  
  /** 
   * Check if a meta region is online by its name 
   * @param startKey name of the meta region to check
   * @return true if the region is online, false otherwise
   */
  public boolean isMetaRegionOnline(byte [] startKey) {
    return onlineMetaRegions.containsKey(startKey);
  }
  
  /** 
   * Set an online MetaRegion offline - remove it from the map. 
   * @param startKey region name
   */
  public void offlineMetaRegion(byte [] startKey) {
    onlineMetaRegions.remove(startKey); 
  }
  
  /** 
   * Check if a region is on the unassigned list
   * @param info HRegionInfo to check for
   * @return true if on the unassigned list, false if it isn't. Note that this
   * means a region could not be on the unassigned list AND not be assigned, if
   * it happens to be between states.
   */
  public boolean isUnassigned(HRegionInfo info) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(info.getRegionName());
      if (s != null) {
        return s.isUnassigned();
      }
    }
    return false;
  }
  
  /**
   * Check if a region is pending 
   * @param regionName name of the region
   * @return true if pending, false otherwise
   */
  public boolean isPending(byte [] regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isPending();
      }
    }
    return false;
  }
  
  /**
   * @param regionName
   * @return true if region has been assigned
   */
  public boolean isAssigned(byte[] regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isAssigned() || s.isPending();
      }
    }
    return false;
  }

  /**
   * @param regionName
   * @return true if region is marked to be offlined.
   */
  public boolean isOfflined(byte[] regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isOfflined();
      }
    }
    return false;
  }

  /** 
   * Set a region to unassigned 
   * @param info Region to set unassigned
   * @param force if true mark region unassigned whatever its current state
   */
  public void setUnassigned(HRegionInfo info, boolean force) {
    synchronized(this.regionsInTransition) {
      RegionState s = regionsInTransition.get(info.getRegionName());
      if (s == null) {
        s = new RegionState(info);
        regionsInTransition.put(info.getRegionName(), s);
      }
      if (force || (!s.isAssigned() && !s.isPending())) {
        s.setUnassigned();
      }
    }
  }
  
  /**
   * Set a region to pending assignment 
   * @param regionName
   */
  public void setPending(byte [] regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setPending();
      }
    }
  }
  
  /** 
   * Remove the map of region names to region infos waiting to be offlined for a 
   * given server
   *  
   * @param serverName
   * @return set of infos to close
   */
  public Set<HRegionInfo> getMarkedToClose(String serverName) {
    Set<HRegionInfo> result = new HashSet<HRegionInfo>();
    synchronized (regionsInTransition) {
      for (RegionState s: regionsInTransition.values()) {
        if (s.isClosing() && !s.isClosed() &&
            s.getServerName().compareTo(serverName) == 0) {
          result.add(s.getRegionInfo());
        }
      }
    }
    return result;
  }
  
  /** 
   * Check if a region is closing 
   * @param regionName 
   * @return true if the region is marked as closing, false otherwise
   */
  public boolean isClosing(byte [] regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isClosing();
      }
    }
    return false;
  }

  /** 
   * Mark a region as closing 
   * @param serverName
   * @param regionInfo
   * @param setOffline
   */
  public void setClosing(String serverName, HRegionInfo regionInfo,
      boolean setOffline) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionInfo.getRegionName());
      if (s != null) {
        if (!s.isClosing()) {
          throw new IllegalStateException(
              "Cannot transition to closing from any other state. Region: " +
              Bytes.toString(regionInfo.getRegionName()));
        }
        return;
      }
      s = new RegionState(regionInfo);
      regionsInTransition.put(regionInfo.getRegionName(), s);
      s.setClosing(serverName, setOffline);
    }
  }
  
  /**
   * @param regionName
   */
  public void setClosed(byte[] regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setClosed();
      }
    }
  }
  /**
   * Add a meta region to the scan queue
   * @param m MetaRegion that needs to get scanned
   */
  public void addMetaRegionToScan(MetaRegion m) {
    metaScannerThread.addMetaRegionToScan(m);
  }
  
  /** 
   * Check if the initial root scan has been completed.
   * @return true if scan completed, false otherwise
   */
  public boolean isInitialRootScanComplete() {
    return rootScannerThread.isInitialScanComplete();
  }

  /** 
   * Check if the initial meta scan has been completed.
   * @return true if meta completed, false otherwise
   */  
  public boolean isInitialMetaScanComplete() {
    return metaScannerThread.isInitialScanComplete();
  }
  
  /** 
   * @return true if the initial meta scan is complete and there are no
   * unassigned or pending regions
   */
  public boolean inSafeMode() {
    if (safeMode) {
      if(isInitialMetaScanComplete() && regionsInTransition.size() == 0) {
        safeMode = false;
        LOG.info("exiting safe mode");
      } else {
        LOG.info("in safe mode");
      }
    }
    return safeMode;
  }
  
  /** 
   * Get the root region location.
   * @return HServerAddress describing root region server.
   */
  public HServerAddress getRootRegionLocation() {
    return rootRegionLocation.get();
  }
  
  /**
   * Block until either the root region location is available or we're shutting
   * down.
   */
  public void waitForRootRegionLocation() {
    synchronized (rootRegionLocation) {
      while (!master.closed.get() && rootRegionLocation.get() == null) {
        // rootRegionLocation will be filled in when we get an 'open region'
        // regionServerReport message from the HRegionServer that has been
        // allocated the ROOT region below.
        try {
          rootRegionLocation.wait();
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
  }
  
  /**
   * Return the number of meta regions.
   * @return number of meta regions
   */
  public int numMetaRegions() {
    return numberOfMetaRegions.get();
  }
  
  /**
   * Bump the count of meta regions up one
   */
  public void incrementNumMetaRegions() {
    numberOfMetaRegions.incrementAndGet();
  }

  /**
   * Set the root region location.
   * @param address Address of the region server where the root lives
   */
  public void setRootRegionLocation(HServerAddress address) {
    synchronized (rootRegionLocation) {
      rootRegionLocation.set(new HServerAddress(address));
      rootRegionLocation.notifyAll();
    } 
  }
  
  /**
   * Set the number of meta regions.
   * @param num Number of meta regions
   */
  public void setNumMetaRegions(int num) {
    numberOfMetaRegions.set(num);
  }

  /**
   * @param regionName
   * @param info
   * @param server
   * @param op
   */
  public void startAction(byte[] regionName, HRegionInfo info,
      HServerAddress server, int op) {
    switch (op) {
      case HConstants.MODIFY_TABLE_SPLIT:
        regionsToSplit.put(regionName, 
          new Pair<HRegionInfo,HServerAddress>(info, server));
        break;
      case HConstants.MODIFY_TABLE_COMPACT:
        regionsToCompact.put(regionName,
          new Pair<HRegionInfo,HServerAddress>(info, server));
        break;
      default:
        throw new IllegalArgumentException("illegal table action " + op);
    }
  }

  /**
   * @param regionName
   * @param op
   */
  public void endAction(byte[] regionName, int op) {
    switch (op) {
    case HConstants.MODIFY_TABLE_SPLIT:
      regionsToSplit.remove(regionName);
      break;
    case HConstants.MODIFY_TABLE_COMPACT:
      regionsToCompact.remove(regionName);
      break;
    default:
      throw new IllegalArgumentException("illegal table action " + op);
    }
  }

  /**
   * @param regionName
   */
  public void endActions(byte[] regionName) {
    regionsToSplit.remove(regionName);
    regionsToCompact.remove(regionName);
  }

  /**
   * Send messages to the given region server asking it to split any
   * regions in 'regionsToSplit'
   * @param serverInfo
   * @param returnMsgs
   */
  public void applyActions(HServerInfo serverInfo, ArrayList<HMsg> returnMsgs) {
    HServerAddress addr = serverInfo.getServerAddress();
    Iterator<Pair<HRegionInfo, HServerAddress>> i =
      regionsToCompact.values().iterator();
    synchronized (regionsToCompact) {
      while (i.hasNext()) {
        Pair<HRegionInfo,HServerAddress> pair = i.next();
        if (addr.equals(pair.getSecond())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("sending MSG_REGION_COMPACT " + pair.getFirst() + " to " +
                addr);
          }
          returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_COMPACT, pair.getFirst()));
          i.remove();
        }
      }
    }
    i = regionsToSplit.values().iterator();
    synchronized (regionsToSplit) {
      while (i.hasNext()) {
        Pair<HRegionInfo,HServerAddress> pair = i.next();
        if (addr.equals(pair.getSecond())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("sending MSG_REGION_SPLIT " + pair.getFirst() + " to " +
                addr);
          }
          returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_SPLIT, pair.getFirst()));
          i.remove();
        }
      }
    }
  }
  
  private static class RegionState implements Comparable<RegionState> {
    private final byte[] regionName;
    private HRegionInfo regionInfo = null;
    private boolean unassigned = false;
    private boolean assigned = false;
    private boolean pending = false;
    private boolean closing = false;
    private boolean closed = false;
    private boolean offlined = false;
    private String serverName = null;
    
    RegionState(byte[] regionName) {
      this.regionName = regionName;
    }
    
    RegionState(HRegionInfo info) {
      this.regionName = info.getRegionName();
      this.regionInfo = info;
    }
    
    byte[] getRegionName() {
      return regionName;
    }

    synchronized HRegionInfo getRegionInfo() {
      return this.regionInfo;
    }
    
    synchronized String getServerName() {
      return this.serverName;
    }
    
    synchronized boolean isUnassigned() {
      return unassigned;
    }

    /*
     * Note: callers of this method (reassignRootRegion, 
     * regionsAwaitingAssignment, setUnassigned) ensure that this method is not
     * called unless it is safe to do so.
     */
    synchronized void setUnassigned() {
      this.unassigned = true;
      this.assigned = false;
      this.pending = false;
      this.closing = false;
      this.serverName = null;
    }

    synchronized boolean isAssigned() {
      return assigned;
    }

    synchronized void setAssigned(String serverName) {
      if (!this.unassigned) {
        throw new IllegalStateException(
            "Cannot assign a region that is not currently unassigned. Region: " +
            Bytes.toString(regionName));
      }
      this.unassigned = false;
      this.assigned = true;
      this.pending = false;
      this.closing = false;
      this.serverName = serverName;
    }

    synchronized boolean isPending() {
      return pending;
    }

    synchronized void setPending() {
      if (!assigned) {
        throw new IllegalStateException(
            "Cannot set a region as pending if it has not been assigned. Region: " +
            Bytes.toString(regionName));
      }
      this.unassigned = false;
      this.assigned = false;
      this.pending = true;
      this.closing = false;
    }

    synchronized boolean isClosing() {
      return closing;
    }

    synchronized void setClosing(String serverName, boolean setOffline) {
      this.unassigned = false;
      this.assigned = false;
      this.pending = false;
      this.closing = true;
      this.offlined = setOffline;
      this.serverName = serverName;
    }
    
    synchronized boolean isClosed() {
      return this.closed;
    }
    
    synchronized void setClosed() {
      if (!closing) {
        throw new IllegalStateException(
            "Cannot set a region to be closed if it was not already marked as" +
            " closing. Region: " + Bytes.toString(regionName));
      }
      this.closed = true;
    }
    
    synchronized boolean isOfflined() {
      return this.offlined;
    }

    @Override
    public synchronized String toString() {
      return "region name: " + Bytes.toString(this.regionName) +
          ", isUnassigned: " + this.unassigned + ", isAssigned: " +
          this.assigned + ", isPending: " + this.pending + ", isClosing: " +
          this.closing + ", isClosed: " + this.closed + ", isOfflined: " +
          this.offlined;
    }
    
    @Override
    public boolean equals(Object o) {
      return this.compareTo((RegionState) o) == 0;
    }
    
    @Override
    public int hashCode() {
      return Bytes.toString(regionName).hashCode();
    }
    
    @Override
    public int compareTo(RegionState o) {
      if (o == null) {
        return 1;
      }
      return Bytes.compareTo(this.regionName, o.getRegionName());
    }
  }
}
