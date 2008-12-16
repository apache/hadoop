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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

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
  private final SortedMap<byte [], MetaRegion> onlineMetaRegions =
    Collections.synchronizedSortedMap(new TreeMap<byte [],
      MetaRegion>(Bytes.BYTES_COMPARATOR));

  private static final byte[] OVERLOADED = Bytes.toBytes("Overloaded");

  /**
   * The 'unassignedRegions' table maps from a HRegionInfo to a timestamp that
   * indicates the last time we *tried* to assign the region to a RegionServer.
   * If the timestamp is out of date, then we can try to reassign it. 
   * 
   * We fill 'unassignedRecords' by scanning ROOT and META tables, learning the
   * set of all known valid regions.
   * 
   * <p>Items are removed from this list when a region server reports in that
   * the region has been deployed.
   *
   * TODO: Need to be a sorted map?
   */
  private final SortedMap<HRegionInfo, Long> unassignedRegions =
    Collections.synchronizedSortedMap(new TreeMap<HRegionInfo, Long>());

  /**
   * Regions that have been assigned, and the server has reported that it has
   * started serving it, but that we have not yet recorded in the meta table.
   */
  private final Set<byte []> pendingRegions =
    Collections.synchronizedSet(new TreeSet<byte []>(Bytes.BYTES_COMPARATOR));

  /**
   * List of regions that are going to be closed.
   */
  private final Map<String, Map<byte [], HRegionInfo>> regionsToClose =
    new ConcurrentHashMap<String, Map<byte [], HRegionInfo>>();

  /** Regions that are in the process of being closed */
  private final Set<byte []> closingRegions =
    Collections.synchronizedSet(new TreeSet<byte []>(Bytes.BYTES_COMPARATOR));

  /**
   * Set of regions that, once closed, should be marked as offline so that they
   * are not reassigned.
   */
  private final Set<byte []> regionsToOffline = 
    Collections.synchronizedSet(new TreeSet<byte []>(Bytes.BYTES_COMPARATOR));
  // How many regions to assign a server at a time.
  private final int maxAssignInOneGo;

  private final HMaster master;
  private final RegionHistorian historian;
  private final float slop;

  /** Set of regions to split. */
  private final Map<byte[],Pair<HRegionInfo,HServerAddress>> regionsToSplit = 
    Collections.synchronizedSortedMap(
      new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to compact. */
  private final Map<byte[],Pair<HRegionInfo,HServerAddress>> regionsToCompact =
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
    rootRegionLocation.set(null);
  }
  
  void reassignRootRegion() {
    unsetRootRegion();
    if (!master.shutdownRequested) {
      unassignedRegions.put(HRegionInfo.ROOT_REGIONINFO, ZERO_L);
    }
  }
  
  /*
   * Assigns regions to region servers attempting to balance the load across
   * all region servers
   * 
   * @param info
   * @param serverName
   * @param returnMsgs
   */
  void assignRegions(HServerInfo info, String serverName,
    HRegionInfo[] mostLoadedRegions, ArrayList<HMsg> returnMsgs) {
    HServerLoad thisServersLoad = info.getLoad();
    synchronized (unassignedRegions) {
      // We need to hold a lock on assign attempts while we figure out what to
      // do so that multiple threads do not execute this method in parallel
      // resulting in assigning the same region to multiple servers.
      
      // figure out what regions need to be assigned and aren't currently being
      // worked on elsewhere.
      Set<HRegionInfo> regionsToAssign = regionsAwaitingAssignment();
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
            unassignSomeRegions(thisServersLoad, avgLoad, mostLoadedRegions,
              returnMsgs);
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
  }
  
  /**
   * Make region assignments taking into account multiple servers' loads.
   */ 
  private void assignRegionsToMultipleServers(final HServerLoad thisServersLoad,
    final Set<HRegionInfo> regionsToAssign, final String serverName, 
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
      
      long now = System.currentTimeMillis();
      for (HRegionInfo regionInfo: regionsToAssign) {
        LOG.info("assigning region " +
          Bytes.toString(regionInfo.getRegionName())+
          " to server " + serverName);
        unassignedRegions.put(regionInfo, Long.valueOf(now));
        this.historian.addRegionAssignment(regionInfo, serverName);
        returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, regionInfo));
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
  
  /**
   * Get the set of regions that should be assignable in this pass.
   */ 
  private Set<HRegionInfo> regionsAwaitingAssignment() {
    long now = System.currentTimeMillis();
    
    // set of regions we want to assign to this server
    Set<HRegionInfo> regionsToAssign = new HashSet<HRegionInfo>();
    
    // Look over the set of regions that aren't currently assigned to 
    // determine which we should assign to this server.
    synchronized (unassignedRegions) {          //must synchronize when iterating
      for (Map.Entry<HRegionInfo, Long> e: unassignedRegions.entrySet()) {
        HRegionInfo i = e.getKey();
        if (numberOfMetaRegions.get() != onlineMetaRegions.size() &&
            !i.isMetaRegion()) {
          // Can't assign user regions until all meta regions have been assigned
          // and are on-line
          continue;
        }
        // If the last attempt to open this region was pretty recent, then we 
        // don't want to try and assign it.
        long diff = now - e.getValue().longValue();
        if (diff > master.maxRegionOpenTime) {
          regionsToAssign.add(e.getKey());
        }
      }
    }
    return regionsToAssign;
  }
  
  /**
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
   * @param regionsToAssign
   * @param serverName
   * @param returnMsgs
   */
  private void assignRegionsToOneServer(final Set<HRegionInfo> regionsToAssign,
      final String serverName, final ArrayList<HMsg> returnMsgs) {
    long now = System.currentTimeMillis();
    for (HRegionInfo regionInfo: regionsToAssign) {
      LOG.info("assigning region " +
          Bytes.toString(regionInfo.getRegionName()) +
          " to the only server " + serverName);
      unassignedRegions.put(regionInfo, Long.valueOf(now));
      this.historian.addRegionAssignment(regionInfo, serverName);
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, regionInfo));
    }
  }
  
  /**
   * The server checking in right now is overloaded. We will tell it to close
   * some or all of its most loaded regions, allowing it to reduce its load.
   * The closed regions will then get picked up by other underloaded machines.
   */
  private synchronized void unassignSomeRegions(final HServerLoad load, 
    final double avgLoad, final HRegionInfo[] mostLoadedRegions, 
    ArrayList<HMsg> returnMsgs) {
    
    int numRegionsToClose = load.getNumberOfRegions() - (int)Math.ceil(avgLoad);
    LOG.debug("Choosing to reassign " + numRegionsToClose 
      + " regions. mostLoadedRegions has " + mostLoadedRegions.length 
      + " regions in it.");
    
    int regionIdx = 0;
    int regionsClosed = 0;
    int skippedClosing = 0;
    while (regionsClosed < numRegionsToClose &&
        regionIdx < mostLoadedRegions.length) {
      HRegionInfo currentRegion = mostLoadedRegions[regionIdx];
      regionIdx++;
      // skip the region if it's meta or root
      if (currentRegion.isRootRegion() || currentRegion.isMetaTable()) {
        continue;
      }
      
      if (isClosing(currentRegion.getRegionName())) {
        skippedClosing++;
        continue;
      }
      
      LOG.debug("Going to close region " +
        currentRegion.getRegionNameAsString());
      // make a message to close the region
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, currentRegion,
        OVERLOADED));
      // mark the region as closing
      setClosing(currentRegion.getRegionName());
      // increment the count of regions we've marked
      regionsClosed++;
    }
    LOG.info("Skipped " + skippedClosing + " region(s) as already closing");
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
    boolean result = false;
    if (rootRegionLocation.get() != null &&
        numberOfMetaRegions.get() == onlineMetaRegions.size()) {
      result = true;
    }
    return result;
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
    unassignedRegions.put(info, ZERO_L);
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
    return unassignedRegions.containsKey(info);
  }
  
  /**
   * Check if a region is pending 
   * @param regionName name of the region
   * @return true if pending, false otherwise
   */
  public boolean isPending(byte [] regionName) {
    return pendingRegions.contains(regionName);
  }
  
  /** 
   * Set a region to unassigned 
   * @param info Region to set unassigned
   */
  public void setUnassigned(HRegionInfo info) {
    synchronized(this.unassignedRegions) {
      if (!this.unassignedRegions.containsKey(info) &&
          !this.pendingRegions.contains(info.getRegionName())) {
        this.unassignedRegions.put(info, ZERO_L);
      }
    }
  }
  
  /**
   * Set a region to pending assignment 
   * @param regionName
   */
  public void setPending(byte [] regionName) {
    pendingRegions.add(regionName);
  }
  
  /**
   * Unset region's pending status 
   * @param regionName 
   */
  public void noLongerPending(byte [] regionName) {
    pendingRegions.remove(regionName);
  }
  
  /**
   * Extend the update assignment deadline for a region.
   * @param info Region whose deadline you want to extend
   */
  public void updateAssignmentDeadline(HRegionInfo info) {
    synchronized (unassignedRegions) {
      // Region server is reporting in that its working on region open
      // (We can get more than one of these messages if region is replaying
      // a bunch of edits and taking a while to open).
      // Extend region open time by max region open time.
      this.unassignedRegions.put(info,
        Long.valueOf(System.currentTimeMillis() + this.master.maxRegionOpenTime));
    }
  }
  
  /** 
   * Unset a region's unassigned status 
   * @param info Region you want to take off the unassigned list
   */
  public void noLongerUnassigned(HRegionInfo info) {
    unassignedRegions.remove(info);
  }
  
  /**
   * Mark a region to be closed. Server manager will inform hosting region server
   * to close the region at its next opportunity.
   * @param serverName address info of server
   * @param info region to close
   */
  public void markToClose(String serverName, HRegionInfo info) {
    synchronized (regionsToClose) {
      Map<byte [], HRegionInfo> serverToClose = regionsToClose.get(serverName);
      if (serverToClose != null) {
        serverToClose.put(info.getRegionName(), info);
      }
    }
  }
  
  /**
   * Mark a bunch of regions as to close at once for a server 
   * @param serverName address info of server
   * @param map map of region names to region infos of regions to close
   */
  public void markToCloseBulk(String serverName,
      Map<byte [], HRegionInfo> map) {
    synchronized (regionsToClose) {
      Map<byte [], HRegionInfo> regions = regionsToClose.get(serverName);
      if (regions != null) {
        regions.putAll(map);
      } else {
        regions = map;
      }
      regionsToClose.put(serverName, regions);
    }
  }
  
  /** 
   * Remove the map of region names to region infos waiting to be offlined for a 
   * given server
   *  
   * @param serverName
   * @return map of region names to region infos to close
   */
  public Map<byte [], HRegionInfo> removeMarkedToClose(String serverName) {
    return regionsToClose.remove(serverName);
  }
  
  /**
   * Check if a region is marked as to close
   * @param serverName address info of server
   * @param regionName name of the region we might want to close
   * @return true if the region is marked to close, false otherwise
   */
  public boolean isMarkedToClose(String serverName, byte [] regionName) {
    synchronized (regionsToClose) {
      Map<byte [], HRegionInfo> serverToClose = regionsToClose.get(serverName);
      return (serverToClose != null && serverToClose.containsKey(regionName));
    }
  }
  
  /**
   * Mark a region as no longer waiting to be closed. Either it was closed or 
   * we don't want to close it anymore for some reason.
   * @param serverName address info of server
   * @param regionName name of the region
   */
  public void noLongerMarkedToClose(String serverName, byte [] regionName) {
    synchronized (regionsToClose) {
      Map<byte [], HRegionInfo> serverToClose = regionsToClose.get(serverName);
      if (serverToClose != null) {
        serverToClose.remove(regionName);
      }
    }
  }
  
  /**
   * Called when all regions for a particular server have been closed
   * 
   * @param serverName
   */
  public void allRegionsClosed(String serverName) {
    regionsToClose.remove(serverName);
  }

  /** 
   * Check if a region is closing 
   * @param regionName 
   * @return true if the region is marked as closing, false otherwise
   */
  public boolean isClosing(byte [] regionName) {
    return closingRegions.contains(regionName);
  }

  /** 
   * Set a region as no longer closing (closed?) 
   * @param regionName
   */
  public void noLongerClosing(byte [] regionName) {
    closingRegions.remove(regionName);
  }
  
  /** 
   * Mark a region as closing 
   * @param regionName
   */
  public void setClosing(byte [] regionName) {
    closingRegions.add(regionName);
  }
  
  /**
   * Add a meta region to the scan queue
   * @param m MetaRegion that needs to get scanned
   */
  public void addMetaRegionToScan(MetaRegion m) {
    metaScannerThread.addMetaRegionToScan(m);
  }
  
  /** 
   * Note that a region should be offlined as soon as its closed. 
   * @param regionName
   */
  public void markRegionForOffline(byte [] regionName) {
    regionsToOffline.add(regionName);
  }
  
  /** 
   * Check if a region is marked for offline 
   * @param regionName
   * @return true if marked for offline, false otherwise
   */
  public boolean isMarkedForOffline(byte [] regionName) {
    return regionsToOffline.contains(regionName);
  }
  
  /** 
   * Region was offlined as planned, remove it from the list to offline 
   * @param regionName
   */
  public void regionOfflined(byte [] regionName) {
    regionsToOffline.remove(regionName);
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
      if(isInitialMetaScanComplete() && unassignedRegions.size() == 0 &&
          pendingRegions.size() == 0) {
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
    Iterator<Pair<HRegionInfo,HServerAddress>> i =
      regionsToCompact.values().iterator();
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
    i = regionsToSplit.values().iterator();
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
