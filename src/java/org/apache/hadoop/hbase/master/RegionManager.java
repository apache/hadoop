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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Class to manage assigning regions to servers, state of root and meta, etc.
 */ 
class RegionManager implements HConstants {
  protected static final Log LOG = LogFactory.getLog(RegionManager.class.getName());
  
  private volatile AtomicReference<HServerAddress> rootRegionLocation =
    new AtomicReference<HServerAddress>(null);
  
  final Lock splitLogLock = new ReentrantLock();
  
  private final RootScanner rootScannerThread;
  final MetaScanner metaScannerThread;
  
  /** Set by root scanner to indicate the number of meta regions */
  private final AtomicInteger numberOfMetaRegions = new AtomicInteger();

  /** These are the online meta regions */
  private final SortedMap<Text, MetaRegion> onlineMetaRegions =
    Collections.synchronizedSortedMap(new TreeMap<Text, MetaRegion>());

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
   */
  private final SortedMap<HRegionInfo, Long> unassignedRegions =
    Collections.synchronizedSortedMap(new TreeMap<HRegionInfo, Long>());

  /**
   * Regions that have been assigned, and the server has reported that it has
   * started serving it, but that we have not yet recorded in the meta table.
   */
  private final Set<Text> pendingRegions =
    Collections.synchronizedSet(new HashSet<Text>());

  /**
   * List of regions that are going to be closed.
   */
  private final Map<String, Map<Text, HRegionInfo>> regionsToClose =
    new ConcurrentHashMap<String, Map<Text, HRegionInfo>>();

  /** Regions that are in the process of being closed */
  private final Set<Text> closingRegions =
    Collections.synchronizedSet(new HashSet<Text>());

  /** Regions that are being reassigned for load balancing. */
  private final Set<Text> regionsBeingReassigned = 
    Collections.synchronizedSet(new HashSet<Text>());

  /**
   * 'regionsToDelete' contains regions that need to be deleted, but cannot be
   * until the region server closes it
   */
  private final Set<Text> regionsToDelete =
    Collections.synchronizedSet(new HashSet<Text>());
  
  private HMaster master;  
  
  RegionManager(HMaster master) {
    this.master = master;
    
    // The root region
    rootScannerThread = new RootScanner(master, this);

    // Scans the meta table
    metaScannerThread = new MetaScanner(master, this);
    
    unassignRootRegion();
  }
  
  void start() {
    Threads.setDaemonThreadRunning(rootScannerThread,
      "RegionManager.rootScanner");
    Threads.setDaemonThreadRunning(metaScannerThread,
      "RegionManager.metaScanner");    
  }
  
  /*
   * Unassign the root region.
   * This method would be used in case where root region server had died
   * without reporting in.  Currently, we just flounder and never recover.  We
   * could 'notice' dead region server in root scanner -- if we failed access
   * multiple times -- but reassigning root is catastrophic.
   * 
   */
  void unassignRootRegion() {
    rootRegionLocation.set(null);
    if (!master.shutdownRequested) {
      unassignedRegions.put(HRegionInfo.rootRegionInfo, ZERO_L);
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
    ArrayList<HMsg> returnMsgs) {
    
    synchronized (unassignedRegions) {
      
      // We need to hold a lock on assign attempts while we figure out what to
      // do so that multiple threads do not execute this method in parallel
      // resulting in assigning the same region to multiple servers.
      
      long now = System.currentTimeMillis();
      Set<HRegionInfo> regionsToAssign = new HashSet<HRegionInfo>();
      for (Map.Entry<HRegionInfo, Long> e: unassignedRegions.entrySet()) {
      HRegionInfo i = e.getKey();
        if (numberOfMetaRegions.get() != onlineMetaRegions.size() &&
            !i.isMetaRegion()) {
          // Can't assign user regions until all meta regions have been assigned
          // and are on-line
          continue;
        }
        long diff = now - e.getValue().longValue();
        if (diff > master.maxRegionOpenTime) {
          regionsToAssign.add(e.getKey());
        }
      }
      int nRegionsToAssign = regionsToAssign.size();
      if (nRegionsToAssign <= 0) {
        // No regions to assign.  Return.
        return;
      }

      if (master.serverManager.numServers() == 1) {
        assignRegionsToOneServer(regionsToAssign, serverName, returnMsgs);
        // Finished.  Return.
        return;
      }

      // Multiple servers in play.
      // We need to allocate regions only to most lightly loaded servers.
      HServerLoad thisServersLoad = info.getLoad();
      int nregions = regionsPerServer(nRegionsToAssign, thisServersLoad);
      nRegionsToAssign -= nregions;
      if (nRegionsToAssign > 0) {
        // We still have more regions to assign. See how many we can assign
        // before this server becomes more heavily loaded than the next
        // most heavily loaded server.
        SortedMap<HServerLoad, Set<String>> heavyServers =
          new TreeMap<HServerLoad, Set<String>>();
        synchronized (master.serverManager.loadToServers) {
          heavyServers.putAll(
            master.serverManager.loadToServers.tailMap(thisServersLoad));
        }
        int nservers = 0;
        HServerLoad heavierLoad = null;
        for (Map.Entry<HServerLoad, Set<String>> e : heavyServers.entrySet()) {
          Set<String> servers = e.getValue();
          nservers += servers.size();
          if (e.getKey().compareTo(thisServersLoad) == 0) {
            // This is the load factor of the server we are considering
            nservers -= 1;
            continue;
          }

          // If we get here, we are at the first load entry that is a
          // heavier load than the server we are considering
          heavierLoad = e.getKey();
          break;
        }

        nregions = 0;
        if (heavierLoad != null) {
          // There is a more heavily loaded server
          for (HServerLoad load =
            new HServerLoad(thisServersLoad.getNumberOfRequests(),
                thisServersLoad.getNumberOfRegions());
          load.compareTo(heavierLoad) <= 0 && nregions < nRegionsToAssign;
          load.setNumberOfRegions(load.getNumberOfRegions() + 1), nregions++) {
            // continue;
          }
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

        now = System.currentTimeMillis();
        for (HRegionInfo regionInfo: regionsToAssign) {
          LOG.info("assigning region " + regionInfo.getRegionName() +
              " to server " + serverName);
          unassignedRegions.put(regionInfo, Long.valueOf(now));
          returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));
          if (--nregions <= 0) {
            break;
          }
        }
      }
    }
  }
  
  /*
   * @param nRegionsToAssign
   * @param thisServersLoad
   * @return How many regions we can assign to more lightly loaded servers
   */
  private int regionsPerServer(final int nRegionsToAssign,
      final HServerLoad thisServersLoad) {
    
    SortedMap<HServerLoad, Set<String>> lightServers =
      new TreeMap<HServerLoad, Set<String>>();
    
    synchronized (master.serverManager.loadToServers) {
      lightServers.putAll(master.serverManager.loadToServers.headMap(thisServersLoad));
    }

    int nRegions = 0;
    for (Map.Entry<HServerLoad, Set<String>> e : lightServers.entrySet()) {
      HServerLoad lightLoad = new HServerLoad(e.getKey().getNumberOfRequests(),
          e.getKey().getNumberOfRegions());
      do {
        lightLoad.setNumberOfRegions(lightLoad.getNumberOfRegions() + 1);
        nRegions += 1;
      } while (lightLoad.compareTo(thisServersLoad) <= 0
          && nRegions < nRegionsToAssign);

      nRegions *= e.getValue().size();
      if (nRegions >= nRegionsToAssign) {
        break;
      }
    }
    return nRegions;
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
      LOG.info("assigning region " + regionInfo.getRegionName() +
          " to the only server " + serverName);
      unassignedRegions.put(regionInfo, Long.valueOf(now));
      returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));
    }
  }
  
  /**
   * @return Read-only map of online regions.
   */
  public Map<Text, MetaRegion> getOnlineMetaRegions() {
    return Collections.unmodifiableSortedMap(onlineMetaRegions);
  }
  
  /*
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
  
  public boolean waitForMetaRegionsOrClose() throws IOException {
    return metaScannerThread.waitForMetaRegionsOrClose();
  }
  
  /**
   * Search our map of online meta regions to find the first meta region that 
   * should contain a pointer to <i>newRegion</i>. 
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
        } else {
          return onlineMetaRegions.get(onlineMetaRegions.headMap(
            newRegion.getTableDesc().getName()).lastKey());
        }
      }
    }
  }
  
  /**
   * Get a set of all the meta regions that contain info about a given table.
   */
  public Set<MetaRegion> getMetaRegionsForTable(Text tableName) {
    Text firstMetaRegion = null;
    Set<MetaRegion> metaRegions = new HashSet<MetaRegion>();
    
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
    return metaRegions;
  }
  
  public void createRegion(HRegionInfo newRegion, HRegionInterface server, 
    Text metaRegionName) 
  throws IOException {
    // 2. Create the HRegion
    HRegion region = 
      HRegion.createHRegion(newRegion, master.rootdir, master.conf);

    // 3. Insert into meta
    HRegionInfo info = region.getRegionInfo();
    Text regionName = region.getRegionName();
    BatchUpdate b = new BatchUpdate(regionName);
    b.put(COL_REGIONINFO, Writables.getBytes(info));
    server.batchUpdate(metaRegionName, b);
    
    // 4. Close the new region to flush it to disk.  Close its log file too.
    region.close();
    region.getLog().closeAndDelete();

    // 5. Get it assigned to a server
    unassignedRegions.put(info, ZERO_L);
  }
  
  /** Set a MetaRegion as online. */
  public void putMetaRegionOnline(MetaRegion metaRegion) {
    onlineMetaRegions.put(metaRegion.getStartKey(), metaRegion);
  }

  /** Get a list of online MetaRegions */
  public List<MetaRegion> getListOfOnlineMetaRegions() {
    List<MetaRegion> regions = new ArrayList<MetaRegion>();
    synchronized(onlineMetaRegions) {
      regions.addAll(onlineMetaRegions.values());
    }
    return regions;
  }
  
  /** count of online meta regions */
  public int numOnlineMetaRegions() {
    return onlineMetaRegions.size();
  }
  
  /** Check if a meta region is online by its name */
  public boolean isMetaRegionOnline(Text startKey) {
    return onlineMetaRegions.containsKey(startKey);
  }
  
  /** Set an online MetaRegion offline - remove it from the map. **/
  public void offlineMetaRegion(Text startKey) {
    onlineMetaRegions.remove(startKey); 
  }
  
  /** Check if a region is unassigned */    
  public boolean isUnassigned(HRegionInfo info) {
    return unassignedRegions.containsKey(info);
  }
  
  /** Check if a region is pending */
  public boolean isPending(Text regionName) {
    return pendingRegions.contains(regionName);
  }
  
  /** Set a region to unassigned */
  public void setUnassigned(HRegionInfo info) {
    synchronized(this.unassignedRegions) {
      if (!this.unassignedRegions.containsKey(info) &&
          !this.pendingRegions.contains(info.getRegionName())) {
        this.unassignedRegions.put(info, ZERO_L);
      }
    }
  }
  
  /** Set a region to pending assignment */
  public void setPending(Text regionName) {
    pendingRegions.add(regionName);
  }
  
  /** Unset region's pending status */
  public void noLongerPending(Text regionName) {
    pendingRegions.remove(regionName);
  }
  
  /** Update the deadline for a region assignment to be completed */
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
  
  /** Unset a region's unassigned status */
  public void noLongerUnassigned(HRegionInfo info) {
    unassignedRegions.remove(info);
  }
  
  /** Mark a region to be closed */
  public void markToClose(String serverName, HRegionInfo info) {
    synchronized (regionsToClose) {
      Map<Text, HRegionInfo> serverToClose = regionsToClose.get(serverName);
      if (serverToClose != null) {
        serverToClose.put(info.getRegionName(), info);
      }
    }
  }
  
  /** Mark a bunch of regions as closed not reopen at once for a server */
  public void markToCloseBulk(String serverName, 
    Map<Text, HRegionInfo> map) {
    regionsToClose.put(serverName, map);
  }
  
  /** 
   * Get a map of region names to region infos waiting to be offlined for a 
   * given server 
   */
  public Map<Text, HRegionInfo> getMarkedToClose(String serverName) {
    return regionsToClose.get(serverName);
  }
  
  /**
   * Check if a region is marked as closed not reopen.
   */
  public boolean isMarkedToClose(String serverName, Text regionName) {
    synchronized (regionsToClose) {
      Map<Text, HRegionInfo> serverToClose = regionsToClose.get(serverName);
      return (serverToClose != null && serverToClose.containsKey(regionName));
    }
  }
  
  /**
   * Mark a region as no longer waiting to be closed and not reopened. 
   */
  public void noLongerMarkedToClose(String serverName, Text regionName) {
    synchronized (regionsToClose) {
      Map<Text, HRegionInfo> serverToClose = regionsToClose.get(serverName);
      if (serverToClose != null) {
        serverToClose.remove(regionName);
      }
    }
  }
  
  /** Check if a region is closing */
  public boolean isClosing(Text regionName) {
    return closingRegions.contains(regionName);
  }
  
  /** Set a region as no longer closing (closed?) */
  public void noLongerClosing(Text regionName) {
    closingRegions.remove(regionName);
  }
  
  /** mark a region as closing */
  public void setClosing(Text regionName) {
    closingRegions.add(regionName);
  }
  
  /**
   * Add a meta region to the scan queue
   */
  public void addMetaRegionToScan(MetaRegion m) throws InterruptedException {
    metaScannerThread.addMetaRegionToScan(m);
  }
  
  /** Mark a region as to be deleted */
  public void markRegionForDeletion(Text regionName) {
    regionsToDelete.add(regionName);
  }
  
  /** Note that a region to delete has been deleted */
  public void regionDeleted(Text regionName) {
    regionsToDelete.remove(regionName);
  }
  
  /** Check if a region is marked for deletion */
  public boolean isMarkedForDeletion(Text regionName) {
    return regionsToDelete.contains(regionName);
  }
  
  public boolean isInitialRootScanComplete() {
    return rootScannerThread.isInitialScanComplete();
  }
  
  public boolean isInitialMetaScanComplete() {
    return metaScannerThread.isInitialScanComplete();
  }
  
  public HServerAddress getRootRegionLocation() {
    return rootRegionLocation.get();
  }
  
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
  
  public int numMetaRegions() {
    return numberOfMetaRegions.get();
  }
  
  public void incrementNumMetaRegions() {
    numberOfMetaRegions.incrementAndGet();
  }
  
  public void setRootRegionLocation(HServerAddress address) {
    synchronized (rootRegionLocation) {
      rootRegionLocation.set(new HServerAddress(address));
      rootRegionLocation.notifyAll();
    } 
  }
  
  public void setNumMetaRegions(int num) {
    numberOfMetaRegions.set(num);
  }
}
