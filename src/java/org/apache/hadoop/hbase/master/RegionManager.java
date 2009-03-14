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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

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

  /**
   * Map of region name to RegionState for regions that are in transition such as
   * 
   * unassigned -> pendingOpen -> open
   * closing -> pendingClose -> closed; if (closed && !offline) -> unassigned
   * 
   * At the end of a transition, removeRegion is used to remove the region from
   * the map (since it is no longer in transition)
   * 
   * Note: Needs to be SortedMap so we can specify a comparator
   * 
   * @see RegionState inner-class below
   */
  private final SortedMap<String, RegionState> regionsInTransition =
    Collections.synchronizedSortedMap(new TreeMap<String, RegionState>());

  // How many regions to assign a server at a time.
  private final int maxAssignInOneGo;

  private final HMaster master;
  private final RegionHistorian historian;
  private final float slop;

  /** Set of regions to split. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToSplit = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to compact. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToCompact = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to major compact. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToMajorCompact = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));
  /** Set of regions to flush. */
  private final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>>
    regionsToFlush = Collections.synchronizedSortedMap(
        new TreeMap<byte[],Pair<HRegionInfo,HServerAddress>>
        (Bytes.BYTES_COMPARATOR));

  private final ZooKeeperWrapper zooKeeperWrapper;
  private final int zooKeeperNumRetries;
  private final int zooKeeperPause;

  RegionManager(HMaster master) {
    HBaseConfiguration conf = master.getConfiguration();

    this.master = master;
    this.historian = RegionHistorian.getInstance();
    this.maxAssignInOneGo = conf.getInt("hbase.regions.percheckin", 10);
    this.slop = conf.getFloat("hbase.regions.slop", (float)0.1);

    // The root region
    rootScannerThread = new RootScanner(master);

    // Scans the meta table
    metaScannerThread = new MetaScanner(master);

    zooKeeperWrapper = master.getZooKeeperWrapper();
    zooKeeperNumRetries = conf.getInt(ZOOKEEPER_RETRIES, DEFAULT_ZOOKEEPER_RETRIES);
    zooKeeperPause = conf.getInt(ZOOKEEPER_PAUSE, DEFAULT_ZOOKEEPER_PAUSE);

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
      regionsInTransition.remove(
          HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString());
    }
  }
  
  void reassignRootRegion() {
    unsetRootRegion();
    if (!master.shutdownRequested.get()) {
      synchronized (regionsInTransition) {
        RegionState s = new RegionState(HRegionInfo.ROOT_REGIONINFO);
        s.setUnassigned();
        regionsInTransition.put(
            HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString(), s);
      }
    }
  }
  
  /*
   * Assigns regions to region servers attempting to balance the load across
   * all region servers. Note that no synchronization is necessary as the caller 
   * (ServerManager.processMsgs) already owns the monitor for the RegionManager.
   * 
   * @param info
   * @param mostLoadedRegions
   * @param returnMsgs
   */
  void assignRegions(HServerInfo info, HRegionInfo[] mostLoadedRegions,
      ArrayList<HMsg> returnMsgs) {
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
            LOG.debug("Server " + info.getServerName() +
                " is overloaded. Server load: " + 
                thisServersLoad.getNumberOfRegions() + " avg: " + avgLoad +
                ", slop: " + this.slop);
          }
          unassignSomeRegions(info, thisServersLoad,
              avgLoad, mostLoadedRegions, returnMsgs);
        }
      }
    } else {
      // if there's only one server, just give it all the regions
      if (master.serverManager.numServers() == 1) {
        assignRegionsToOneServer(regionsToAssign, info, returnMsgs);
      } else {
        // otherwise, give this server a few regions taking into account the 
        // load of all the other servers.
        assignRegionsToMultipleServers(thisServersLoad, regionsToAssign, 
            info, returnMsgs);
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
    final Set<RegionState> regionsToAssign, final HServerInfo info, 
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
          " to server " + info.getServerName());
        s.setPendingOpen(info.getServerName());
        this.historian.addRegionAssignment(s.getRegionInfo(),
            info.getServerName());
        returnMsgs.add(
            new HMsg(HMsg.Type.MSG_REGION_OPEN, s.getRegionInfo(), inSafeMode()));
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
   * only caller (assignRegions, whose caller is ServerManager.processMsgs) owns
   * the monitor for RegionManager
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
      if (s.isUnassigned()) {
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
      final HServerInfo info, final ArrayList<HMsg> returnMsgs) {
    for (RegionState s: regionsToAssign) {
      LOG.info("assigning region " + Bytes.toString(s.getRegionName()) +
          " to the only server " + info.getServerName());
      s.setPendingOpen(info.getServerName());
      this.historian.addRegionAssignment(s.getRegionInfo(), info.getServerName());
      returnMsgs.add(
          new HMsg(HMsg.Type.MSG_REGION_OPEN, s.getRegionInfo(), inSafeMode()));
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
  private void unassignSomeRegions(final HServerInfo info, 
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
      String regionName = currentRegion.getRegionNameAsString();
      if (regionIsInTransition(regionName)) {
        skipped++;
        continue;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to close region " + regionName);
      }
      // make a message to close the region
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, currentRegion,
        OVERLOADED, inSafeMode()));
      // mark the region as closing
      setClosing(info.getServerName(), currentRegion, false);
      setPendingClose(regionName);
      // increment the count of regions we've marked
      regionsClosed++;
    }
    LOG.info("Skipped " + skipped + " region(s) that are in transition states");
  }
  
  static class TableDirFilter implements PathFilter {

    public boolean accept(Path path) {
      // skip the region servers' log dirs && version file
      // HBASE-1112 want to sperate the log dirs from table's data dirs by a special character.
      String pathname = path.getName();
      return !pathname.startsWith("log_") && !pathname.equals(VERSION_FILE_NAME);
    }
    
  }
  
  static class RegionDirFilter implements PathFilter {

    public boolean accept(Path path) { 
      return !path.getName().equals(HREGION_COMPACTIONDIR_NAME);
    }
    
  }
  
  /**
   * @return the rough number of the regions on fs
   * Note: this method simply counts the regions on fs by accumulating all the dirs 
   * in each table dir (${HBASE_ROOT}/$TABLE) and skipping logfiles, compaction dirs.
   * @throws IOException 
   */
  public int countRegionsOnFS() throws IOException {
    int regions = 0;
    
    FileStatus[] tableDirs = 
      master.fs.listStatus(master.rootdir, new TableDirFilter());
    
    FileStatus[] regionDirs;
    RegionDirFilter rdf = new RegionDirFilter();
    for(FileStatus tabledir : tableDirs) {
      if(tabledir.isDir()) {
        regionDirs = master.fs.listStatus(tabledir.getPath(), rdf);
        regions += regionDirs.length;
      }
    }
    
    return regions;
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
    zooKeeperWrapper.close();
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
   * Remove a region from the region state map.
   * 
   * @param info
   */
  public void removeRegion(HRegionInfo info) {
    regionsInTransition.remove(info.getRegionNameAsString());
  }
  
  /**
   * @param regionName
   * @return true if the named region is in a transition state
   */
  public boolean regionIsInTransition(String regionName) {
    return regionsInTransition.containsKey(regionName);
  }

  /**
   * @param regionName
   * @return true if the region is unassigned, pendingOpen or open
   */
  public boolean regionIsOpening(String regionName) {
    RegionState state = regionsInTransition.get(regionName);
    if (state != null) {
      return state.isOpening();
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
      RegionState s = regionsInTransition.get(info.getRegionNameAsString());
      if (s == null) {
        s = new RegionState(info);
        regionsInTransition.put(info.getRegionNameAsString(), s);
      }
      if (force || (!s.isPendingOpen() && !s.isOpen())) {
        s.setUnassigned();
      }
    }
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
      RegionState s = regionsInTransition.get(info.getRegionNameAsString());
      if (s != null) {
        return s.isUnassigned();
      }
    }
    return false;
  }
  
  /**
   * Check if a region has been assigned and we're waiting for a response from
   * the region server.
   * 
   * @param regionName name of the region
   * @return true if open, false otherwise
   */
  public boolean isPendingOpen(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isPendingOpen();
      }
    }
    return false;
  }

  /**
   * Region has been assigned to a server and the server has told us it is open
   * @param regionName
   */
  public void setOpen(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setOpen();
      }
    }
  }
  
  /**
   * @param regionName
   * @return true if region is marked to be offlined.
   */
  public boolean isOfflined(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        return s.isOfflined();
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
  public void setClosing(final String serverName, final HRegionInfo regionInfo,
      final boolean setOffline) {
    synchronized (this.regionsInTransition) {
      RegionState s =
        this.regionsInTransition.get(regionInfo.getRegionNameAsString());
      if (s == null) {
        s = new RegionState(regionInfo);
      }
      s.setClosing(serverName, setOffline);
      this.regionsInTransition.put(regionInfo.getRegionNameAsString(), s);
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
        if (s.isClosing() && !s.isPendingClose() && !s.isClosed() &&
            s.getServerName().compareTo(serverName) == 0) {
          result.add(s.getRegionInfo());
        }
      }
    }
    return result;
  }
  
  /**
   * Called when we have told a region server to close the region
   * 
   * @param regionName
   */
  public void setPendingClose(String regionName) {
    synchronized (regionsInTransition) {
      RegionState s = regionsInTransition.get(regionName);
      if (s != null) {
        s.setPendingClose();
      }
    }
  }
  
  /**
   * @param regionName
   */
  public void setClosed(String regionName) {
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

  private boolean tellZooKeeperOutOfSafeMode() {
    for (int attempt = 0; attempt < zooKeeperNumRetries; ++attempt) {
      if (zooKeeperWrapper.writeOutOfSafeMode()) {
        return true;
      }

      sleep(attempt);
    }

    LOG.error("Failed to tell ZooKeeper we're out of safe mode after " +
              zooKeeperNumRetries + " retries");

    return false;
  }

  /** 
   * @return true if the initial meta scan is complete and there are no
   * unassigned or pending regions
   */
  public boolean inSafeMode() {
    if (safeMode) {
      if(isInitialMetaScanComplete() && regionsInTransition.size() == 0 &&
         tellZooKeeperOutOfSafeMode()) {
        master.connection.unsetRootRegionLocation();
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

  private long getPauseTime(int tries) {
    int attempt = tries;
    if (attempt >= RETRY_BACKOFF.length) {
      attempt = RETRY_BACKOFF.length - 1;
    }
    return this.zooKeeperPause * RETRY_BACKOFF[attempt];
  }

  private void sleep(int attempt) {
    try {
      Thread.sleep(getPauseTime(attempt));
    } catch (InterruptedException e) {
      // continue
    }
  }

  private void writeRootRegionLocationToZooKeeper(HServerAddress address) {
    for (int attempt = 0; attempt < zooKeeperNumRetries; ++attempt) {
      if (zooKeeperWrapper.writeRootRegionLocation(address)) {
        return;
      }

      sleep(attempt);
    }

    LOG.error("Failed to write root region location to ZooKeeper after " +
              zooKeeperNumRetries + " retries, shutting down");

    this.master.shutdown();
  }

  /**
   * Set the root region location.
   * @param address Address of the region server where the root lives
   */
  public void setRootRegionLocation(HServerAddress address) {
    writeRootRegionLocationToZooKeeper(address);

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
        startAction(regionName, info, server, this.regionsToSplit);
        break;
      case HConstants.MODIFY_TABLE_COMPACT:
        startAction(regionName, info, server, this.regionsToCompact);
        break;
      case HConstants.MODIFY_TABLE_MAJOR_COMPACT:
        startAction(regionName, info, server, this.regionsToMajorCompact);
        break;
      case HConstants.MODIFY_TABLE_FLUSH:
        startAction(regionName, info, server, this.regionsToFlush);
        break;
      default:
        throw new IllegalArgumentException("illegal table action " + op);
    }
  }

  private void startAction(final byte[] regionName, final HRegionInfo info,
      final HServerAddress server,
      final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>> map) {
    map.put(regionName, new Pair<HRegionInfo,HServerAddress>(info, server));
  }

  /**
   * @param regionName
   * @param op
   */
  public void endAction(byte[] regionName, int op) {
    switch (op) {
    case HConstants.MODIFY_TABLE_SPLIT:
      this.regionsToSplit.remove(regionName);
      break;
    case HConstants.MODIFY_TABLE_COMPACT:
      this.regionsToCompact.remove(regionName);
      break;
    case HConstants.MODIFY_TABLE_MAJOR_COMPACT:
      this.regionsToMajorCompact.remove(regionName);
      break;
    case HConstants.MODIFY_TABLE_FLUSH:
      this.regionsToFlush.remove(regionName);
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
   * regions in 'regionsToSplit', etc.
   * @param serverInfo
   * @param returnMsgs
   */
  public void applyActions(HServerInfo serverInfo, ArrayList<HMsg> returnMsgs) {
    applyActions(serverInfo, returnMsgs, this.regionsToCompact,
        HMsg.Type.MSG_REGION_COMPACT);
    applyActions(serverInfo, returnMsgs, this.regionsToSplit,
      HMsg.Type.MSG_REGION_SPLIT);
    applyActions(serverInfo, returnMsgs, this.regionsToFlush,
        HMsg.Type.MSG_REGION_FLUSH);
    applyActions(serverInfo, returnMsgs, this.regionsToMajorCompact,
        HMsg.Type.MSG_REGION_MAJOR_COMPACT);
  }
  
  private void applyActions(final HServerInfo serverInfo,
      final ArrayList<HMsg> returnMsgs,
      SortedMap<byte[], Pair<HRegionInfo,HServerAddress>> map,
      final HMsg.Type msg) {
    HServerAddress addr = serverInfo.getServerAddress();
    Iterator<Pair<HRegionInfo, HServerAddress>> i = map.values().iterator();
    synchronized (map) {
      while (i.hasNext()) {
        Pair<HRegionInfo,HServerAddress> pair = i.next();
        if (addr.equals(pair.getSecond())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sending " + msg + " " + pair.getFirst() + " to " + addr);
          }
          returnMsgs.add(new HMsg(msg, pair.getFirst(), inSafeMode()));
          i.remove();
        }
      }
    }
  }

  /*
   * State of a Region as it transitions from closed to open, etc.  See
   * note on regionsInTransition data member above for listing of state
   * transitions.
   */
  private static class RegionState implements Comparable<RegionState> {
    private final HRegionInfo regionInfo;
    private volatile boolean unassigned = false;
    private volatile boolean pendingOpen = false;
    private volatile boolean open = false;
    private volatile boolean closing = false;
    private volatile boolean pendingClose = false;
    private volatile boolean closed = false;
    private volatile boolean offlined = false;
    
    /* Set when region is assigned or closing */
    private volatile String serverName = null;

    /* Constructor */
    RegionState(HRegionInfo info) {
      this.regionInfo = info;
    }
    
    synchronized HRegionInfo getRegionInfo() {
      return this.regionInfo;
    }
    
    synchronized byte [] getRegionName() {
      return this.regionInfo.getRegionName();
    }

    /*
     * @return Server this region was assigned to
     */
    synchronized String getServerName() {
      return this.serverName;
    }

    /*
     * @return true if the region is being opened
     */
    synchronized boolean isOpening() {
      return this.unassigned || this.pendingOpen || this.open;
    }

    /*
     * @return true if region is unassigned
     */
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
      this.pendingOpen = false;
      this.open = false;
      this.closing = false;
      this.pendingClose = false;
      this.closed = false;
      this.offlined = false;
      this.serverName = null;
    }

    synchronized boolean isPendingOpen() {
      return pendingOpen;
    }

    /*
     * @param serverName Server region was assigned to.
     */
    synchronized void setPendingOpen(final String serverName) {
      if (!this.unassigned) {
        throw new IllegalStateException(
            "Cannot assign a region that is not currently unassigned. State: " +
            toString());
      }
      this.unassigned = false;
      this.pendingOpen = true;
      this.open = false;
      this.closing = false;
      this.pendingClose = false;
      this.closed = false;
      this.offlined = false;
      this.serverName = serverName;
    }

    synchronized boolean isOpen() {
      return open;
    }

    synchronized void setOpen() {
      if (!pendingOpen) {
        throw new IllegalStateException(
            "Cannot set a region as open if it has not been pending. State: " +
            toString());
      }
      this.unassigned = false;
      this.pendingOpen = false;
      this.open = true;
      this.closing = false;
      this.pendingClose = false;
      this.closed = false;
      this.offlined = false;
    }

    synchronized boolean isClosing() {
      return closing;
    }

    synchronized void setClosing(String serverName, boolean setOffline) {
      this.unassigned = false;
      this.pendingOpen = false;
      this.open = false;
      this.closing = true;
      this.pendingClose = false;
      this.closed = false;
      this.offlined = setOffline;
      this.serverName = serverName;
    }
    
    synchronized boolean isPendingClose() {
      return this.pendingClose;
    }

    synchronized void setPendingClose() {
      if (!closing) {
        throw new IllegalStateException(
            "Cannot set a region as pending close if it has not been closing. " +
            "State: " + toString());
      }
      this.unassigned = false;
      this.pendingOpen = false;
      this.open = false;
      this.closing = false;
      this.pendingClose = true;
      this.closed = false;
    }

    synchronized boolean isClosed() {
      return this.closed;
    }
    
    synchronized void setClosed() {
      if (!pendingClose && !pendingOpen) {
        throw new IllegalStateException(
            "Cannot set a region to be closed if it was not already marked as" +
            " pending close or pending open. State: " + toString());
      }
      this.unassigned = false;
      this.pendingOpen = false;
      this.open = false;
      this.closing = false;
      this.pendingClose = false;
      this.closed = true;
    }
    
    synchronized boolean isOfflined() {
      return this.offlined;
    }

    @Override
    public synchronized String toString() {
      return ("name=" + Bytes.toString(getRegionName()) +
          ", unassigned=" + this.unassigned +
          ", pendingOpen=" + this.pendingOpen +
          ", open=" + this.open +
          ", closing=" + this.closing +
          ", pendingClose=" + this.pendingClose +
          ", closed=" + this.closed +
          ", offlined=" + this.offlined);
    }
    
    @Override
    public boolean equals(Object o) {
      return this.compareTo((RegionState) o) == 0;
    }
    
    @Override
    public int hashCode() {
      return Bytes.toString(getRegionName()).hashCode();
    }
    
    public int compareTo(RegionState o) {
      if (o == null) {
        return 1;
      }
      return Bytes.compareTo(getRegionName(), o.getRegionName());
    }
  }
}
