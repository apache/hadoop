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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;

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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class to manage assigning regions to servers, state of root and meta, etc.
 */
public class RegionManager implements HConstants {
  protected static final Log LOG = LogFactory.getLog(RegionManager.class);

  private AtomicReference<HServerAddress> rootRegionLocation =
    new AtomicReference<HServerAddress>(null);

  private final RootScanner rootScannerThread;
  final MetaScanner metaScannerThread;

  /** Set by root scanner to indicate the number of meta regions */
  private final AtomicInteger numberOfMetaRegions = new AtomicInteger();

  /** These are the online meta regions */
  private final NavigableMap<byte [], MetaRegion> onlineMetaRegions =
    new ConcurrentSkipListMap<byte [], MetaRegion>(Bytes.BYTES_COMPARATOR);

  private static final byte[] OVERLOADED = Bytes.toBytes("Overloaded");

  private static final byte [] META_REGION_PREFIX = Bytes.toBytes(".META.,");

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
   final SortedMap<String, RegionState> regionsInTransition =
    Collections.synchronizedSortedMap(new TreeMap<String, RegionState>());

  // How many regions to assign a server at a time.
  private final int maxAssignInOneGo;

  final HMaster master;
  private final LoadBalancer loadBalancer;

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
  private final int zooKeeperNumRetries;
  private final int zooKeeperPause;

  RegionManager(HMaster master) {
    Configuration conf = master.getConfiguration();

    this.master = master;
    this.maxAssignInOneGo = conf.getInt("hbase.regions.percheckin", 10);
    this.loadBalancer = new LoadBalancer(conf);

    // The root region
    rootScannerThread = new RootScanner(master);

    // Scans the meta table
    metaScannerThread = new MetaScanner(master);

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
      LOG.info("-ROOT- region unset (but not set to be reassigned)");
    }
  }

  void reassignRootRegion() {
    unsetRootRegion();
    if (!master.getShutdownRequested().get()) {
      synchronized (regionsInTransition) {
        RegionState s = new RegionState(HRegionInfo.ROOT_REGIONINFO,
            RegionState.State.UNASSIGNED);
        regionsInTransition.put(
            HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString(), s);
        LOG.info("ROOT inserted into regionsInTransition");
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
    boolean isSingleServer = this.master.numServers() == 1;

    // figure out what regions need to be assigned and aren't currently being
    // worked on elsewhere.
    Set<RegionState> regionsToAssign =
      regionsAwaitingAssignment(info.getServerAddress(), isSingleServer);
    if (regionsToAssign.size() == 0) {
      // There are no regions waiting to be assigned.
      this.loadBalancer.loadBalancing(info, mostLoadedRegions, returnMsgs);
    } else {
      // if there's only one server, just give it all the regions
      if (isSingleServer) {
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
   *
   * TODO: This code is unintelligible.  REWRITE. Add TESTS! St.Ack 09/30/2009
   * @param thisServersLoad
   * @param regionsToAssign
   * @param info
   * @param returnMsgs
   */
  private void assignRegionsToMultipleServers(final HServerLoad thisServersLoad,
    final Set<RegionState> regionsToAssign, final HServerInfo info,
    final ArrayList<HMsg> returnMsgs) {
    boolean isMetaAssign = false;
    for (RegionState s : regionsToAssign) {
      if (s.getRegionInfo().isMetaRegion())
        isMetaAssign = true;
    }
    int nRegionsToAssign = regionsToAssign.size();
    // Now many regions to assign this server.
    int nregions = regionsPerServer(nRegionsToAssign, thisServersLoad);
    LOG.debug("Assigning for " + info + ": total nregions to assign=" +
      nRegionsToAssign + ", nregions to reach balance=" + nregions +
      ", isMetaAssign=" + isMetaAssign);
    if (nRegionsToAssign <= nregions) {
      // I do not know whats supposed to happen in this case.  Assign one.
      LOG.debug("Assigning one region only (playing it safe..)");
      assignRegions(regionsToAssign, 1, info, returnMsgs);
    } else {
      nRegionsToAssign -= nregions;
      if (nRegionsToAssign > 0 || isMetaAssign) {
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
        LOG.debug("Doing for " + info + " nregions: " + nregions +
            " and nRegionsToAssign: " + nRegionsToAssign);
        if (nregions < nRegionsToAssign) {
          // There are some more heavily loaded servers
          // but we can't assign all the regions to this server.
          if (nservers > 0) {
            // There are other servers that can share the load.
            // Split regions that need assignment across the servers.
            nregions = (int) Math.ceil((1.0 * nRegionsToAssign)/(1.0 * nservers));
          } else {
            // No other servers with same load.
            // Split regions over all available servers
            nregions = (int) Math.ceil((1.0 * nRegionsToAssign)/
                (1.0 * this.master.numServers()));
          }
        } else {
          // Assign all regions to this server
          nregions = nRegionsToAssign;
        }
        assignRegions(regionsToAssign, nregions, info, returnMsgs);
      }
    }
  }

  /*
   * Assign <code>nregions</code> regions.
   * @param regionsToAssign
   * @param nregions
   * @param info
   * @param returnMsgs
   */
  private void assignRegions(final Set<RegionState> regionsToAssign,
      final int nregions, final HServerInfo info,
      final ArrayList<HMsg> returnMsgs) {
    int count = nregions;
    if (count > this.maxAssignInOneGo) {
      count = this.maxAssignInOneGo;
    }
    for (RegionState s: regionsToAssign) {
      doRegionAssignment(s, info, returnMsgs);
      if (--count <= 0) {
        break;
      }
    }
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
      doRegionAssignment(s, info, returnMsgs);
    }
  }

  /*
   * Do single region assignment.
   * @param rs
   * @param sinfo
   * @param returnMsgs
   */
  private void doRegionAssignment(final RegionState rs,
      final HServerInfo sinfo, final ArrayList<HMsg> returnMsgs) {
    String regionName = rs.getRegionInfo().getRegionNameAsString();
    LOG.info("Assigning region " + regionName + " to " + sinfo.getServerName());
    rs.setPendingOpen(sinfo.getServerName());
    synchronized (this.regionsInTransition) {
      this.regionsInTransition.put(regionName, rs);
    }

    returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_OPEN, rs.getRegionInfo()));
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
    this.master.getLightServers(thisServersLoad, lightServers);
    // Examine the list of servers that are more lightly loaded than this one.
    // Pretend that we will assign regions to these more lightly loaded servers
    // until they reach load equal with ours. Then, see how many regions are left
    // unassigned. That is how many regions we should assign to this server.
    int nRegions = 0;
    for (Map.Entry<HServerLoad, Set<String>> e: lightServers.entrySet()) {
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
  private Set<RegionState> regionsAwaitingAssignment(HServerAddress addr,
                                                     boolean isSingleServer) {
    // set of regions we want to assign to this server
    Set<RegionState> regionsToAssign = new HashSet<RegionState>();

    boolean isMetaServer = isMetaServer(addr);
    RegionState rootState = null;
    // Handle if root is unassigned... only assign root if root is offline.
    synchronized (this.regionsInTransition) {
      rootState = regionsInTransition.get(HRegionInfo.ROOT_REGIONINFO.getRegionNameAsString());
    }
    if (rootState != null && rootState.isUnassigned()) {
      // make sure root isnt assigned here first.
      // if so return 'empty list'
      // by definition there is no way this could be a ROOT region (since it's
      // unassigned) so just make sure it isn't hosting META regions (unless
      // it's the only server left).
      if (!isMetaServer || isSingleServer) {
        regionsToAssign.add(rootState);
      }
      return regionsToAssign;
    }

    // Look over the set of regions that aren't currently assigned to
    // determine which we should assign to this server.
    boolean reassigningMetas = numberOfMetaRegions.get() != onlineMetaRegions.size();
    boolean isMetaOrRoot = isMetaServer || isRootServer(addr);
    if (reassigningMetas && isMetaOrRoot && !isSingleServer) {
      return regionsToAssign; // dont assign anything to this server.
    }
    synchronized (this.regionsInTransition) {
      for (RegionState s: regionsInTransition.values()) {
        HRegionInfo i = s.getRegionInfo();
        if (i == null) {
          continue;
        }
        if (reassigningMetas &&
            !i.isMetaRegion()) {
          // Can't assign user regions until all meta regions have been assigned
          // and are on-line
          continue;
        }
        if (!i.isMetaRegion() &&
            !master.getServerManager().canAssignUserRegions()) {
          LOG.debug("user region " + i.getRegionNameAsString() +
            " is in transition but not enough servers yet");
          continue;
        }
        if (s.isUnassigned()) {
          regionsToAssign.add(s);
        }
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
    synchronized (master.getLoadToServers()) {
      heavyServers.putAll(
        master.getLoadToServers().tailMap(referenceLoad));
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
   * The server checking in right now is overloaded. We will tell it to close
   * some or all of its most loaded regions, allowing it to reduce its load.
   * The closed regions will then get picked up by other underloaded machines.
   *
   * Note that no synchronization is needed because the only caller
   * (assignRegions) whose caller owns the monitor for RegionManager
   */
  void unassignSomeRegions(final HServerInfo info,
      int numRegionsToClose, final HRegionInfo[] mostLoadedRegions,
      ArrayList<HMsg> returnMsgs) {
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
      final String regionName = currentRegion.getRegionNameAsString();
      if (regionIsInTransition(regionName)) {
        skipped++;
        continue;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to close region " + regionName);
      }
      // make a message to close the region
      returnMsgs.add(new HMsg(HMsg.Type.MSG_REGION_CLOSE, currentRegion,
        OVERLOADED));
      // mark the region as closing
      setClosing(info.getServerName(), currentRegion, false);
      setPendingClose(regionName);
      // increment the count of regions we've marked
      regionsClosed++;
    }
    LOG.info("Skipped " + skipped + " region(s) that are in transition states");
  }

  /*
   * PathFilter that accepts hbase tables only.
   */
  static class TableDirFilter implements PathFilter {
    public boolean accept(Path path) {
      // skip the region servers' log dirs && version file
      // HBASE-1112 want to separate the log dirs from table's data dirs by a
      // special character.
      String pathname = path.getName();
      return !pathname.equals(HLog.HREGION_LOGDIR_NAME) &&
        !pathname.equals(VERSION_FILE_NAME);
    }

  }

  /*
   * PathFilter that accepts all but compaction.dir names.
   */
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
    FileStatus [] tableDirs =
      this.master.getFileSystem().listStatus(this.master.getRootDir(), new TableDirFilter());
    FileStatus[] regionDirs;
    RegionDirFilter rdf = new RegionDirFilter();
    for(FileStatus tabledir : tableDirs) {
      if(tabledir.isDir()) {
        regionDirs = this.master.getFileSystem().listStatus(tabledir.getPath(), rdf);
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

  public boolean metaRegionsInTransition() {
    synchronized (onlineMetaRegions) {
      for (MetaRegion metaRegion : onlineMetaRegions.values()) {
        String regionName = Bytes.toString(metaRegion.getRegionName());
        if (regionIsInTransition(regionName)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Return a map of the regions in transition on a server.
   * Returned map entries are region name -> RegionState
   */
  Map<String, RegionState> getRegionsInTransitionOnServer(String serverName) {
    Map<String, RegionState> ret = new HashMap<String, RegionState>();
    synchronized (regionsInTransition) {
      for (Map.Entry<String, RegionState> entry : regionsInTransition.entrySet()) {
        RegionState rs = entry.getValue();
        if (serverName.equals(rs.getServerName())) {
          ret.put(entry.getKey(), rs);
        }
      }
    }
    return ret;
  }

  /**
   * Stop the root and meta scanners so that the region servers serving meta
   * regions can shut down.
   */
  public void stopScanners() {
    this.rootScannerThread.interruptAndStop();
    this.metaScannerThread.interruptAndStop();
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
    master.getZooKeeperWrapper().clearRSDirectory();
    master.getZooKeeperWrapper().close();
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
            newRegion.getRegionName()).lastKey());
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
          HRegionInfo.ROOT_REGIONINFO));
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
   * @return MetaRegion for passed row.
   * @throws NotAllMetaRegionsOnlineException
   */
  public MetaRegion getMetaRegionForRow(final byte [] row)
  throws NotAllMetaRegionsOnlineException {
    if (!areAllMetaRegionsOnline()) {
      throw new NotAllMetaRegionsOnlineException();
    }
    // Row might be in -ROOT- table.  If so, return -ROOT- region.
    int prefixlen = META_REGION_PREFIX.length;
    if (row.length > prefixlen &&
     Bytes.compareTo(META_REGION_PREFIX, 0, prefixlen, row, 0, prefixlen) == 0) {
    	return new MetaRegion(this.master.getRegionManager().getRootRegionLocation(),
    	  HRegionInfo.ROOT_REGIONINFO);
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
    HRegion region = HRegion.createHRegion(newRegion, this.master.getRootDir(),
      master.getConfiguration());

    // 3. Insert into meta
    HRegionInfo info = region.getRegionInfo();
    byte [] regionName = region.getRegionName();

    Put put = new Put(regionName);
    put.add(CATALOG_FAMILY, REGIONINFO_QUALIFIER, Writables.getBytes(info));
    server.put(metaRegionName, put);

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
    List<MetaRegion> regions;
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
   * @param startKey Startkey to use finding region to remove.
   * @return the MetaRegion that was taken offline.
   */
  public MetaRegion offlineMetaRegionWithStartKey(byte [] startKey) {
    LOG.info("META region whose startkey is " + Bytes.toString(startKey) +
      " removed from onlineMetaRegions");
    return onlineMetaRegions.remove(startKey);
  }

  public boolean isRootServer(HServerAddress server) {
    return this.master.getRegionManager().getRootRegionLocation() != null &&
      server.equals(master.getRegionManager().getRootRegionLocation());
  }

  /**
   * Returns the list of byte[] start-keys for any .META. regions hosted
   * on the indicated server.
   *
   * @param server server address
   * @return list of meta region start-keys.
   */
  public List<byte[]> listMetaRegionsForServer(HServerAddress server) {
    List<byte[]> metas = new ArrayList<byte[]>();
    for ( MetaRegion region : onlineMetaRegions.values() ) {
      if (server.equals(region.getServer())) {
        metas.add(region.getStartKey());
      }
    }
    return metas;
  }

  /**
   * Does this server have any META regions open on it, or any meta
   * regions being assigned to it?
   *
   * @param server Server IP:port
   * @return true if server has meta region assigned
   */
  public boolean isMetaServer(HServerAddress server) {
    for ( MetaRegion region : onlineMetaRegions.values() ) {
      if (server.equals(region.getServer())) {
        return true;
      }
    }

    // This might be expensive, but we need to make sure we dont
    // get double assignment to the same regionserver.
    synchronized(regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (s.getRegionInfo().isMetaRegion()
            && !s.isUnassigned()
            && s.getServerName() != null
            && s.getServerName().equals(server.toString())) {
          // TODO this code appears to be entirely broken, since
          // server.toString() has no start code, but s.getServerName()
          // does!
          LOG.fatal("I DONT BELIEVE YOU WILL EVER SEE THIS!");
          // Has an outstanding meta region to be assigned.
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Is this server assigned to transition the ROOT table. HBASE-1928
   *
   * @param server Server
   * @return true if server is transitioning the ROOT table
   */
  public boolean isRootInTransitionOnThisServer(final String server) {
    synchronized (this.regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (s.getRegionInfo().isRootRegion()
            && !s.isUnassigned()
            && s.getServerName() != null
            && s.getServerName().equals(server)) {
          // Has an outstanding root region to be assigned.
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Is this server assigned to transition a META table. HBASE-1928
   *
   * @param server Server
   * @return if this server was transitioning a META table then a not null HRegionInfo pointing to it
   */
  public HRegionInfo getMetaServerRegionInfo(final String server) {
    synchronized (this.regionsInTransition) {
      for (RegionState s : regionsInTransition.values()) {
        if (s.getRegionInfo().isMetaRegion()
            && !s.isUnassigned()
            && s.getServerName() != null
            && s.getServerName().equals(server)) {
          // Has an outstanding meta region to be assigned.
          return s.getRegionInfo();
        }
      }
    }
    return null;
  }

  /**
   * Call to take this metaserver offline for immediate reassignment.  Used only
   * when we know a region has shut down cleanly.
   *
   * A meta server is a server that hosts either -ROOT- or any .META. regions.
   *
   * If you are considering a unclean shutdown potentially, use ProcessServerShutdown which
   * calls other methods to immediately unassign root/meta but delay the reassign until the
   * log has been split.
   *
   * @param server the server that went down
   * @return true if this was in fact a meta server, false if it did not carry meta regions.
   */
  public synchronized boolean offlineMetaServer(HServerAddress server) {
    boolean hasMeta = false;

    // check to see if ROOT and/or .META. are on this server, reassign them.
    // use master.getRootRegionLocation.
    if (master.getRegionManager().getRootRegionLocation() != null &&
        server.equals(master.getRegionManager().getRootRegionLocation())) {
      LOG.info("Offlined ROOT server: " + server);
      reassignRootRegion();
      hasMeta = true;
    }
    // AND
    for ( MetaRegion region : onlineMetaRegions.values() ) {
      if (server.equals(region.getServer())) {
        LOG.info("Offlining META region: " + region);
        offlineMetaRegionWithStartKey(region.getStartKey());
        // Set for reassignment.
        setUnassigned(region.getRegionInfo(), true);
        hasMeta = true;
      }
    }
    return hasMeta;
  }

  /**
   * Remove a region from the region state map.
   *
   * @param info
   */
  public void removeRegion(HRegionInfo info) {
    synchronized (this.regionsInTransition) {
      this.regionsInTransition.remove(info.getRegionNameAsString());
    }
  }

  /**
   * @param regionName
   * @return true if the named region is in a transition state
   */
  public boolean regionIsInTransition(String regionName) {
    synchronized (this.regionsInTransition) {
      return regionsInTransition.containsKey(regionName);
    }
  }

  /**
   * @param regionName
   * @return true if the region is unassigned, pendingOpen or open
   */
  public boolean regionIsOpening(String regionName) {
    synchronized (this.regionsInTransition) {
      RegionState state = regionsInTransition.get(regionName);
      if (state != null) {
        return state.isOpening();
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
    RegionState s = null;
    synchronized(this.regionsInTransition) {
      s = regionsInTransition.get(info.getRegionNameAsString());
      if (s == null) {
        s = new RegionState(info, RegionState.State.UNASSIGNED);
        regionsInTransition.put(info.getRegionNameAsString(), s);
      }
    }
    if (force || (!s.isPendingOpen() && !s.isOpen())) {
      s.setUnassigned();
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
  public void setClosing(String serverName, final HRegionInfo regionInfo,
      final boolean setOffline) {
    synchronized (this.regionsInTransition) {
      RegionState s =
        this.regionsInTransition.get(regionInfo.getRegionNameAsString());
      if (s == null) {
        s = new RegionState(regionInfo, RegionState.State.CLOSING);
      }
      // If region was asked to open before getting here, we could be taking
      // the wrong server name
      if(s.isPendingOpen()) {
        serverName = s.getServerName();
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
      while (!master.getShutdownRequested().get() &&
          !master.isClosed() && rootRegionLocation.get() == null) {
        // rootRegionLocation will be filled in when we get an 'open region'
        // regionServerReport message from the HRegionServer that has been
        // allocated the ROOT region below.
        try {
          // Cycle rather than hold here in case master is closed meantime.
          rootRegionLocation.wait(this.master.getThreadWakeFrequency());
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
      if (master.getZooKeeperWrapper().writeRootRegionLocation(address)) {
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
      HServerAddress server, HConstants.Modify op) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding operation " + op + " from tasklist");
    }
    switch (op) {
      case TABLE_SPLIT:
        startAction(regionName, info, server, this.regionsToSplit);
        break;
      case TABLE_COMPACT:
        startAction(regionName, info, server, this.regionsToCompact);
        break;
      case TABLE_MAJOR_COMPACT:
        startAction(regionName, info, server, this.regionsToMajorCompact);
        break;
      case TABLE_FLUSH:
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
  public void endAction(byte[] regionName, HConstants.Modify op) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing operation " + op + " from tasklist");
    }
    switch (op) {
    case TABLE_SPLIT:
      this.regionsToSplit.remove(regionName);
      break;
    case TABLE_COMPACT:
      this.regionsToCompact.remove(regionName);
      break;
    case TABLE_MAJOR_COMPACT:
      this.regionsToMajorCompact.remove(regionName);
      break;
    case TABLE_FLUSH:
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
      final SortedMap<byte[], Pair<HRegionInfo,HServerAddress>> map,
      final HMsg.Type msg) {
    HServerAddress addr = serverInfo.getServerAddress();
    synchronized (map) {
      Iterator<Pair<HRegionInfo, HServerAddress>> i = map.values().iterator();
      while (i.hasNext()) {
        Pair<HRegionInfo,HServerAddress> pair = i.next();
        if (addr.equals(pair.getSecond())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Sending " + msg + " " + pair.getFirst() + " to " + addr);
          }
          returnMsgs.add(new HMsg(msg, pair.getFirst()));
          i.remove();
        }
      }
    }
  }

  /**
   * Class to balance region servers load.
   * It keeps Region Servers load in slop range by unassigning Regions
   * from most loaded servers.
   *
   * Equilibrium is reached when load of all serves are in slop range
   * [avgLoadMinusSlop, avgLoadPlusSlop], where
   *  avgLoadPlusSlop = Math.ceil(avgLoad * (1 + this.slop)), and
   *  avgLoadMinusSlop = Math.floor(avgLoad * (1 - this.slop)) - 1.
   */
  private class LoadBalancer {
    private float slop;                 // hbase.regions.slop
    private final int maxRegToClose;    // hbase.regions.close.max

    LoadBalancer(Configuration conf) {
      this.slop = conf.getFloat("hbase.regions.slop", (float)0.3);
      if (this.slop <= 0) this.slop = 1;
      //maxRegToClose to constrain balance closing per one iteration
      // -1 to turn off
      // TODO: change default in HBASE-862, need a suggestion
      this.maxRegToClose = conf.getInt("hbase.regions.close.max", -1);
    }

    /**
     * Balance server load by unassigning some regions.
     *
     * @param info - server info
     * @param mostLoadedRegions - array of most loaded regions
     * @param returnMsgs - array of return massages
     */
    void loadBalancing(HServerInfo info, HRegionInfo[] mostLoadedRegions,
        ArrayList<HMsg> returnMsgs) {
      HServerLoad servLoad = info.getLoad();
      double avg = master.getAverageLoad();

      // nothing to balance if server load not more then average load
      if(servLoad.getLoad() <= Math.ceil(avg) || avg <= 2.0) {
        return;
      }

      // check if current server is overloaded
      int numRegionsToClose = balanceFromOverloaded(info, servLoad, avg);

      // check if we can unload server by low loaded servers
      if(numRegionsToClose <= 0) {
        numRegionsToClose = balanceToLowloaded(info.getServerName(), servLoad,
            avg);
      }

      if(maxRegToClose > 0) {
        numRegionsToClose = Math.min(numRegionsToClose, maxRegToClose);
      }

      if(numRegionsToClose > 0) {
        unassignSomeRegions(info, numRegionsToClose, mostLoadedRegions,
            returnMsgs);
      }
    }

    /*
     * Check if server load is not overloaded (with load > avgLoadPlusSlop).
     * @return number of regions to unassign.
     */
    private int balanceFromOverloaded(final HServerInfo info,
        final HServerLoad srvLoad, final double avgLoad) {
      int avgLoadPlusSlop = (int)Math.ceil(avgLoad * (1 + this.slop));
      int numSrvRegs = srvLoad.getNumberOfRegions();
      if (numSrvRegs > avgLoadPlusSlop) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Server " + info.getServerName() + " is overloaded: load=" +
            numSrvRegs + ", avg=" + avgLoad + ", slop=" + this.slop);
        }
        return numSrvRegs - (int)Math.ceil(avgLoad);
      }
      return 0;
    }

    /*
     * Check if server is most loaded and can be unloaded to
     * low loaded servers (with load < avgLoadMinusSlop).
     * @return number of regions to unassign.
     */
    private int balanceToLowloaded(String srvName, HServerLoad srvLoad,
        double avgLoad) {

      SortedMap<HServerLoad, Set<String>> loadToServers =
        master.getLoadToServers();
      // check if server most loaded
      if (!loadToServers.get(loadToServers.lastKey()).contains(srvName))
        return 0;

      // this server is most loaded, we will try to unload it by lowest
      // loaded servers
      int avgLoadMinusSlop = (int)Math.floor(avgLoad * (1 - this.slop)) - 1;
      int lowestLoad = loadToServers.firstKey().getNumberOfRegions();

      if(lowestLoad >= avgLoadMinusSlop)
        return 0; // there is no low loaded servers

      int lowSrvCount = loadToServers.get(loadToServers.firstKey()).size();
      int numRegionsToClose = 0;

      int numSrvRegs = srvLoad.getNumberOfRegions();
      int numMoveToLowLoaded = (avgLoadMinusSlop - lowestLoad) * lowSrvCount;
      numRegionsToClose = numSrvRegs - (int)Math.ceil(avgLoad);
      numRegionsToClose = Math.min(numRegionsToClose, numMoveToLowLoaded);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server " + srvName + " will be unloaded for " +
            "balance. Server load: " + numSrvRegs + " avg: " +
            avgLoad + ", regions can be moved: " + numMoveToLowLoaded +
            ". Regions to close: " + numRegionsToClose);
      }
      return numRegionsToClose;
    }
  }

  /**
   * @return Snapshot of regionsintransition as a sorted Map.
   */
  NavigableMap<String, String> getRegionsInTransition() {
    NavigableMap<String, String> result = new TreeMap<String, String>();
    synchronized (this.regionsInTransition) {
      if (this.regionsInTransition.isEmpty()) return result;
      for (Map.Entry<String, RegionState> e: this.regionsInTransition.entrySet()) {
        result.put(e.getKey(), e.getValue().toString());
      }
    }
    return result;
  }

  /**
   * @param regionname Name to clear from regions in transistion.
   * @return True if we removed an element for the passed regionname.
   */
  boolean clearFromInTransition(final byte [] regionname) {
    boolean result = false;
    synchronized (this.regionsInTransition) {
      if (this.regionsInTransition.isEmpty()) return result;
      for (Map.Entry<String, RegionState> e: this.regionsInTransition.entrySet()) {
        if (Bytes.equals(regionname, e.getValue().getRegionName())) {
          this.regionsInTransition.remove(e.getKey());
          LOG.debug("Removed " + e.getKey() + ", " + e.getValue());
          result = true;
          break;
        }
      }
    }
    return result;
  }

  /*
   * State of a Region as it transitions from closed to open, etc.  See
   * note on regionsInTransition data member above for listing of state
   * transitions.
   */
  static class RegionState implements Comparable<RegionState> {
    private final HRegionInfo regionInfo;

    enum State {
      UNASSIGNED, // awaiting a server to be assigned
      PENDING_OPEN, // told a server to open, hasn't opened yet
      OPEN, // has been opened on RS, but not yet marked in META/ROOT
      CLOSING, // a msg has been enqueued to close ths region, but not delivered to RS yet
      PENDING_CLOSE, // msg has been delivered to RS to close this region
      CLOSED // region has been closed but not yet marked in meta

    }

    private State state;

    private boolean isOfflined;

    /* Set when region is assigned or closing */
    private String serverName = null;

    /* Constructor */
    RegionState(HRegionInfo info, State state) {
      this.regionInfo = info;
      this.state = state;
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
      return state == State.UNASSIGNED ||
        state == State.PENDING_OPEN ||
        state == State.OPEN;
    }

    /*
     * @return true if region is unassigned
     */
    synchronized boolean isUnassigned() {
      return state == State.UNASSIGNED;
    }

    /*
     * Note: callers of this method (reassignRootRegion,
     * regionsAwaitingAssignment, setUnassigned) ensure that this method is not
     * called unless it is safe to do so.
     */
    synchronized void setUnassigned() {
      state = State.UNASSIGNED;
      this.serverName = null;
    }

    synchronized boolean isPendingOpen() {
      return state == State.PENDING_OPEN;
    }

    /*
     * @param serverName Server region was assigned to.
     */
    synchronized void setPendingOpen(final String serverName) {
      if (state != State.UNASSIGNED) {
        LOG.warn("Cannot assign a region that is not currently unassigned. " +
          "FIX!! State: " + toString());
      }
      state = State.PENDING_OPEN;
      this.serverName = serverName;
    }

    synchronized boolean isOpen() {
      return state == State.OPEN;
    }

    synchronized void setOpen() {
      if (state != State.PENDING_OPEN) {
        LOG.warn("Cannot set a region as open if it has not been pending. " +
          "FIX!! State: " + toString());
      }
      state = State.OPEN;
    }

    synchronized boolean isClosing() {
      return state == State.CLOSING;
    }

    synchronized void setClosing(String serverName, boolean setOffline) {
      state = State.CLOSING;
      this.serverName = serverName;
      this.isOfflined = setOffline;
    }

    synchronized boolean isPendingClose() {
      return state == State.PENDING_CLOSE;
    }

    synchronized void setPendingClose() {
      if (state != State.CLOSING) {
        LOG.warn("Cannot set a region as pending close if it has not been " +
          "closing.  FIX!! State: " + toString());
      }
      state = State.PENDING_CLOSE;
    }

    synchronized boolean isClosed() {
      return state == State.CLOSED;
    }

    synchronized void setClosed() {
      if (state != State.PENDING_CLOSE &&
          state != State.PENDING_OPEN &&
          state != State.CLOSING) {
        throw new IllegalStateException(
            "Cannot set a region to be closed if it was not already marked as" +
            " pending close, pending open or closing. State: " + this);
      }
      state = State.CLOSED;
    }

    synchronized boolean isOfflined() {
      return (state == State.CLOSING ||
        state == State.PENDING_CLOSE) && isOfflined;
    }

    @Override
    public synchronized String toString() {
      return ("name=" + Bytes.toString(getRegionName()) +
          ", state=" + this.state);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
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
