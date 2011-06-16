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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * Makes decisions about the placement and movement of Regions across
 * RegionServers.
 *
 * <p>Cluster-wide load balancing will occur only when there are no regions in
 * transition and according to a fixed period of a time using {@link #balanceCluster(Map)}.
 *
 * <p>Inline region placement with {@link #immediateAssignment} can be used when
 * the Master needs to handle closed regions that it currently does not have
 * a destination set for.  This can happen during master failover.
 *
 * <p>On cluster startup, bulk assignment can be used to determine
 * locations for all Regions in a cluster.
 *
 * <p>This classes produces plans for the {@link AssignmentManager} to execute.
 */
public class LoadBalancer {
  private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  // slop for regions
  private float slop;

  LoadBalancer(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
    if (slop < 0) slop = 0;
    else if (slop > 1) slop = 1;
  }
  
  /* 
   * The following comparator assumes that RegionId from HRegionInfo can
   * represent the age of the region - larger RegionId means the region
   * is younger.
   * This comparator is used in balanceCluster() to account for the out-of-band
   * regions which were assigned to the server after some other region server
   * crashed.
   */
   static class RegionInfoComparator implements Comparator<HRegionInfo> {
       @Override
       public int compare(HRegionInfo l, HRegionInfo r) {
          long diff = r.getRegionId() - l.getRegionId();
          if (diff < 0) return -1;
          if (diff > 0) return 1;
          return 0;
       } 
   }
   static RegionInfoComparator riComparator = new RegionInfoComparator();
   
   static class RegionPlanComparator implements Comparator<RegionPlan> {
    @Override
    public int compare(RegionPlan l, RegionPlan r) {
      long diff = r.getRegionInfo().getRegionId() - l.getRegionInfo().getRegionId();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }
  static RegionPlanComparator rpComparator = new RegionPlanComparator();

  /**
   * Data structure that holds servername and 'load'.
   */
  static class ServerAndLoad implements Comparable<ServerAndLoad> {
    private final ServerName sn;
    private final int load;
    ServerAndLoad(final ServerName sn, final int load) {
      this.sn = sn;
      this.load = load;
    }

    ServerName getServerName() {return this.sn;}
    int getLoad() {return this.load;}

    @Override
    public int compareTo(ServerAndLoad other) {
      int diff = this.load - other.load;
      return diff != 0? diff: this.sn.compareTo(other.getServerName());
    }
  }

  /**
   * Generate a global load balancing plan according to the specified map of
   * server information to the most loaded regions of each server.
   *
   * The load balancing invariant is that all servers are within 1 region of the
   * average number of regions per server.  If the average is an integer number,
   * all servers will be balanced to the average.  Otherwise, all servers will
   * have either floor(average) or ceiling(average) regions.
   *
   * HBASE-3609 Modeled regionsToMove using Guava's MinMaxPriorityQueue so that
   *   we can fetch from both ends of the queue. 
   * At the beginning, we check whether there was empty region server 
   *   just discovered by Master. If so, we alternately choose new / old
   *   regions from head / tail of regionsToMove, respectively. This alternation
   *   avoids clustering young regions on the newly discovered region server.
   *   Otherwise, we choose new regions from head of regionsToMove.
   *   
   * Another improvement from HBASE-3609 is that we assign regions from
   *   regionsToMove to underloaded servers in round-robin fashion.
   *   Previously one underloaded server would be filled before we move onto
   *   the next underloaded server, leading to clustering of young regions.
   *   
   * Finally, we randomly shuffle underloaded servers so that they receive
   *   offloaded regions relatively evenly across calls to balanceCluster().
   *         
   * The algorithm is currently implemented as such:
   *
   * <ol>
   * <li>Determine the two valid numbers of regions each server should have,
   *     <b>MIN</b>=floor(average) and <b>MAX</b>=ceiling(average).
   *
   * <li>Iterate down the most loaded servers, shedding regions from each so
   *     each server hosts exactly <b>MAX</b> regions.  Stop once you reach a
   *     server that already has &lt;= <b>MAX</b> regions.
   *     <p>
   *     Order the regions to move from most recent to least.
   *
   * <li>Iterate down the least loaded servers, assigning regions so each server
   *     has exactly </b>MIN</b> regions.  Stop once you reach a server that
   *     already has &gt;= <b>MIN</b> regions.
   *
   *     Regions being assigned to underloaded servers are those that were shed
   *     in the previous step.  It is possible that there were not enough
   *     regions shed to fill each underloaded server to <b>MIN</b>.  If so we
   *     end up with a number of regions required to do so, <b>neededRegions</b>.
   *
   *     It is also possible that we were able to fill each underloaded but ended
   *     up with regions that were unassigned from overloaded servers but that
   *     still do not have assignment.
   *
   *     If neither of these conditions hold (no regions needed to fill the
   *     underloaded servers, no regions leftover from overloaded servers),
   *     we are done and return.  Otherwise we handle these cases below.
   *
   * <li>If <b>neededRegions</b> is non-zero (still have underloaded servers),
   *     we iterate the most loaded servers again, shedding a single server from
   *     each (this brings them from having <b>MAX</b> regions to having
   *     <b>MIN</b> regions).
   *
   * <li>We now definitely have more regions that need assignment, either from
   *     the previous step or from the original shedding from overloaded servers.
   *     Iterate the least loaded servers filling each to <b>MIN</b>.
   *
   * <li>If we still have more regions that need assignment, again iterate the
   *     least loaded servers, this time giving each one (filling them to
   *     </b>MAX</b>) until we run out.
   *
   * <li>All servers will now either host <b>MIN</b> or <b>MAX</b> regions.
   *
   *     In addition, any server hosting &gt;= <b>MAX</b> regions is guaranteed
   *     to end up with <b>MAX</b> regions at the end of the balancing.  This
   *     ensures the minimal number of regions possible are moved.
   * </ol>
   *
   * TODO: We can at-most reassign the number of regions away from a particular
   *       server to be how many they report as most loaded.
   *       Should we just keep all assignment in memory?  Any objections?
   *       Does this mean we need HeapSize on HMaster?  Or just careful monitor?
   *       (current thinking is we will hold all assignments in memory)
   *
   * @param clusterState Map of regionservers and their load/region information to
   *                   a list of their most loaded regions
   * @return a list of regions to be moved, including source and destination,
   *         or null if cluster is already balanced
   */
  public List<RegionPlan> balanceCluster(
      Map<ServerName, List<HRegionInfo>> clusterState) {
    boolean emptyRegionServerPresent = false;
    long startTime = System.currentTimeMillis();

    int numServers = clusterState.size();
    if (numServers == 0) {
      LOG.debug("numServers=0 so skipping load balancing");
      return null;
    }
    NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad =
      new TreeMap<ServerAndLoad, List<HRegionInfo>>();
    int numRegions = 0;
    StringBuilder strBalanceParam = new StringBuilder("Server information: ");
    // Iterate so we can count regions as we build the map
    for (Map.Entry<ServerName, List<HRegionInfo>> server: clusterState.entrySet()) {
      List<HRegionInfo> regions = server.getValue();
      int sz = regions.size();
      if (sz == 0) emptyRegionServerPresent = true;
      numRegions += sz;
      serversByLoad.put(new ServerAndLoad(server.getKey(), sz), regions);
      strBalanceParam.append(server.getKey().getServerName()).append("=").
        append(server.getValue().size()).append(", ");
    }
    strBalanceParam.delete(strBalanceParam.length() - 2,
      strBalanceParam.length());
    LOG.debug(strBalanceParam.toString());

    // Check if we even need to do any load balancing
    float average = (float)numRegions / numServers; // for logging
    // HBASE-3681 check sloppiness first
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    if (serversByLoad.lastKey().getLoad() <= ceiling &&
       serversByLoad.firstKey().getLoad() >= floor) {
      // Skipped because no server outside (min,max) range
      LOG.info("Skipping load balancing because balanced cluster; " +
        "servers=" + numServers + " " +
        "regions=" + numRegions + " average=" + average + " " +
        "mostloaded=" + serversByLoad.lastKey().getLoad() +
        " leastloaded=" + serversByLoad.firstKey().getLoad());
      return null;
    }
    int min = numRegions / numServers;
    int max = numRegions % numServers == 0 ? min : min + 1;

    // Using to check banance result.
    strBalanceParam.delete(0, strBalanceParam.length());
    strBalanceParam.append("Balance parameter: numRegions=").append(numRegions)
        .append(", numServers=").append(numServers).append(", max=").append(max)
        .append(", min=").append(min);
    LOG.debug(strBalanceParam.toString());
    
    // Balance the cluster
    // TODO: Look at data block locality or a more complex load to do this
    MinMaxPriorityQueue<RegionPlan> regionsToMove =
      MinMaxPriorityQueue.orderedBy(rpComparator).create();
    List<RegionPlan> regionsToReturn = new ArrayList<RegionPlan>();

    // Walk down most loaded, pruning each to the max
    int serversOverloaded = 0;
    // flag used to fetch regions from head and tail of list, alternately
    boolean fetchFromTail = false;
    Map<ServerName, BalanceInfo> serverBalanceInfo =
      new TreeMap<ServerName, BalanceInfo>();
    for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server:
        serversByLoad.descendingMap().entrySet()) {
      ServerAndLoad sal = server.getKey();
      int regionCount = sal.getLoad();
      if (regionCount <= max) {
        serverBalanceInfo.put(sal.getServerName(), new BalanceInfo(0, 0));
        break;
      }
      serversOverloaded++;
      List<HRegionInfo> regions = server.getValue();
      int numToOffload = Math.min(regionCount - max, regions.size());
      // account for the out-of-band regions which were assigned to this server
      // after some other region server crashed 
      Collections.sort(regions, riComparator);
      int numTaken = 0;
      for (int i = 0; i <= numToOffload; ) {
        HRegionInfo hri = regions.get(i); // fetch from head
        if (fetchFromTail) {
          hri = regions.get(regions.size() - 1 - i);
        }
        i++;
        // Don't rebalance meta regions.
        if (hri.isMetaRegion()) continue;
        regionsToMove.add(new RegionPlan(hri, sal.getServerName(), null));
        numTaken++;
        if (numTaken >= numToOffload) break;
        // fetch in alternate order if there is new region server
        if (emptyRegionServerPresent) {
          fetchFromTail = !fetchFromTail;
        }
      }
      serverBalanceInfo.put(sal.getServerName(),
        new BalanceInfo(numToOffload, (-1)*numTaken));
    }
    int totalNumMoved = regionsToMove.size();

    // Walk down least loaded, filling each to the min
    int neededRegions = 0; // number of regions needed to bring all up to min
    fetchFromTail = false;

    Map<ServerName, Integer> underloadedServers = new HashMap<ServerName, Integer>();
    for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server:
        serversByLoad.entrySet()) {
      int regionCount = server.getKey().getLoad();
      if (regionCount >= min) {
        break;
      }
      underloadedServers.put(server.getKey().getServerName(), min - regionCount);
    }
    // number of servers that get new regions
    int serversUnderloaded = underloadedServers.size();
    int incr = 1;
    List<ServerName> sns =
      Arrays.asList(underloadedServers.keySet().toArray(new ServerName[serversUnderloaded]));
    Collections.shuffle(sns, RANDOM);
    while (regionsToMove.size() > 0) {
      int cnt = 0;
      int i = incr > 0 ? 0 : underloadedServers.size()-1;
      for (; i >= 0 && i < underloadedServers.size(); i += incr) {
        if (regionsToMove.isEmpty()) break;
        ServerName si = sns.get(i);
        int numToTake = underloadedServers.get(si);
        if (numToTake == 0) continue;

        addRegionPlan(regionsToMove, fetchFromTail, si, regionsToReturn);
        if (emptyRegionServerPresent) {
          fetchFromTail = !fetchFromTail;
        }

        underloadedServers.put(si, numToTake-1);
        cnt++;
        BalanceInfo bi = serverBalanceInfo.get(si);
        if (bi == null) {
          bi = new BalanceInfo(0, 0);
          serverBalanceInfo.put(si, bi);
        }
        bi.setNumRegionsAdded(bi.getNumRegionsAdded()+1);
      }
      if (cnt == 0) break;
      // iterates underloadedServers in the other direction
      incr = -incr;
    }
    for (Integer i : underloadedServers.values()) {
      // If we still want to take some, increment needed
      neededRegions += i;
    }

    // If none needed to fill all to min and none left to drain all to max,
    // we are done
    if (neededRegions == 0 && regionsToMove.isEmpty()) {
      long endTime = System.currentTimeMillis();
      LOG.info("Calculated a load balance in " + (endTime-startTime) + "ms. " +
          "Moving " + totalNumMoved + " regions off of " +
          serversOverloaded + " overloaded servers onto " +
          serversUnderloaded + " less loaded servers");
      return regionsToReturn;
    }

    // Need to do a second pass.
    // Either more regions to assign out or servers that are still underloaded

    // If we need more to fill min, grab one from each most loaded until enough
    if (neededRegions != 0) {
      // Walk down most loaded, grabbing one from each until we get enough
      for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
        serversByLoad.descendingMap().entrySet()) {
        BalanceInfo balanceInfo =
          serverBalanceInfo.get(server.getKey().getServerName());
        int idx =
          balanceInfo == null ? 0 : balanceInfo.getNextRegionForUnload();
        if (idx >= server.getValue().size()) break;
        HRegionInfo region = server.getValue().get(idx);
        if (region.isMetaRegion()) continue; // Don't move meta regions.
        regionsToMove.add(new RegionPlan(region, server.getKey().getServerName(), null));
        totalNumMoved++;
        if (--neededRegions == 0) {
          // No more regions needed, done shedding
          break;
        }
      }
    }

    // Now we have a set of regions that must be all assigned out
    // Assign each underloaded up to the min, then if leftovers, assign to max

    // Walk down least loaded, assigning to each to fill up to min
    for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
        serversByLoad.entrySet()) {
      int regionCount = server.getKey().getLoad();
      if (regionCount >= min) break;
      BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey().getServerName());
      if(balanceInfo != null) {
        regionCount += balanceInfo.getNumRegionsAdded();
      }
      if(regionCount >= min) {
        continue;
      }
      int numToTake = min - regionCount;
      int numTaken = 0;
      while(numTaken < numToTake && 0 < regionsToMove.size()) {
        addRegionPlan(regionsToMove, fetchFromTail,
          server.getKey().getServerName(), regionsToReturn);
        numTaken++;
        if (emptyRegionServerPresent) {
          fetchFromTail = !fetchFromTail;
        }
      }
    }

    // If we still have regions to dish out, assign underloaded to max
    if (0 < regionsToMove.size()) {
      for (Map.Entry<ServerAndLoad, List<HRegionInfo>> server :
        serversByLoad.entrySet()) {
        int regionCount = server.getKey().getLoad();
        if(regionCount >= max) {
          break;
        }
        addRegionPlan(regionsToMove, fetchFromTail,
          server.getKey().getServerName(), regionsToReturn);
        if (emptyRegionServerPresent) {
          fetchFromTail = !fetchFromTail;
        }
        if (regionsToMove.isEmpty()) {
          break;
        }
      }
    }

    long endTime = System.currentTimeMillis();

    if (!regionsToMove.isEmpty() || neededRegions != 0) {
      // Emit data so can diagnose how balancer went astray.
      LOG.warn("regionsToMove=" + totalNumMoved +
        ", numServers=" + numServers + ", serversOverloaded=" + serversOverloaded +
        ", serversUnderloaded=" + serversUnderloaded);
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<ServerName, List<HRegionInfo>> e: clusterState.entrySet()) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(e.getKey().toString());
        sb.append(" ");
        sb.append(e.getValue().size());
      }
      LOG.warn("Input " + sb.toString());
    }

    // All done!
    LOG.info("Done. Calculated a load balance in " + (endTime-startTime) + "ms. " +
        "Moving " + totalNumMoved + " regions off of " +
        serversOverloaded + " overloaded servers onto " +
        serversUnderloaded + " less loaded servers");

    return regionsToReturn;
  }

  /**
   * Add a region from the head or tail to the List of regions to return.
   */
  void addRegionPlan(final MinMaxPriorityQueue<RegionPlan> regionsToMove,
      final boolean fetchFromTail, final ServerName sn, List<RegionPlan> regionsToReturn) {
    RegionPlan rp = null;
    if (!fetchFromTail) rp = regionsToMove.remove();
    else rp = regionsToMove.removeLast();
    rp.setDestination(sn);
    regionsToReturn.add(rp);
  }

  /**
   * @param regions
   * @return Randomization of passed <code>regions</code>
   */
  static List<HRegionInfo> randomize(final List<HRegionInfo> regions) {
    Collections.shuffle(regions, RANDOM);
    return regions;
  }

  /**
   * Stores additional per-server information about the regions added/removed
   * during the run of the balancing algorithm.
   *
   * For servers that shed regions, we need to track which regions we have
   * already shed.  <b>nextRegionForUnload</b> contains the index in the list
   * of regions on the server that is the next to be shed.
   */
  private static class BalanceInfo {

    private final int nextRegionForUnload;
    private int numRegionsAdded;

    public BalanceInfo(int nextRegionForUnload, int numRegionsAdded) {
      this.nextRegionForUnload = nextRegionForUnload;
      this.numRegionsAdded = numRegionsAdded;
    }

    public int getNextRegionForUnload() {
      return nextRegionForUnload;
    }

    public int getNumRegionsAdded() {
      return numRegionsAdded;
    }

    public void setNumRegionsAdded(int numAdded) {
      this.numRegionsAdded = numAdded;
    }
  }

  /**
   * Generates a bulk assignment plan to be used on cluster startup using a
   * simple round-robin assignment.
   * <p>
   * Takes a list of all the regions and all the servers in the cluster and
   * returns a map of each server to the regions that it should be assigned.
   * <p>
   * Currently implemented as a round-robin assignment.  Same invariant as
   * load balancing, all servers holding floor(avg) or ceiling(avg).
   *
   * TODO: Use block locations from HDFS to place regions with their blocks
   *
   * @param regions all regions
   * @param servers all servers
   * @return map of server to the regions it should take, or null if no
   *         assignment is possible (ie. no regions or no servers)
   */
  public static Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) {
    if (regions.isEmpty() || servers.isEmpty()) {
      return null;
    }
    Map<ServerName, List<HRegionInfo>> assignments =
      new TreeMap<ServerName,List<HRegionInfo>>();
    int numRegions = regions.size();
    int numServers = servers.size();
    int max = (int)Math.ceil((float)numRegions/numServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = RANDOM.nextInt(numServers);
    }
    int regionIdx = 0;
    for (int j = 0; j < numServers; j++) {
      ServerName server = servers.get((j + serverIdx) % numServers);
      List<HRegionInfo> serverRegions = new ArrayList<HRegionInfo>(max);
      for (int i=regionIdx; i<numRegions; i += numServers) {
        serverRegions.add(regions.get(i % numRegions));
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }
    return assignments;
  }

  /**
   * Generates a bulk assignment startup plan, attempting to reuse the existing
   * assignment information from META, but adjusting for the specified list of
   * available/online servers available for assignment.
   * <p>
   * Takes a map of all regions to their existing assignment from META.  Also
   * takes a list of online servers for regions to be assigned to.  Attempts to
   * retain all assignment, so in some instances initial assignment will not be
   * completely balanced.
   * <p>
   * Any leftover regions without an existing server to be assigned to will be
   * assigned randomly to available servers.
   * @param regions regions and existing assignment from meta
   * @param servers available servers
   * @return map of servers and regions to be assigned to them
   */
  public static Map<ServerName, List<HRegionInfo>> retainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    Map<ServerName, List<HRegionInfo>> assignments =
      new TreeMap<ServerName, List<HRegionInfo>>();
    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<HRegionInfo>());
    }
    for (Map.Entry<HRegionInfo, ServerName> region : regions.entrySet()) {
      ServerName sn = region.getValue();
      if (sn != null && servers.contains(sn)) {
        assignments.get(sn).add(region.getKey());
      } else {
        int size = assignments.size();
        assignments.get(servers.get(RANDOM.nextInt(size))).add(region.getKey());
      }
    }
    return assignments;
  }

  /**
   * Find the block locations for all of the files for the specified region.
   *
   * Returns an ordered list of hosts that are hosting the blocks for this
   * region.  The weight of each host is the sum of the block lengths of all
   * files on that host, so the first host in the list is the server which
   * holds the most bytes of the given region's HFiles.
   *
   * TODO: Make this work.  Need to figure out how to match hadoop's hostnames
   *       given for block locations with our HServerAddress.
   * TODO: Use the right directory for the region
   * TODO: Use getFileBlockLocations on the files not the directory
   *
   * @param fs the filesystem
   * @param region region
   * @return ordered list of hosts holding blocks of the specified region
   * @throws IOException if any filesystem errors
   */
  @SuppressWarnings("unused")
  private List<String> getTopBlockLocations(FileSystem fs, HRegionInfo region)
  throws IOException {
    String encodedName = region.getEncodedName();
    Path path = new Path("/hbase/table/" + encodedName);
    FileStatus status = fs.getFileStatus(path);
    BlockLocation [] blockLocations =
      fs.getFileBlockLocations(status, 0, status.getLen());
    Map<HostAndWeight,HostAndWeight> hostWeights =
      new TreeMap<HostAndWeight,HostAndWeight>(new HostAndWeight.HostComparator());
    for(BlockLocation bl : blockLocations) {
      String [] hosts = bl.getHosts();
      long len = bl.getLength();
      for(String host : hosts) {
        HostAndWeight haw = hostWeights.get(host);
        if(haw == null) {
          haw = new HostAndWeight(host, len);
          hostWeights.put(haw, haw);
        } else {
          haw.addWeight(len);
        }
      }
    }
    NavigableSet<HostAndWeight> orderedHosts = new TreeSet<HostAndWeight>(
        new HostAndWeight.WeightComparator());
    orderedHosts.addAll(hostWeights.values());
    List<String> topHosts = new ArrayList<String>(orderedHosts.size());
    for(HostAndWeight haw : orderedHosts.descendingSet()) {
      topHosts.add(haw.getHost());
    }
    return topHosts;
  }

  /**
   * Stores the hostname and weight for that hostname.
   *
   * This is used when determining the physical locations of the blocks making
   * up a region.
   *
   * To make a prioritized list of the hosts holding the most data of a region,
   * this class is used to count the total weight for each host.  The weight is
   * currently just the size of the file.
   */
  private static class HostAndWeight {

    private final String host;
    private long weight;

    public HostAndWeight(String host, long weight) {
      this.host = host;
      this.weight = weight;
    }

    public void addWeight(long weight) {
      this.weight += weight;
    }

    public String getHost() {
      return host;
    }

    public long getWeight() {
      return weight;
    }

    private static class HostComparator implements Comparator<HostAndWeight> {
      @Override
      public int compare(HostAndWeight l, HostAndWeight r) {
        return l.getHost().compareTo(r.getHost());
      }
    }

    private static class WeightComparator implements Comparator<HostAndWeight> {
      @Override
      public int compare(HostAndWeight l, HostAndWeight r) {
        if(l.getWeight() == r.getWeight()) {
          return l.getHost().compareTo(r.getHost());
        }
        return l.getWeight() < r.getWeight() ? -1 : 1;
      }
    }
  }

  /**
   * Generates an immediate assignment plan to be used by a new master for
   * regions in transition that do not have an already known destination.
   *
   * Takes a list of regions that need immediate assignment and a list of
   * all available servers.  Returns a map of regions to the server they
   * should be assigned to.
   *
   * This method will return quickly and does not do any intelligent
   * balancing.  The goal is to make a fast decision not the best decision
   * possible.
   *
   * Currently this is random.
   *
   * @param regions
   * @param servers
   * @return map of regions to the server it should be assigned to
   */
  public static Map<HRegionInfo, ServerName> immediateAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) {
    Map<HRegionInfo,ServerName> assignments =
      new TreeMap<HRegionInfo,ServerName>();
    for(HRegionInfo region : regions) {
      assignments.put(region, servers.get(RANDOM.nextInt(servers.size())));
    }
    return assignments;
  }

  public static ServerName randomAssignment(List<ServerName> servers) {
    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    return servers.get(RANDOM.nextInt(servers.size()));
  }

  /**
   * Stores the plan for the move of an individual region.
   *
   * Contains info for the region being moved, info for the server the region
   * should be moved from, and info for the server the region should be moved
   * to.
   *
   * The comparable implementation of this class compares only the region
   * information and not the source/dest server info.
   */
  public static class RegionPlan implements Comparable<RegionPlan> {
    private final HRegionInfo hri;
    private final ServerName source;
    private ServerName dest;

    /**
     * Instantiate a plan for a region move, moving the specified region from
     * the specified source server to the specified destination server.
     *
     * Destination server can be instantiated as null and later set
     * with {@link #setDestination(ServerName)}.
     *
     * @param hri region to be moved
     * @param source regionserver region should be moved from
     * @param dest regionserver region should be moved to
     */
    public RegionPlan(final HRegionInfo hri, ServerName source, ServerName dest) {
      this.hri = hri;
      this.source = source;
      this.dest = dest;
    }

    /**
     * Set the destination server for the plan for this region.
     */
    public void setDestination(ServerName dest) {
      this.dest = dest;
    }

    /**
     * Get the source server for the plan for this region.
     * @return server info for source
     */
    public ServerName getSource() {
      return source;
    }

    /**
     * Get the destination server for the plan for this region.
     * @return server info for destination
     */
    public ServerName getDestination() {
      return dest;
    }

    /**
     * Get the encoded region name for the region this plan is for.
     * @return Encoded region name
     */
    public String getRegionName() {
      return this.hri.getEncodedName();
    }

    public HRegionInfo getRegionInfo() {
      return this.hri;
    }

    /**
     * Compare the region info.
     * @param o region plan you are comparing against
     */
    @Override
    public int compareTo(RegionPlan o) {
      return getRegionName().compareTo(o.getRegionName());
    }

    @Override
    public String toString() {
      return "hri=" + this.hri.getRegionNameAsString() + ", src=" +
        (this.source == null? "": this.source.toString()) +
        ", dest=" + (this.dest == null? "": this.dest.toString());
    }
  }
}
