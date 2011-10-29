/**
 * Copyright 2011 The Apache Software Foundation
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;

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
public class DefaultLoadBalancer implements LoadBalancer {
  private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  // slop for regions
  private float slop;
  private Configuration config;
  private ClusterStatus status;
  private MasterServices services;

  public void setClusterStatus(ClusterStatus st) {
    this.status = st;
  }

  public void setMasterServices(MasterServices masterServices) {
    this.services = masterServices;
  }

  @Override
  public void setConf(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
    if (slop < 0) slop = 0;
    else if (slop > 1) slop = 1;
    this.config = conf;
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  /*
  * The following comparator assumes that RegionId from HRegionInfo can
  * represent the age of the region - larger RegionId means the region
  * is younger.
  * This comparator is used in balanceCluster() to account for the out-of-band
  * regions which were assigned to the server after some other region server
  * crashed.
  */
   private class RegionInfoComparator implements Comparator<HRegionInfo> {
       @Override
       public int compare(HRegionInfo l, HRegionInfo r) {
          long diff = r.getRegionId() - l.getRegionId();
          if (diff < 0) return -1;
          if (diff > 0) return 1;
          return 0;
       }
   }


   RegionInfoComparator riComparator = new RegionInfoComparator();
   
   private class RegionPlanComparator implements Comparator<RegionPlan> {
    @Override
    public int compare(RegionPlan l, RegionPlan r) {
      long diff = r.getRegionInfo().getRegionId() - l.getRegionInfo().getRegionId();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }

  RegionPlanComparator rpComparator = new RegionPlanComparator();

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
    // Iterate so we can count regions as we build the map
    for (Map.Entry<ServerName, List<HRegionInfo>> server: clusterState.entrySet()) {
      List<HRegionInfo> regions = server.getValue();
      int sz = regions.size();
      if (sz == 0) emptyRegionServerPresent = true;
      numRegions += sz;
      serversByLoad.put(new ServerAndLoad(server.getKey(), sz), regions);
    }
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

    // Using to check balance result.
    StringBuilder strBalanceParam = new StringBuilder();
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
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
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
  public Map<ServerName, List<HRegionInfo>> retainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
    // Group all of the old assignments by their hostname.
    // We can't group directly by ServerName since the servers all have
    // new start-codes.
    
    // Group the servers by their hostname. It's possible we have multiple
    // servers on the same host on different ports.
    ArrayListMultimap<String, ServerName> serversByHostname =
        ArrayListMultimap.create();
    for (ServerName server : servers) {
      serversByHostname.put(server.getHostname(), server);
    }
    
    // Now come up with new assignments
    Map<ServerName, List<HRegionInfo>> assignments =
      new TreeMap<ServerName, List<HRegionInfo>>();
    
    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<HRegionInfo>());
    }
    
    // Collection of the hostnames that used to have regions
    // assigned, but for which we no longer have any RS running
    // after the cluster restart.
    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();
    
    int numRandomAssignments = 0;
    int numRetainedAssigments = 0;
    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      HRegionInfo region = entry.getKey();
      ServerName oldServerName = entry.getValue();
      List<ServerName> localServers = new ArrayList<ServerName>();
      if (oldServerName != null) {
        localServers = serversByHostname.get(oldServerName.getHostname());
      }
      if (localServers.isEmpty()) {
        // No servers on the new cluster match up with this hostname,
        // assign randomly.
        ServerName randomServer = servers.get(RANDOM.nextInt(servers.size()));
        assignments.get(randomServer).add(region);
        numRandomAssignments++;
        if (oldServerName != null) oldHostsNoLongerPresent.add(oldServerName.getHostname());
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        assignments.get(localServers.get(0)).add(region);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        int size = localServers.size();
        ServerName target = localServers.get(RANDOM.nextInt(size));
        assignments.get(target).add(region);
        numRetainedAssigments++;
      }
    }
    
    String randomAssignMsg = "";
    if (numRandomAssignments > 0) {
      randomAssignMsg = numRandomAssignments + " regions were assigned " +
      		"to random hosts, since the old hosts for these regions are no " +
      		"longer present in the cluster. These hosts were:\n  " +
          Joiner.on("\n  ").join(oldHostsNoLongerPresent);
    }
    
    LOG.info("Reassigned " + regions.size() + " regions. " +
        numRetainedAssigments + " retained the pre-restart assignment. " +
        randomAssignMsg);
    return assignments;
  }

  /**
   * Returns an ordered list of hosts that are hosting the blocks for this
   * region.  The weight of each host is the sum of the block lengths of all
   * files on that host, so the first host in the list is the server which
   * holds the most bytes of the given region's HFiles.
   *
   * @param fs the filesystem
   * @param region region
   * @return ordered list of hosts holding blocks of the specified region
   */
  @SuppressWarnings("unused")
  private List<ServerName> getTopBlockLocations(FileSystem fs,
    HRegionInfo region) {
    List<ServerName> topServerNames = null;
    try {
      HTableDescriptor tableDescriptor = getTableDescriptor(
        region.getTableName());
      if (tableDescriptor != null) {
        HDFSBlocksDistribution blocksDistribution =
          HRegion.computeHDFSBlocksDistribution(config, tableDescriptor,
          region.getEncodedName());
        List<String> topHosts = blocksDistribution.getTopHosts();
        topServerNames = mapHostNameToServerName(topHosts);
      }
    } catch (IOException ioe) {
      LOG.debug("IOException during HDFSBlocksDistribution computation. for " +
        "region = " + region.getEncodedName() , ioe);
    }
    
    return topServerNames;
  }

  /**
   * return HTableDescriptor for a given tableName
   * @param tableName the table name
   * @return HTableDescriptor
   * @throws IOException
   */
  private HTableDescriptor getTableDescriptor(byte[] tableName)
    throws IOException {
    HTableDescriptor tableDescriptor = null;
    try {
      if ( this.services != null)
      {
        tableDescriptor = this.services.getTableDescriptors().
          get(Bytes.toString(tableName));
    }
    } catch (TableExistsException tee) {
      LOG.debug("TableExistsException during getTableDescriptors." +
        " Current table name = " + tableName , tee);
    } catch (FileNotFoundException fnfe) {
      LOG.debug("FileNotFoundException during getTableDescriptors." +
        " Current table name = " + tableName , fnfe);
    }

    return tableDescriptor;
    }

  /**
   * Map hostname to ServerName, The output ServerName list will have the same
   * order as input hosts.
   * @param hosts the list of hosts
   * @return ServerName list
   */  
  private List<ServerName> mapHostNameToServerName(List<String> hosts) {
    if ( hosts == null || status == null) {
      return null;
    }

    List<ServerName> topServerNames = new ArrayList<ServerName>();
    Collection<ServerName> regionServers = status.getServers();

    // create a mapping from hostname to ServerName for fast lookup
    HashMap<String, ServerName> hostToServerName =
      new HashMap<String, ServerName>();
    for (ServerName sn : regionServers) {
      hostToServerName.put(sn.getHostname(), sn);
        }

    for (String host : hosts ) {
      ServerName sn = hostToServerName.get(host);
      // it is possible that HDFS is up ( thus host is valid ),
      // but RS is down ( thus sn is null )
      if (sn != null) {
        topServerNames.add(sn);
      }
    }
    return topServerNames;
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
  public Map<HRegionInfo, ServerName> immediateAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) {
    Map<HRegionInfo,ServerName> assignments =
      new TreeMap<HRegionInfo,ServerName>();
    for(HRegionInfo region : regions) {
      assignments.put(region, servers.get(RANDOM.nextInt(servers.size())));
    }
    return assignments;
  }

  public ServerName randomAssignment(List<ServerName> servers) {
    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    return servers.get(RANDOM.nextInt(servers.size()));
  }

}
