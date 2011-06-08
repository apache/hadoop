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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLoadBalancer {
  private static final Log LOG = LogFactory.getLog(TestLoadBalancer.class);

  private static LoadBalancer loadBalancer;

  private static Random rand;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0");
    loadBalancer = new LoadBalancer(conf);
    rand = new Random();
  }

  // int[testnum][servernumber] -> numregions
  int [][] clusterStateMocks = new int [][] {
      // 1 node
      new int [] { 0 },
      new int [] { 1 },
      new int [] { 10 },
      // 2 node
      new int [] { 0, 0 },
      new int [] { 2, 0 },
      new int [] { 2, 1 },
      new int [] { 2, 2 },
      new int [] { 2, 3 },
      new int [] { 2, 4 },
      new int [] { 1, 1 },
      new int [] { 0, 1 },
      new int [] { 10, 1 },
      new int [] { 14, 1432 },
      new int [] { 47, 53 },
      // 3 node
      new int [] { 0, 1, 2 },
      new int [] { 1, 2, 3 },
      new int [] { 0, 2, 2 },
      new int [] { 0, 3, 0 },
      new int [] { 0, 4, 0 },
      new int [] { 20, 20, 0 },
      // 4 node
      new int [] { 0, 1, 2, 3 },
      new int [] { 4, 0, 0, 0 },
      new int [] { 5, 0, 0, 0 },
      new int [] { 6, 6, 0, 0 },
      new int [] { 6, 2, 0, 0 },
      new int [] { 6, 1, 0, 0 },
      new int [] { 6, 0, 0, 0 },
      new int [] { 4, 4, 4, 7 },
      new int [] { 4, 4, 4, 8 },
      new int [] { 0, 0, 0, 7 },
      // 5 node
      new int [] { 1, 1, 1, 1, 4 },
      // more nodes
      new int [] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 10 },
      new int [] { 6, 6, 5, 6, 6, 6, 6, 6, 6, 1 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 54 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 55 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 56 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 16 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 8 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 9 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 10 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 123 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 155 },
      new int [] { 0, 0, 144, 1, 1, 1, 1, 1123, 133, 138, 12, 1444 },
      new int [] { 0, 0, 144, 1, 0, 4, 1, 1123, 133, 138, 12, 1444 },
      new int [] { 1538, 1392, 1561, 1557, 1535, 1553, 1385, 1542, 1619 }
  };

  int [][] regionsAndServersMocks = new int [][] {
      // { num regions, num servers }
      new int [] { 0, 0 },
      new int [] { 0, 1 },
      new int [] { 1, 1 },
      new int [] { 2, 1 },
      new int [] { 10, 1 },
      new int [] { 1, 2 },
      new int [] { 2, 2 },
      new int [] { 3, 2 },
      new int [] { 1, 3 },
      new int [] { 2, 3 },
      new int [] { 3, 3 },
      new int [] { 25, 3 },
      new int [] { 2, 10 },
      new int [] { 2, 100 },
      new int [] { 12, 10 },
      new int [] { 12, 100 },
  };

  @Test
  public void testRandomizer() {
    for(int [] mockCluster : clusterStateMocks) {
      if (mockCluster.length < 5) continue;
      Map<ServerName, List<HRegionInfo>> servers =
        mockClusterServers(mockCluster);
      for (Map.Entry<ServerName, List<HRegionInfo>> e: servers.entrySet()) {
        List<HRegionInfo> original = e.getValue();
        if (original.size() < 5) continue;
        // Try ten times in case random chances upon original order more than
        // one or two times in a row.
        boolean same = true;
        for (int i = 0; i < 10 && same; i++) {
          List<HRegionInfo> copy = new ArrayList<HRegionInfo>(original);
          System.out.println("Randomizing before " + copy.size());
          for (HRegionInfo hri: copy) {
            System.out.println(hri.getEncodedName());
          }
          List<HRegionInfo> randomized = LoadBalancer.randomize(copy);
          System.out.println("Randomizing after " + randomized.size());
          for (HRegionInfo hri: randomized) {
            System.out.println(hri.getEncodedName());
          }
          if (original.equals(randomized)) continue;
          same = false;
          break;
        }
        assertFalse(same);
      }
    }
  }

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either
   * floor(average) or ceiling(average)
   *
   * @throws Exception
   */
  @Test
  public void testBalanceCluster() throws Exception {

    for(int [] mockCluster : clusterStateMocks) {
      Map<ServerName, List<HRegionInfo>> servers =  mockClusterServers(mockCluster);
      List <LoadBalancer.ServerAndLoad> list = convertToList(servers);
      LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
      List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
      List<LoadBalancer.ServerAndLoad> balancedCluster = reconcile(list, plans);
      LOG.info("Mock Balance : " + printMock(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
      for(Map.Entry<ServerName, List<HRegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }

  }

  /**
   * Invariant is that all servers have between floor(avg) and ceiling(avg)
   * number of regions.
   */
  public void assertClusterAsBalanced(List<LoadBalancer.ServerAndLoad> servers) {
    int numServers = servers.size();
    int numRegions = 0;
    int maxRegions = 0;
    int minRegions = Integer.MAX_VALUE;
    for(LoadBalancer.ServerAndLoad server : servers) {
      int nr = server.getLoad();
      if(nr > maxRegions) {
        maxRegions = nr;
      }
      if(nr < minRegions) {
        minRegions = nr;
      }
      numRegions += nr;
    }
    if(maxRegions - minRegions < 2) {
      // less than 2 between max and min, can't balance
      return;
    }
    int min = numRegions / numServers;
    int max = numRegions % numServers == 0 ? min : min + 1;

    for(LoadBalancer.ServerAndLoad server : servers) {
      assertTrue(server.getLoad() <= max);
      assertTrue(server.getLoad() >= min);
    }
  }

  /**
   * Tests immediate assignment.
   *
   * Invariant is that all regions have an assignment.
   *
   * @throws Exception
   */
  @Test
  public void testImmediateAssignment() throws Exception {
    for(int [] mock : regionsAndServersMocks) {
      LOG.debug("testImmediateAssignment with " + mock[0] + " regions and " + mock[1] + " servers");
      List<HRegionInfo> regions = randomRegions(mock[0]);
      List<LoadBalancer.ServerAndLoad> servers = randomServers(mock[1], 0);
      List<ServerName> list = getListOfServerNames(servers);
      Map<HRegionInfo,ServerName> assignments =
        LoadBalancer.immediateAssignment(regions, list);
      assertImmediateAssignment(regions, list, assignments);
      returnRegions(regions);
      returnServers(list);
    }
  }

  /**
   * All regions have an assignment.
   * @param regions
   * @param servers
   * @param assignments
   */
  private void assertImmediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers, Map<HRegionInfo, ServerName> assignments) {
    for(HRegionInfo region : regions) {
      assertTrue(assignments.containsKey(region));
    }
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   *
   * Round-robin.  Should yield a balanced cluster so same invariant as the load
   * balancer holds, all servers holding either floor(avg) or ceiling(avg).
   *
   * @throws Exception
   */
  @Test
  public void testBulkAssignment() throws Exception {
    for(int [] mock : regionsAndServersMocks) {
      LOG.debug("testBulkAssignment with " + mock[0] + " regions and " + mock[1] + " servers");
      List<HRegionInfo> regions = randomRegions(mock[0]);
      List<LoadBalancer.ServerAndLoad> servers = randomServers(mock[1], 0);
      List<ServerName> list = getListOfServerNames(servers);
      Map<ServerName, List<HRegionInfo>> assignments =
        LoadBalancer.roundRobinAssignment(regions, list);
      float average = (float)regions.size()/servers.size();
      int min = (int)Math.floor(average);
      int max = (int)Math.ceil(average);
      if(assignments != null && !assignments.isEmpty()) {
        for(List<HRegionInfo> regionList : assignments.values()) {
          assertTrue(regionList.size() == min || regionList.size() == max);
        }
      }
      returnRegions(regions);
      returnServers(list);
    }
  }

  /**
   * Test the cluster startup bulk assignment which attempts to retain
   * assignment info.
   * @throws Exception
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    List<LoadBalancer.ServerAndLoad> servers = randomServers(10, 10);
    List<HRegionInfo> regions = randomRegions(100);
    Map<HRegionInfo, ServerName> existing =
      new TreeMap<HRegionInfo, ServerName>();
    for (int i = 0; i < regions.size(); i++) {
      existing.put(regions.get(i), servers.get(i % servers.size()).getServerName());
    }
    List<ServerName> listOfServerNames = getListOfServerNames(servers);
    Map<ServerName, List<HRegionInfo>> assignment =
      LoadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);

    // Include two new servers that were not there before
    List<LoadBalancer.ServerAndLoad> servers2 =
      new ArrayList<LoadBalancer.ServerAndLoad>(servers);
    servers2.add(randomServer(10));
    servers2.add(randomServer(10));
    listOfServerNames = getListOfServerNames(servers2);
    assignment = LoadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);

    // Remove two of the servers that were previously there
    List<LoadBalancer.ServerAndLoad> servers3 =
      new ArrayList<LoadBalancer.ServerAndLoad>(servers);
    servers3.remove(servers3.size()-1);
    servers3.remove(servers3.size()-2);
    listOfServerNames = getListOfServerNames(servers2);
    assignment = LoadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);
  }

  private List<ServerName> getListOfServerNames(final List<LoadBalancer.ServerAndLoad> sals) {
    List<ServerName> list = new ArrayList<ServerName>();
    for (LoadBalancer.ServerAndLoad e: sals) {
      list.add(e.getServerName());
    }
    return list;
  }

  /**
   * Asserts a valid retained assignment plan.
   * <p>
   * Must meet the following conditions:
   * <ul>
   *   <li>Every input region has an assignment, and to an online server
   *   <li>If a region had an existing assignment to a server with the same
   *       address a a currently online server, it will be assigned to it
   * </ul>
   * @param existing
   * @param servers
   * @param assignment
   */
  private void assertRetainedAssignment(
      Map<HRegionInfo, ServerName> existing, List<ServerName> servers,
      Map<ServerName, List<HRegionInfo>> assignment) {
    // Verify condition 1, every region assigned, and to online server
    Set<ServerName> onlineServerSet = new TreeSet<ServerName>(servers);
    Set<HRegionInfo> assignedRegions = new TreeSet<HRegionInfo>();
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      assertTrue("Region assigned to server that was not listed as online",
          onlineServerSet.contains(a.getKey()));
      for (HRegionInfo r : a.getValue()) assignedRegions.add(r);
    }
    assertEquals(existing.size(), assignedRegions.size());

    // Verify condition 2, if server had existing assignment, must have same
    Set<ServerName> onlineAddresses = new TreeSet<ServerName>();
    for (ServerName s : servers) onlineAddresses.add(s);
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      for (HRegionInfo r : a.getValue()) {
        ServerName address = existing.get(r);
        if (address != null && onlineAddresses.contains(address)) {
          assertTrue(a.getKey().equals(address));
        }
      }
    }
  }

  private String printStats(List<LoadBalancer.ServerAndLoad> servers) {
    int numServers = servers.size();
    int totalRegions = 0;
    for(LoadBalancer.ServerAndLoad server : servers) {
      totalRegions += server.getLoad();
    }
    float average = (float)totalRegions / numServers;
    int max = (int)Math.ceil(average);
    int min = (int)Math.floor(average);
    return "[srvr=" + numServers + " rgns=" + totalRegions + " avg=" + average + " max=" + max + " min=" + min + "]";
  }

  private List<LoadBalancer.ServerAndLoad> convertToList(final Map<ServerName, List<HRegionInfo>> servers) {
    List<LoadBalancer.ServerAndLoad> list =
      new ArrayList<LoadBalancer.ServerAndLoad>(servers.size());
    for (Map.Entry<ServerName, List<HRegionInfo>> e: servers.entrySet()) {
      list.add(new LoadBalancer.ServerAndLoad(e.getKey(), e.getValue().size()));
    }
    return list;
  }

  private String printMock(Map<ServerName, List<HRegionInfo>> servers) {
    return printMock(convertToList(servers));
  }

  private String printMock(List<LoadBalancer.ServerAndLoad> balancedCluster) {
    SortedSet<LoadBalancer.ServerAndLoad> sorted =
      new TreeSet<LoadBalancer.ServerAndLoad>(balancedCluster);
    LoadBalancer.ServerAndLoad [] arr =
      sorted.toArray(new LoadBalancer.ServerAndLoad[sorted.size()]);
    StringBuilder sb = new StringBuilder(sorted.size() * 4 + 4);
    sb.append("{ ");
    for(int i = 0; i < arr.length; i++) {
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(arr[i].getLoad());
    }
    sb.append(" }");
    return sb.toString();
  }

  /**
   * This assumes the RegionPlan HSI instances are the same ones in the map, so
   * actually no need to even pass in the map, but I think it's clearer.
   * @param servers
   * @param plans
   * @return
   */
  private List<LoadBalancer.ServerAndLoad> reconcile(List<LoadBalancer.ServerAndLoad> list,
      List<RegionPlan> plans) {
    List<LoadBalancer.ServerAndLoad> result =
      new ArrayList<LoadBalancer.ServerAndLoad>(list.size());
    if (plans == null) return result;
    Map<ServerName, LoadBalancer.ServerAndLoad> map =
      new HashMap<ServerName, LoadBalancer.ServerAndLoad>(list.size());
    for (RegionPlan plan : plans) {
      ServerName source = plan.getSource();
      updateLoad(map, source, -1);
      ServerName destination = plan.getDestination();
      updateLoad(map, destination, +1);
    }
    result.clear();
    result.addAll(map.values());
    return result;
  }

  private void updateLoad(Map<ServerName, LoadBalancer.ServerAndLoad> map,
      final ServerName sn, final int diff) {
    LoadBalancer.ServerAndLoad sal = map.get(sn);
    if (sal == null) return;
    sal = new LoadBalancer.ServerAndLoad(sn, sal.getLoad() + diff);
    map.put(sn, sal);
  }

  private Map<ServerName, List<HRegionInfo>> mockClusterServers(
      int [] mockCluster) {
    int numServers = mockCluster.length;
    Map<ServerName, List<HRegionInfo>> servers =
      new TreeMap<ServerName, List<HRegionInfo>>();
    for(int i = 0; i < numServers; i++) {
      int numRegions = mockCluster[i];
      LoadBalancer.ServerAndLoad sal = randomServer(0);
      List<HRegionInfo> regions = randomRegions(numRegions);
      servers.put(sal.getServerName(), regions);
    }
    return servers;
  }

  private Queue<HRegionInfo> regionQueue = new LinkedList<HRegionInfo>();
  static int regionId = 0;

  private List<HRegionInfo> randomRegions(int numRegions) {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(numRegions);
    byte [] start = new byte[16];
    byte [] end = new byte[16];
    rand.nextBytes(start);
    rand.nextBytes(end);
    for(int i=0;i<numRegions;i++) {
      if(!regionQueue.isEmpty()) {
        regions.add(regionQueue.poll());
        continue;
      }
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      HRegionInfo hri = new HRegionInfo(
          new HTableDescriptor(Bytes.toBytes("table" + i)), start, end,
          false, regionId++);
      regions.add(hri);
    }
    return regions;
  }

  private void returnRegions(List<HRegionInfo> regions) {
    regionQueue.addAll(regions);
  }

  private Queue<ServerName> serverQueue = new LinkedList<ServerName>();

  private LoadBalancer.ServerAndLoad randomServer(final int numRegionsPerServer) {
    if (!this.serverQueue.isEmpty()) {
      ServerName sn = this.serverQueue.poll();
      return new LoadBalancer.ServerAndLoad(sn, numRegionsPerServer);
    }
    String host = "127.0.0.1";
    int port = rand.nextInt(60000);
    long startCode = rand.nextLong();
    ServerName sn = new ServerName(host, port, startCode);
    return new LoadBalancer.ServerAndLoad(sn, numRegionsPerServer);
  }

  private List<LoadBalancer.ServerAndLoad> randomServers(int numServers, int numRegionsPerServer) {
    List<LoadBalancer.ServerAndLoad> servers =
      new ArrayList<LoadBalancer.ServerAndLoad>(numServers);
    for (int i = 0; i < numServers; i++) {
      servers.add(randomServer(numRegionsPerServer));
    }
    return servers;
  }

  private void returnServer(ServerName server) {
    serverQueue.add(server);
  }

  private void returnServers(List<ServerName> servers) {
    this.serverQueue.addAll(servers);
  }
}
