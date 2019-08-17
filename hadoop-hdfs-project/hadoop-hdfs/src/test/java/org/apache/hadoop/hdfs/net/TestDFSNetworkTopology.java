/**
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
package org.apache.hadoop.hdfs.net;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.Node;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the correctness of storage type info stored in
 * DFSNetworkTopology.
 */
public class TestDFSNetworkTopology {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDFSNetworkTopology.class);
  private final static DFSNetworkTopology CLUSTER =
      DFSNetworkTopology.getInstance(new Configuration());
  private DatanodeDescriptor[] dataNodes;

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @Before
  public void setupDatanodes() {
    final String[] racks = {
        "/l1/d1/r1", "/l1/d1/r1", "/l1/d1/r2", "/l1/d1/r2", "/l1/d1/r2",

        "/l1/d2/r3", "/l1/d2/r3", "/l1/d2/r3",

        "/l2/d3/r1", "/l2/d3/r2", "/l2/d3/r3", "/l2/d3/r4", "/l2/d3/r5",

        "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r1", "/l2/d4/r1",
        "/l2/d4/r1", "/l2/d4/r1"};
    final String[] hosts = {
        "host1", "host2", "host3", "host4", "host5",
        "host6", "host7", "host8",
        "host9", "host10", "host11", "host12", "host13",
        "host14", "host15", "host16", "host17", "host18", "host19", "host20"};
    final StorageType[] types = {
        StorageType.ARCHIVE, StorageType.DISK, StorageType.ARCHIVE,
        StorageType.DISK, StorageType.DISK,

        StorageType.DISK, StorageType.RAM_DISK, StorageType.SSD,

        StorageType.DISK, StorageType.RAM_DISK, StorageType.DISK,
        StorageType.ARCHIVE, StorageType.ARCHIVE,

        StorageType.DISK, StorageType.DISK, StorageType.RAM_DISK,
        StorageType.RAM_DISK, StorageType.ARCHIVE, StorageType.ARCHIVE,
        StorageType.SSD};
    final DatanodeStorageInfo[] storages =
        DFSTestUtil.createDatanodeStorageInfos(20, racks, hosts, types);
    dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);
    for (int i = 0; i < dataNodes.length; i++) {
      CLUSTER.add(dataNodes[i]);
    }
    dataNodes[9].setDecommissioned();
    dataNodes[10].setDecommissioned();
  }

  /**
   * Test getting the storage type info of subtree.
   * @throws Exception
   */
  @Test
  public void testGetStorageTypeInfo() throws Exception {
    // checking level = 2 nodes
    DFSTopologyNodeImpl d1 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1/d1");
    HashMap<String, EnumMap<StorageType, Integer>> d1info =
        d1.getChildrenStorageInfo();
    assertEquals(2, d1info.keySet().size());
    assertTrue(d1info.get("r1").size() == 2 && d1info.get("r2").size() == 2);
    assertEquals(1, (int)d1info.get("r1").get(StorageType.DISK));
    assertEquals(1, (int)d1info.get("r1").get(StorageType.ARCHIVE));
    assertEquals(2, (int)d1info.get("r2").get(StorageType.DISK));
    assertEquals(1, (int)d1info.get("r2").get(StorageType.ARCHIVE));

    DFSTopologyNodeImpl d2 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1/d2");
    HashMap<String, EnumMap<StorageType, Integer>> d2info =
        d2.getChildrenStorageInfo();
    assertEquals(1, d2info.keySet().size());
    assertTrue(d2info.get("r3").size() == 3);
    assertEquals(1, (int)d2info.get("r3").get(StorageType.DISK));
    assertEquals(1, (int)d2info.get("r3").get(StorageType.RAM_DISK));
    assertEquals(1, (int)d2info.get("r3").get(StorageType.SSD));

    DFSTopologyNodeImpl d3 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l2/d3");
    HashMap<String, EnumMap<StorageType, Integer>> d3info =
        d3.getChildrenStorageInfo();
    assertEquals(5, d3info.keySet().size());
    assertEquals(1, (int)d3info.get("r1").get(StorageType.DISK));
    assertEquals(1, (int)d3info.get("r2").get(StorageType.RAM_DISK));
    assertEquals(1, (int)d3info.get("r3").get(StorageType.DISK));
    assertEquals(1, (int)d3info.get("r4").get(StorageType.ARCHIVE));
    assertEquals(1, (int)d3info.get("r5").get(StorageType.ARCHIVE));

    DFSTopologyNodeImpl d4 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l2/d4");
    HashMap<String, EnumMap<StorageType, Integer>> d4info =
        d4.getChildrenStorageInfo();
    assertEquals(1, d4info.keySet().size());
    assertEquals(2, (int)d4info.get("r1").get(StorageType.DISK));
    assertEquals(2, (int)d4info.get("r1").get(StorageType.RAM_DISK));
    assertEquals(2, (int)d4info.get("r1").get(StorageType.ARCHIVE));
    assertEquals(1, (int)d4info.get("r1").get(StorageType.SSD));

    DFSTopologyNodeImpl l1 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1");
    HashMap<String, EnumMap<StorageType, Integer>> l1info =
        l1.getChildrenStorageInfo();
    assertEquals(2, l1info.keySet().size());
    assertTrue(l1info.get("d1").size() == 2
        && l1info.get("d2").size() == 3);
    assertEquals(2, (int)l1info.get("d1").get(StorageType.ARCHIVE));
    assertEquals(3, (int)l1info.get("d1").get(StorageType.DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.RAM_DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.SSD));

    // checking level = 1 nodes
    DFSTopologyNodeImpl l2 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l2");
    HashMap<String, EnumMap<StorageType, Integer>> l2info =
        l2.getChildrenStorageInfo();
    assertTrue(l2info.get("d3").size() == 3
        && l2info.get("d4").size() == 4);
    assertEquals(2, l2info.keySet().size());
    assertEquals(2, (int)l2info.get("d3").get(StorageType.DISK));
    assertEquals(2, (int)l2info.get("d3").get(StorageType.ARCHIVE));
    assertEquals(1, (int)l2info.get("d3").get(StorageType.RAM_DISK));
    assertEquals(2, (int)l2info.get("d4").get(StorageType.DISK));
    assertEquals(2, (int)l2info.get("d4").get(StorageType.ARCHIVE));
    assertEquals(2, (int)l2info.get("d4").get(StorageType.RAM_DISK));
    assertEquals(1, (int)l2info.get("d4").get(StorageType.SSD));
  }

  /**
   * Test the correctness of storage type info when nodes are added and removed.
   * @throws Exception
   */
  @Test
  public void testAddAndRemoveTopology() throws Exception {
    String[] newRack = {"/l1/d1/r1", "/l1/d1/r3", "/l1/d3/r3", "/l1/d3/r3"};
    String[] newHost = {"nhost1", "nhost2", "nhost3", "nhost4"};
    String[] newips = {"30.30.30.30", "31.31.31.31", "32.32.32.32",
        "33.33.33.33"};
    StorageType[] newTypes = {StorageType.DISK, StorageType.SSD,
        StorageType.SSD, StorageType.SSD};
    DatanodeDescriptor[] newDD = new DatanodeDescriptor[4];

    for (int i = 0; i<4; i++) {
      DatanodeStorageInfo dsi = DFSTestUtil.createDatanodeStorageInfo(
          "s" + newHost[i], newips[i], newRack[i], newHost[i],
          newTypes[i], null);
      newDD[i] = dsi.getDatanodeDescriptor();
      CLUSTER.add(newDD[i]);
    }

    DFSTopologyNodeImpl d1 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1/d1");
    HashMap<String, EnumMap<StorageType, Integer>> d1info =
        d1.getChildrenStorageInfo();
    assertEquals(3, d1info.keySet().size());
    assertTrue(d1info.get("r1").size() == 2 && d1info.get("r2").size() == 2
      && d1info.get("r3").size() == 1);
    assertEquals(2, (int)d1info.get("r1").get(StorageType.DISK));
    assertEquals(1, (int)d1info.get("r1").get(StorageType.ARCHIVE));
    assertEquals(2, (int)d1info.get("r2").get(StorageType.DISK));
    assertEquals(1, (int)d1info.get("r2").get(StorageType.ARCHIVE));
    assertEquals(1, (int)d1info.get("r3").get(StorageType.SSD));

    DFSTopologyNodeImpl d3 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1/d3");
    HashMap<String, EnumMap<StorageType, Integer>> d3info =
        d3.getChildrenStorageInfo();
    assertEquals(1, d3info.keySet().size());
    assertTrue(d3info.get("r3").size() == 1);
    assertEquals(2, (int)d3info.get("r3").get(StorageType.SSD));

    DFSTopologyNodeImpl l1 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1");
    HashMap<String, EnumMap<StorageType, Integer>> l1info =
        l1.getChildrenStorageInfo();
    assertEquals(3, l1info.keySet().size());
    assertTrue(l1info.get("d1").size() == 3 &&
        l1info.get("d2").size() == 3 && l1info.get("d3").size() == 1);
    assertEquals(4, (int)l1info.get("d1").get(StorageType.DISK));
    assertEquals(2, (int)l1info.get("d1").get(StorageType.ARCHIVE));
    assertEquals(1, (int)l1info.get("d1").get(StorageType.SSD));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.SSD));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.RAM_DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.DISK));
    assertEquals(2, (int)l1info.get("d3").get(StorageType.SSD));

    for (int i = 0; i<4; i++) {
      CLUSTER.remove(newDD[i]);
    }

    // /d1/r3 should've been out, /d1/r1 should've been resumed
    DFSTopologyNodeImpl nd1 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1/d1");
    HashMap<String, EnumMap<StorageType, Integer>> nd1info =
        nd1.getChildrenStorageInfo();
    assertEquals(2, nd1info.keySet().size());
    assertTrue(nd1info.get("r1").size() == 2 && nd1info.get("r2").size() == 2);
    assertEquals(1, (int)nd1info.get("r1").get(StorageType.DISK));
    assertEquals(1, (int)nd1info.get("r1").get(StorageType.ARCHIVE));
    assertEquals(2, (int)nd1info.get("r2").get(StorageType.DISK));
    assertEquals(1, (int)nd1info.get("r2").get(StorageType.ARCHIVE));

    // /l1/d3 should've been out, and /l1/d1 should've been resumed
    DFSTopologyNodeImpl nl1 =
        (DFSTopologyNodeImpl) CLUSTER.getNode("/l1");
    HashMap<String, EnumMap<StorageType, Integer>> nl1info =
        nl1.getChildrenStorageInfo();
    assertEquals(2, nl1info.keySet().size());
    assertTrue(l1info.get("d1").size() == 2
        && l1info.get("d2").size() == 3);
    assertEquals(2, (int)nl1info.get("d1").get(StorageType.ARCHIVE));
    assertEquals(3, (int)nl1info.get("d1").get(StorageType.DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.RAM_DISK));
    assertEquals(1, (int)l1info.get("d2").get(StorageType.SSD));

    assertNull(CLUSTER.getNode("/l1/d3"));
  }

  @Test
  public void testChooseRandomWithStorageType() throws Exception {
    Node n;
    DatanodeDescriptor dd;
    // test the choose random can return desired storage type nodes without
    // exclude
    Set<String> diskUnderL1 =
        Sets.newHashSet("host2", "host4", "host5", "host6");
    Set<String> archiveUnderL1 = Sets.newHashSet("host1", "host3");
    Set<String> ramdiskUnderL1 = Sets.newHashSet("host7");
    Set<String> ssdUnderL1 = Sets.newHashSet("host8");
    for (int i = 0; i < 10; i++) {
      n = CLUSTER.chooseRandomWithStorageType("/l1", null, null,
          StorageType.DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(diskUnderL1.contains(dd.getHostName()));

      n = CLUSTER.chooseRandomWithStorageType("/l1", null, null,
          StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(ramdiskUnderL1.contains(dd.getHostName()));

      n = CLUSTER.chooseRandomWithStorageType("/l1", null, null,
          StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(archiveUnderL1.contains(dd.getHostName()));

      n = CLUSTER.chooseRandomWithStorageType("/l1", null, null,
          StorageType.SSD);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(ssdUnderL1.contains(dd.getHostName()));
    }
  }

  @Test
  public void testChooseRandomWithStorageTypeWithExcluded() throws Exception {
    Node n;
    DatanodeDescriptor dd;
    // below test choose random with exclude, for /l2/d3, every rack has exactly
    // one host
    // /l2/d3 has five racks r[1~5] but only r4 and r5 have ARCHIVE
    // host12 is the one under "/l2/d3/r4", host13 is the one under "/l2/d3/r5"
    n = CLUSTER.chooseRandomWithStorageType("/l2/d3/r4", null, null,
        StorageType.ARCHIVE);
    HashSet<Node> excluded = new HashSet<>();
    // exclude the host on r4 (since there is only one host, no randomness here)
    excluded.add(n);

    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType("/l2/d3", null, null,
          StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host12") ||
          dd.getHostName().equals("host13"));
    }

    // test exclude nodes
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType("/l2/d3", null, excluded,
          StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host13"));
    }

    // test exclude scope
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType("/l2/d3", "/l2/d3/r4", null,
          StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host13"));
    }

    // test exclude scope + excluded node with expected null return node
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType("/l2/d3", "/l2/d3/r5", excluded,
          StorageType.ARCHIVE);
      assertNull(n);
    }

    // test exclude scope + excluded node with expected non-null return node
    n = CLUSTER.chooseRandomWithStorageType("/l1/d2", null, null,
        StorageType.DISK);
    dd = (DatanodeDescriptor)n;
    assertEquals("host6", dd.getHostName());
    // exclude the host on r4 (since there is only one host, no randomness here)
    excluded.add(n);
    Set<String> expectedSet = Sets.newHashSet("host4", "host5");
    for (int i = 0; i<10; i++) {
      // under l1, there are four hosts with DISK:
      // /l1/d1/r1/host2, /l1/d1/r2/host4, /l1/d1/r2/host5 and /l1/d2/r3/host6
      // host6 is excludedNode, host2 is under excluded range scope /l1/d1/r1
      // so should always return r4 or r5
      n = CLUSTER.chooseRandomWithStorageType(
          "/l1", "/l1/d1/r1", excluded, StorageType.DISK);
      dd = (DatanodeDescriptor) n;
      assertTrue(expectedSet.contains(dd.getHostName()));
    }
  }


  /**
   * This test tests the wrapper method. The wrapper method only takes one scope
   * where if it starts with a ~, it is an excluded scope, and searching always
   * from root. Otherwise it is a scope.
   * @throws Exception throws exception.
   */
  @Test
  public void testChooseRandomWithStorageTypeWrapper() throws Exception {
    Node n;
    DatanodeDescriptor dd;
    n = CLUSTER.chooseRandomWithStorageType("/l2/d3/r4", null, null,
        StorageType.ARCHIVE);
    HashSet<Node> excluded = new HashSet<>();
    // exclude the host on r4 (since there is only one host, no randomness here)
    excluded.add(n);

    // search with given scope being desired scope
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "/l2/d3", null, StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host12") ||
          dd.getHostName().equals("host13"));
    }

    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "/l2/d3", excluded, StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host13"));
    }

    // search with given scope being exclude scope

    // a total of 4 ramdisk nodes:
    // /l1/d2/r3/host7, /l2/d3/r2/host10, /l2/d4/r1/host7 and /l2/d4/r1/host10
    // so if we exclude /l2/d4/r1, if should be always either host7 or host10
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "~/l2/d4", null, StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host7") ||
          dd.getHostName().equals("host10"));
    }

    // similar to above, except that we also exclude host10 here. so it should
    // always be host7
    n = CLUSTER.chooseRandomWithStorageType("/l2/d3/r2", null, null,
        StorageType.RAM_DISK);
    // add host10 to exclude
    excluded.add(n);
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "~/l2/d4", excluded, StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host7"));
    }
  }

  @Test
  public void testNonExistingNode() throws Exception {
    Node n;
    n = CLUSTER.chooseRandomWithStorageType(
        "/l100", null, null, StorageType.DISK);
    assertNull(n);
    n = CLUSTER.chooseRandomWithStorageType(
        "/l100/d100", null, null, StorageType.DISK);
    assertNull(n);
    n = CLUSTER.chooseRandomWithStorageType(
        "/l100/d100/r100", null, null, StorageType.DISK);
    assertNull(n);
  }

  /**
   * Tests getting subtree storage counts, and see whether it is correct when
   * we update subtree.
   * @throws Exception
   */
  @Test
  public void testGetSubtreeStorageCount() throws Exception {
    // add and remove a node to rack /l2/d3/r1. So all the inner nodes /l2,
    // /l2/d3 and /l2/d3/r1 should be affected. /l2/d3/r3 should still be the
    // same, only checked as a reference
    Node l2 = CLUSTER.getNode("/l2");
    Node l2d3 = CLUSTER.getNode("/l2/d3");
    Node l2d3r1 = CLUSTER.getNode("/l2/d3/r1");
    Node l2d3r3 = CLUSTER.getNode("/l2/d3/r3");

    assertTrue(l2 instanceof DFSTopologyNodeImpl);
    assertTrue(l2d3 instanceof DFSTopologyNodeImpl);
    assertTrue(l2d3r1 instanceof DFSTopologyNodeImpl);
    assertTrue(l2d3r3 instanceof DFSTopologyNodeImpl);

    DFSTopologyNodeImpl innerl2 = (DFSTopologyNodeImpl)l2;
    DFSTopologyNodeImpl innerl2d3 = (DFSTopologyNodeImpl)l2d3;
    DFSTopologyNodeImpl innerl2d3r1 = (DFSTopologyNodeImpl)l2d3r1;
    DFSTopologyNodeImpl innerl2d3r3 = (DFSTopologyNodeImpl)l2d3r3;

    assertEquals(4,
        innerl2.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(2,
        innerl2d3.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(1,
        innerl2d3r1.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(1,
        innerl2d3r3.getSubtreeStorageCount(StorageType.DISK));

    DatanodeStorageInfo storageInfo =
        DFSTestUtil.createDatanodeStorageInfo("StorageID",
            "1.2.3.4", "/l2/d3/r1", "newhost");
    DatanodeDescriptor newNode = storageInfo.getDatanodeDescriptor();
    CLUSTER.add(newNode);

    // after adding a storage to /l2/d3/r1, ancestor inner node should have
    // DISK count incremented by 1.
    assertEquals(5,
        innerl2.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(3,
        innerl2d3.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(2,
        innerl2d3r1.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(1,
        innerl2d3r3.getSubtreeStorageCount(StorageType.DISK));

    CLUSTER.remove(newNode);

    assertEquals(4,
        innerl2.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(2,
        innerl2d3.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(1,
        innerl2d3r1.getSubtreeStorageCount(StorageType.DISK));
    assertEquals(1,
        innerl2d3r3.getSubtreeStorageCount(StorageType.DISK));
  }

  @Test
  public void testChooseRandomWithStorageTypeTwoTrial() throws Exception {
    Node n;
    DatanodeDescriptor dd;
    n = CLUSTER.chooseRandomWithStorageType("/l2/d3/r4", null, null,
        StorageType.ARCHIVE);
    HashSet<Node> excluded = new HashSet<>();
    // exclude the host on r4 (since there is only one host, no randomness here)
    excluded.add(n);

    // search with given scope being desired scope
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "/l2/d3", null, StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host12") ||
          dd.getHostName().equals("host13"));
    }

    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "/l2/d3", excluded, StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host13"));
    }

    // search with given scope being exclude scope

    // a total of 4 ramdisk nodes:
    // /l1/d2/r3/host7, /l2/d3/r2/host10, /l2/d4/r1/host7 and /l2/d4/r1/host10
    // so if we exclude /l2/d4/r1, if should be always either host7 or host10
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "~/l2/d4", null, StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host7") ||
          dd.getHostName().equals("host10"));
    }

    // similar to above, except that we also exclude host10 here. so it should
    // always be host7
    n = CLUSTER.chooseRandomWithStorageType("/l2/d3/r2", null, null,
        StorageType.RAM_DISK);
    // add host10 to exclude
    excluded.add(n);
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "~/l2/d4", excluded, StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host7"));
    }
  }
}
