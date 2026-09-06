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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end test that verifies {@link DatanodeAffinityManager} drives block
 * placement to the correct DataNodes in a 10-node MiniDFSCluster.
 *
 * <h3>Scenario</h3>
 * <ol>
 *   <li>Start a {@link MiniDFSCluster} with <b>10 DataNodes</b>.</li>
 *   <li>Once the cluster is active, enumerate all registered DataNodes and
 *       sort them by transfer port.  The first three become the
 *       <em>affinity group</em>; the remaining seven are the
 *       <em>non-affinity group</em>.</li>
 *   <li>Write a {@link FileDatanodeAffinityManager} JSON file whose
 *       {@code datanodeRegex} exactly matches the three affinity DataNodes
 *       by their {@code "hostname:port"} addresses, and whose
 *       {@code regexPattern} covers {@code /tenant-data/}.</li>
 *   <li>Trigger {@link DatanodeAffinityManager#refresh()} so the resolved
 *       map is populated.</li>
 *   <li>Create <b>5 files</b> under {@code /tenant-data/} each with
 *       replication factor 3.</li>
 *   <li>Assert that <b>every replica of every block</b> lives on one of the
 *       three affinity DataNodes — no non-affinity node should appear.</li>
 * </ol>
 *
 * <p>Because all DataNodes share hostname {@code 127.0.0.1} in
 * MiniDFSCluster, the {@code datanodeRegex} matches against the full
 * {@code "hostname:port"} string (the format used by
 * {@link DatanodeAffinityManager#refresh()}) and uses the transfer port to
 * discriminate between DataNodes.
 */
public class TestDatanodeAffinityBlockPlacement {

  private static final int NUM_DATANODES  = 10;
  private static final int AFFINITY_COUNT = 3;
  private static final short REPLICATION  = 3;
  private static final int NUM_FILES      = 5;
  private static final int FILE_SIZE      = 1024;  // bytes — fits in one block
  private static final String AFFINITY_DIR = "/tenant-data";
  private static final String OTHER_DIR    = "/other-data";

  private MiniDFSCluster cluster;
  private Configuration conf;
  private File affinityJsonFile;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();

    // Create a placeholder JSON file (empty array) so the affinity manager
    // does not throw on first refresh() during cluster startup.
    affinityJsonFile = File.createTempFile("dn-affinity-e2e", ".json",
        GenericTestUtils.getTestDir());
    affinityJsonFile.deleteOnExit();
    writeJson("[]");

    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MANAGER_CLASSNAME_KEY,
        FileDatanodeAffinityManager.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY,
        affinityJsonFile.getAbsolutePath());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (affinityJsonFile != null) {
      affinityJsonFile.delete();
    }
  }

  // ---------------------------------------------------------------------------
  // Main E2E test
  // ---------------------------------------------------------------------------

  /**
   * Verifies that with a 10-node cluster and an affinity rule targeting 3
   * DataNodes, every replica of every block created under the affinity path
   * lands on one of those 3 DataNodes only.
   */
  @Test
  public void testBlocksPlacedOnlyOnAffinityDatanodes() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    // 1. Collect all registered DataNodes and sort by xfer port for
    //    determinism across test runs.
    List<DatanodeDescriptor> allDNs = new ArrayList<>(dnManager.getAllDatanodes());
    assertEquals(NUM_DATANODES, allDNs.size(), "Expected 10 DataNodes");
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));

    // 2. Pick the first AFFINITY_COUNT DataNodes as the affinity group.
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);
    List<DatanodeDescriptor> nonAffinityDNs =
        allDNs.subList(AFFINITY_COUNT, allDNs.size());

    // Build xferAddr (IP:port) sets for assertion — must match what
    // DatanodeAffinityManager.refresh() stores via dn.getXferAddr().
    Set<String> affinityAddrs = affinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    Set<String> nonAffinityAddrs = nonAffinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());

    assertEquals(AFFINITY_COUNT, affinityAddrs.size());
    assertEquals(NUM_DATANODES - AFFINITY_COUNT, nonAffinityAddrs.size());

    // 3. Build datanodeRegex that matches exactly the AFFINITY_COUNT nodes by
    //    their "hostname:port" address.  We use Pattern.quote() to escape any
    //    special characters in the address strings.
    String datanodeRegex = affinityAddrs.stream()
        .map(addr -> "(" + escapeForRegex(addr) + ")")
        .collect(Collectors.joining("|"));

    // 4. Write the JSON affinity file targeting AFFINITY_DIR.
    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));

    // 5. Re-trigger refresh() so the manager resolves the regex against now-
    //    registered DataNodes and populates fileRegexToDataNodeMap.
    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    assertNotNull(affinityManager, "DatanodeAffinityManager must be configured");
    affinityManager.refresh();

    // Verify the map now contains exactly AFFINITY_COUNT resolved nodes.
    List<String> resolved =
        affinityManager.getFileRegexToDataNodeMap()
            .get("^" + AFFINITY_DIR + "/.*");
    assertNotNull(resolved,
        "Affinity map must contain entry for " + AFFINITY_DIR);
    assertEquals(AFFINITY_COUNT, resolved.size(),
        "Affinity map must list exactly " + AFFINITY_COUNT + " DataNodes");
    assertTrue(new HashSet<>(resolved).equals(affinityAddrs),
        "Resolved addresses must be a subset of affinityAddrs");

    // 6. Create NUM_FILES files under the affinity directory.
    DistributedFileSystem dfs = cluster.getFileSystem();
    List<Path> createdFiles = new ArrayList<>();
    for (int i = 0; i < NUM_FILES; i++) {
      Path p = new Path(AFFINITY_DIR + "/file-" + i + ".parquet");
      DFSTestUtil.createFile(dfs, p, FILE_SIZE, REPLICATION, i /* seed */);
      createdFiles.add(p);
    }

    // Wait for all replicas to be placed.
    for (Path p : createdFiles) {
      DFSTestUtil.waitReplication(dfs, p, REPLICATION);
    }

    // 7. Assert every replica of every block is on an affinity DataNode.
    for (Path filePath : createdFiles) {
      long fileLen = dfs.getFileStatus(filePath).getLen();
      LocatedBlocks locatedBlocks = dfs.getClient()
          .getLocatedBlocks(filePath.toString(), 0, fileLen);

      assertFalse(locatedBlocks.getLocatedBlocks().isEmpty(),
          "File " + filePath + " has no blocks");

      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        DatanodeInfo[] locations = lb.getLocations();
        assertEquals(REPLICATION, locations.length,
            "Block must have " + REPLICATION + " replicas");

        for (DatanodeInfo dn : locations) {
          String hostPort = dn.getXferAddr();
          assertFalse(nonAffinityAddrs.contains(hostPort),
              "Block replica found on non-affinity node " + hostPort
                  + " for file " + filePath);
          assertTrue(affinityAddrs.contains(hostPort),
              "Block replica " + hostPort + " not in affinity set for "
                  + filePath);
        }
      }
    }
  }

  /**
   * Verifies that files created outside the affinity directory are NOT
   * restricted to the affinity DataNodes — the non-affinity DataNodes
   * participate in placement.
   */
  @Test
  public void testBlocksOutsideAffinityDirUseAllDatanodes() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    // Same setup: configure affinity for AFFINITY_DIR only.
    List<DatanodeDescriptor> allDNs = new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);

    Set<String> affinityAddrs = affinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    String datanodeRegex = affinityAddrs.stream()
        .map(addr -> "(" + escapeForRegex(addr) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    dnManager.getDatanodeAffinityManager().refresh();

    // Create files under a DIFFERENT directory (no affinity rule).
    DistributedFileSystem dfs = cluster.getFileSystem();
    Set<String> allAddrs = allDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    Set<String> seenHosts = new HashSet<>();

    for (int i = 0; i < 20; i++) {
      Path p = new Path(OTHER_DIR + "/file-" + i + ".parquet");
      DFSTestUtil.createFile(dfs, p, FILE_SIZE, REPLICATION, i);
      DFSTestUtil.waitReplication(dfs, p, REPLICATION);

      long fileLen = dfs.getFileStatus(p).getLen();
      LocatedBlocks lb = dfs.getClient()
          .getLocatedBlocks(p.toString(), 0, fileLen);
      for (LocatedBlock block : lb.getLocatedBlocks()) {
        for (DatanodeInfo dn : block.getLocations()) {
          seenHosts.add(dn.getXferAddr());
        }
      }
    }

    // After 20 files × 3 replicas, at least one non-affinity DataNode must
    // have received a block — confirming no restriction outside AFFINITY_DIR.
    Set<String> seenNonAffinity = new HashSet<>(seenHosts);
    seenNonAffinity.removeAll(affinityAddrs);
    assertFalse(seenNonAffinity.isEmpty(),
        "Non-affinity DataNodes should receive blocks for files outside "
            + AFFINITY_DIR + "; seenHosts=" + seenHosts);
  }

  /**
   * Regression test for the DataNode re-registration isolation breach:
   * {@link DatanodeAffinityManager#onDatanodeRegistered} must report
   * {@code true} for a node that matches an affinity group EVEN WHEN the node
   * is already tracked (i.e. on a re-registration / restart).  Previously it
   * only returned {@code true} the first time the address was inserted, so a
   * restarted isolated DataNode was silently re-added to the default topology
   * and leaked back into default block placement.
   */
  @Test
  public void testReRegistrationKeepsNodeIsolated() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs = new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);
    List<DatanodeDescriptor> nonAffinityDNs =
        allDNs.subList(AFFINITY_COUNT, allDNs.size());

    Set<String> affinityAddrs = affinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    String datanodeRegex = affinityAddrs.stream()
        .map(addr -> "(" + escapeForRegex(addr) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    // refresh() resolves the regex and already tracks the affinity nodes, so
    // any subsequent onDatanodeRegistered() call is effectively a
    // "re-registration" of an already-tracked address.
    affinityManager.refresh();

    for (DatanodeDescriptor dn : affinityDNs) {
      assertTrue(affinityManager.onDatanodeRegistered(dn),
          "Affinity node " + dn.getXferAddr()
              + " must report as isolated on re-registration "
              + "(already tracked)");
    }

    for (DatanodeDescriptor dn : nonAffinityDNs) {
      assertFalse(affinityManager.onDatanodeRegistered(dn), "Non-affinity node " + dn.getXferAddr()
              + " must not report as isolated");
    }
  }

  /**
   * When an isolated DataNode is removed (decommission / death), it must be
   * pruned from every affinity structure so a stale, unreachable node is no
   * longer offered as an affinity placement target. Exercises
   * {@link DatanodeAffinityManager#onDatanodeRemoved}.
   */
  @Test
  public void testRemovedNodePrunedFromAffinity() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs = new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);

    Set<String> affinityAddrs = affinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    String datanodeRegex = affinityAddrs.stream()
        .map(addr -> "(" + escapeForRegex(addr) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    affinityManager.refresh();

    // Register all affinity nodes so they populate the per-group topology.
    for (DatanodeDescriptor dn : affinityDNs) {
      affinityManager.onDatanodeRegistered(dn);
    }

    DatanodeDescriptor removed = affinityDNs.get(0);
    String removedAddr = removed.getXferAddr();
    assertTrue(affinityManager.getIsolatedDatanodes().contains(removedAddr),
        "Node should be isolated before removal");

    affinityManager.onDatanodeRemoved(removed);

    assertFalse(affinityManager.getIsolatedDatanodes().contains(removedAddr),
        "Removed node must be gone from the isolated-pool set");
    boolean stillInAnyList = affinityManager.getFileRegexToDataNodeMap()
        .values().stream().anyMatch(l -> l.contains(removedAddr));
    assertFalse(stillInAnyList, "Removed node must be pruned from every per-group list");
    // Surviving affinity nodes must remain isolated.
    for (DatanodeDescriptor dn : affinityDNs.subList(1, affinityDNs.size())) {
      assertTrue(affinityManager.getIsolatedDatanodes().contains(
          dn.getXferAddr()), "Surviving affinity node must stay isolated");
    }

    // Idempotent: removing again (or removing a non-affinity node) is a no-op.
    affinityManager.onDatanodeRemoved(removed);
    assertFalse(affinityManager.getIsolatedDatanodes().contains(removedAddr));
  }

  /**
   * With {@code dfs.namenode.affinity.strict.isolation.enabled=true}, a write to
   * an affinity path whose group cannot satisfy the replicas (here the group's
   * datanodeRegex matches no live DataNode, so the isolated pool is empty) must
   * FAIL rather than spill over to the shared pool. A companion write to a
   * non-affinity path must still succeed.
   */
  @Test
  public void testStrictIsolationFailsWhenGroupCannotSatisfy()
      throws Exception {
    // Rebuild the cluster with strict isolation enabled.
    cluster.shutdown();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AFFINITY_STRICT_ISOLATION_KEY, true);
    // Affinity group whose datanodeRegex matches NO datanode in the cluster,
    // so the isolated pool is empty and cannot satisfy any replica.
    writeJson(buildAffinityJson("tenant-empty",
        "^" + AFFINITY_DIR + "/.*", "^host-that-does-not-exist:0$"));
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();

    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();
    // refresh() rebuilds the (empty) affinity placement group.
    dnManager.getDatanodeAffinityManager().refresh();

    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      DFSTestUtil.createFile(fs, new Path(AFFINITY_DIR + "/strict.dat"),
          FILE_SIZE, REPLICATION, 0L);
      fail("Write to an unsatisfiable affinity group must fail under strict "
          + "isolation instead of spilling over to the shared pool");
    } catch (IOException expected) {
      // Expected: strict isolation fails the write.
    }

    // A write outside the affinity path must still succeed (strict isolation
    // must not break normal, non-isolated writes).
    DFSTestUtil.createFile(fs, new Path(OTHER_DIR + "/normal.dat"),
        FILE_SIZE, REPLICATION, 0L);
  }

  /**
   * Sanity check: with strict isolation DISABLED (default), the same
   * unsatisfiable affinity group falls back to the shared pool so the write
   * still succeeds (availability over isolation).
   */
  @Test
  public void testNonStrictIsolationFallsBackToSharedPool() throws Exception {
    cluster.shutdown();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AFFINITY_STRICT_ISOLATION_KEY, false);
    writeJson(buildAffinityJson("tenant-empty",
        "^" + AFFINITY_DIR + "/.*", "^host-that-does-not-exist:0$"));
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();

    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();
    dnManager.getDatanodeAffinityManager().refresh();

    DistributedFileSystem fs = cluster.getFileSystem();
    // Must succeed via fallback to the default (shared-pool) placement policy.
    DFSTestUtil.createFile(fs, new Path(AFFINITY_DIR + "/fallback.dat"),
        FILE_SIZE, REPLICATION, 0L);
  }

  /**
   * Under strict isolation, a group that is PARTIALLY provisioned (fewer live
   * DataNodes than the requested replication, but more than minReplication)
   * must also fail the write. Otherwise the block would be created
   * under-replicated and the redundancy monitor would later place the missing
   * replicas on shared-pool nodes via the default policy -- an isolation leak.
   * This exercises the "&gt;= numOfReplicas" (not "&gt;= minReplication")
   * sufficiency threshold.
   */
  @Test
  public void testStrictIsolationFailsWhenGroupPartiallyProvisioned()
      throws Exception {
    cluster.shutdown();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AFFINITY_STRICT_ISOLATION_KEY, true);
    // Start with an empty group so the cluster comes up normally.
    writeJson("[]");
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();

    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    // Pick only 2 DataNodes for the group -- fewer than REPLICATION (3).
    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> groupDNs = allDNs.subList(0, REPLICATION - 1);
    String datanodeRegex = groupDNs.stream()
        .map(dn -> "(" + escapeForRegex(dn.getXferAddr()) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("tenant-partial",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    dnManager.getDatanodeAffinityManager().refresh();

    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      DFSTestUtil.createFile(fs, new Path(AFFINITY_DIR + "/partial.dat"),
          FILE_SIZE, REPLICATION, 0L);
      fail("Write to a partially-provisioned affinity group (only "
          + (REPLICATION - 1) + " of " + REPLICATION + " replicas placeable "
          + "in-group) must fail under strict isolation");
    } catch (IOException expected) {
      // Expected: strict isolation requires ALL replicas in-group.
    }
  }

  /**
   * Under NON-strict (availability) isolation, a group that is PARTIALLY
   * provisioned (fewer live DataNodes than the requested replication) must
   * spill the WHOLE block to the shared pool via the default policy, so the
   * block is fully replicated at write time. A partial in-group placement
   * (only >= minReplication) would instead create the block under-replicated
   * and rely on the asynchronous redundancy monitor to repair the remainder,
   * leaving a window of under-replication. This test verifies that none of the
   * replicas land on the under-provisioned group's nodes -- i.e. the fallback
   * is all-or-nothing, not partial.
   */
  @Test
  public void testNonStrictPartialGroupSpillsWholeBlockToSharedPool()
      throws Exception {
    cluster.shutdown();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AFFINITY_STRICT_ISOLATION_KEY, false);
    // Start with an empty group so the cluster comes up normally.
    writeJson("[]");
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();

    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    // Pick only 2 DataNodes for the group -- fewer than REPLICATION (3).
    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> groupDNs = allDNs.subList(0, REPLICATION - 1);
    Set<String> groupAddrs = groupDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    String datanodeRegex = groupDNs.stream()
        .map(dn -> "(" + escapeForRegex(dn.getXferAddr()) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("tenant-partial-nonstrict",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    dnManager.getDatanodeAffinityManager().refresh();

    DistributedFileSystem fs = cluster.getFileSystem();
    Path filePath = new Path(AFFINITY_DIR + "/partial-nonstrict.dat");
    // Must succeed via full fallback to the default (shared-pool) policy.
    DFSTestUtil.createFile(fs, filePath, FILE_SIZE, REPLICATION, 0L);
    DFSTestUtil.waitReplication(fs, filePath, REPLICATION);

    long fileLen = fs.getFileStatus(filePath).getLen();
    LocatedBlocks locatedBlocks =
        fs.getClient().getLocatedBlocks(filePath.toString(), 0, fileLen);
    assertFalse(locatedBlocks.getLocatedBlocks().isEmpty(), "File has no blocks");
    for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
      DatanodeInfo[] locations = lb.getLocations();
      assertEquals(REPLICATION, locations.length,
          "Block must be fully replicated to " + REPLICATION);
      for (DatanodeInfo dn : locations) {
        assertFalse(groupAddrs.contains(dn.getXferAddr()),
            "Non-strict partial group must spill the WHOLE block to the "
                + "shared pool, but a replica landed on the "
                + "under-provisioned group node " + dn.getXferAddr());
      }
    }
  }

  /**
   * Regression (pipeline recovery, non-strict availability mode): a block whose
   * under-provisioned group spilled ENTIRELY to the shared pool has surviving
   * replicas that are NOT in the group topology. On pipeline recovery,
   * chooseTarget4AdditionalDatanode() must route through the DEFAULT policy and
   * return a replacement. Routing through the tiny group policy would count the
   * out-of-topology survivors against the group in getMaxNodesPerRack()
   * (numChosen + numAdditional exceeds the group leaf count), drive numAdditional
   * to 0, return no node, and -- with the default best-effort=false -- fail the
   * write. A positive control confirms that when survivors ARE in the group,
   * recovery still stays in-group.
   */
  @Test
  public void testAdditionalDatanodeForSpilledBlockUsesSharedPool()
      throws Exception {
    cluster.shutdown();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_AFFINITY_STRICT_ISOLATION_KEY, false);
    writeJson("[]");
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();

    BlockManager blockManager =
        cluster.getNameNode().getNamesystem().getBlockManager();
    DatanodeManager dnManager = blockManager.getDatanodeManager();
    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));

    // Group = first AFFINITY_COUNT nodes; the rest form the shared pool.
    List<DatanodeDescriptor> groupDNs = allDNs.subList(0, AFFINITY_COUNT);
    List<DatanodeDescriptor> sharedDNs =
        new ArrayList<>(allDNs.subList(AFFINITY_COUNT, allDNs.size()));
    Set<String> groupAddrs = groupDNs.stream()
        .map(DatanodeDescriptor::getXferAddr).collect(Collectors.toSet());
    String datanodeRegex = groupDNs.stream()
        .map(dn -> "(" + escapeForRegex(dn.getXferAddrWithHostname()) + ")")
        .collect(Collectors.joining("|"));
    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    dnManager.getDatanodeAffinityManager().refresh();

    byte storagePolicyID =
        blockManager.getStoragePolicySuite().getDefaultPolicy().getId();

    // A non-strict spilled block: its surviving pipeline replicas live on
    // shared-pool nodes (absent from the group topology).
    List<DatanodeStorageInfo> spilledChosen = new ArrayList<>();
    Set<Node> spilledExcludes = new HashSet<>();
    for (int i = 0; i < REPLICATION; i++) {
      DatanodeDescriptor dn = sharedDNs.get(i);
      spilledChosen.add(dn.getStorageInfos()[0]);
      spilledExcludes.add(dn);
    }
    DatanodeStorageInfo[] targets = blockManager.chooseTarget4AdditionalDatanode(
        AFFINITY_DIR + "/spilled.dat", 1, null, spilledChosen, spilledExcludes,
        FILE_SIZE, storagePolicyID, BlockType.CONTIGUOUS);
    assertNotNull(targets, "Recovery must return a target array");
    // chooseTarget4AdditionalDatanode returns the survivors too, so a successful
    // recovery yields chosen + 1; the old bug would have added zero (length ==
    // chosen size) and failed the write.
    assertEquals(REPLICATION + 1, targets.length,
        "Pipeline recovery for a shared-pool (spilled) block must add "
            + "exactly one replacement -- not fail the write by adding zero");
    for (DatanodeStorageInfo t : targets) {
      assertFalse(groupAddrs.contains(t.getDatanodeDescriptor().getXferAddr()),
          "A spilled block's pipeline (and its replacement) must stay in "
              + "the shared pool, never an affinity node");
    }

    // Positive control: when the survivors ARE in the group, recovery stays
    // in-group (routes through the affinity policy).
    List<DatanodeStorageInfo> inGroupChosen = new ArrayList<>();
    Set<Node> inGroupExcludes = new HashSet<>();
    for (int i = 0; i < AFFINITY_COUNT - 1; i++) {
      DatanodeDescriptor dn = groupDNs.get(i);
      inGroupChosen.add(dn.getStorageInfos()[0]);
      inGroupExcludes.add(dn);
    }
    DatanodeStorageInfo[] inGroupTargets =
        blockManager.chooseTarget4AdditionalDatanode(
            AFFINITY_DIR + "/ingroup.dat", 1, null, inGroupChosen,
            inGroupExcludes, FILE_SIZE, storagePolicyID, BlockType.CONTIGUOUS);
    assertNotNull(inGroupTargets);
    assertEquals(AFFINITY_COUNT, inGroupTargets.length,
        "In-group recovery must return survivors + one replacement");
    for (DatanodeStorageInfo t : inGroupTargets) {
      assertTrue(groupAddrs.contains(t.getDatanodeDescriptor().getXferAddr()),
          "In-group recovery (and its replacement) must stay within the "
              + "affinity group");
    }
  }

  /**
   * A malformed affinity record with a null/empty regex field must be skipped
   * with a warning without aborting the whole refresh (a null regex would
   * otherwise throw NullPointerException before the PatternSyntaxException
   * catch). The remaining valid group must still be applied.
   */
  @Test
  public void testRefreshSkipsRecordsWithNullRegex() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);
    String datanodeRegex = affinityDNs.stream()
        .map(dn -> "(" + escapeForRegex(dn.getXferAddr()) + ")")
        .collect(Collectors.joining("|"));

    // JSON with one malformed record (null regexPattern) followed by a valid
    // group. JSON string literals need backslashes doubled.
    String goodPath = ("^" + AFFINITY_DIR + "/.*").replace("\\", "\\\\");
    String goodDn = datanodeRegex.replace("\\", "\\\\");
    String json = "["
        + "{\"affinityGroupName\":\"bad\",\"regexPattern\":null,"
        + "\"datanodeRegex\":\"^unused:0$\"},"
        + String.format("{\"affinityGroupName\":\"good\","
            + "\"regexPattern\":\"%s\",\"datanodeRegex\":\"%s\"}",
            goodPath, goodDn)
        + "]";
    writeJson(json);

    DatanodeAffinityManager mgr = dnManager.getDatanodeAffinityManager();
    // Must not throw despite the null-regex record.
    mgr.refresh();

    // The valid "good" group must still have isolated its DataNodes.
    for (DatanodeDescriptor dn : affinityDNs) {
      assertTrue(mgr.getIsolatedDatanodes().contains(dn.getXferAddr()),
          "Valid group's DataNode " + dn.getXferAddr()
              + " must be isolated even though a sibling record was malformed");
    }
  }

  /**
   * Regression test for the concurrent-registration reconciliation race.
   *
   * <p>A DataNode that registers <em>during</em>
   * {@link DatanodeAffinityManager#internalRefresh()} -- after the rebuild
   * snapshot was taken but before the new structures are published -- is
   * absent from the point-in-time isolated-address set handed to
   * {@link DatanodeManager#postAffinityRefresh}. The old logic would then
   * "restore" that still-matching node to the default topology, leaking an
   * isolated node into shared-pool placement. The fix re-evaluates every live
   * DataNode against the current affinity patterns, so a still-matching node is
   * kept isolated (and re-inserted into its group) rather than restored.
   *
   * <p>We reproduce the stale-snapshot condition deterministically by invoking
   * {@code postAffinityRefresh} with a set that intentionally omits one
   * already-isolated affinity node, then assert it is NOT re-added to the
   * default topology.
   */
  @Test
  public void testConcurrentRegistrationDuringRefreshReconciled()
      throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);
    List<DatanodeDescriptor> nonAffinityDNs =
        allDNs.subList(AFFINITY_COUNT, allDNs.size());

    String datanodeRegex = affinityDNs.stream()
        .map(dn -> "(" + escapeForRegex(dn.getXferAddrWithHostname()) + ")")
        .collect(Collectors.joining("|"));
    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));

    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    affinityManager.refresh();

    // After the refresh, all affinity nodes are isolated (removed from the
    // default topology); non-affinity nodes remain in it.
    NetworkTopology topology = dnManager.getNetworkTopology();
    for (DatanodeDescriptor dn : affinityDNs) {
      assertFalse(topology.contains(dn), "Affinity node " + dn.getXferAddrWithHostname()
              + " must be removed from the default topology after refresh");
    }
    for (DatanodeDescriptor dn : nonAffinityDNs) {
      assertTrue(topology.contains(dn), "Non-affinity node " + dn.getXferAddrWithHostname()
              + " must remain in the default topology");
    }

    // Simulate a node that registered during the rebuild: build a "stale"
    // isolated set that OMITS one affinity node, as if it were not yet visible
    // when internalRefresh() snapshotted the live datanodes.
    DatanodeDescriptor missed = affinityDNs.get(AFFINITY_COUNT - 1);
    Set<String> staleIsolated = new HashSet<>();
    for (DatanodeDescriptor dn : affinityDNs) {
      if (dn != missed) {
        staleIsolated.add(dn.getXferAddrWithHostname());
      }
    }

    dnManager.postAffinityRefresh(staleIsolated);

    // The omitted-but-still-matching node must NOT be restored to the default
    // topology (that would be an isolation leak); reconciliation re-matches it
    // against the live patterns and keeps it isolated.
    assertFalse(topology.contains(missed),
        "Omitted affinity node " + missed.getXferAddrWithHostname()
            + " must stay isolated (not leaked back into the default topology) "
            + "after reconciliation");
    assertTrue(affinityManager.getIsolatedDatanodes()
            .contains(missed.getXferAddrWithHostname()),
        "Reconciliation must re-track the omitted node in the "
            + "affinity isolated-pool set");
    // The nodes present in the stale set stay isolated as well.
    for (DatanodeDescriptor dn : affinityDNs) {
      if (dn != missed) {
        assertFalse(topology.contains(dn), "Affinity node " + dn.getXferAddrWithHostname()
                + " must stay isolated");
      }
    }
    // Non-affinity nodes are untouched by the reconciliation.
    for (DatanodeDescriptor dn : nonAffinityDNs) {
      assertTrue(topology.contains(dn), "Non-affinity node " + dn.getXferAddrWithHostname()
              + " must remain in the default topology after reconciliation");
    }
  }

  /**
   * Regression test for the DataNode-replacement descriptor race.
   *
   * <p>The per-group restricted {@link NetworkTopology} stores DataNode
   * descriptor OBJECTS, while affinity membership is tracked by transfer
   * address. When a DataNode is REPLACED -- the same {@code host:port}
   * re-registers with a brand-new descriptor / storage UUID (disk wipe, host
   * reimage) -- the address is unchanged, so the address-keyed bookkeeping sees
   * "already tracked" and, without reconciliation, the group topology would
   * keep the STALE descriptor. Block placement could then hand out a dead
   * descriptor. {@link DatanodeAffinityManager#onDatanodeRegistered} must swap
   * the stale descriptor for the live one.
   *
   * <p>We reproduce the state deterministically: after an affinity node is
   * isolated, we build a fresh descriptor with the same address but a different
   * UUID and register it, then assert the group topology now holds the NEW
   * descriptor object (and not the old one).
   */
  @Test
  public void testReplacementDescriptorReconciledInGroupTopology()
      throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    DatanodeDescriptor original = allDNs.get(0);

    String datanodeRegex = "("
        + escapeForRegex(original.getXferAddrWithHostname()) + ")";
    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));

    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    affinityManager.refresh();

    // Locate the group topology for the affinity path and confirm the original
    // descriptor is the one currently stored.
    NetworkTopology groupTopo = null;
    for (DatanodeAffinityManager.AffinityGroupTopology g :
        affinityManager.getAffinityGroupTopologies()) {
      if (g.pathPattern.matcher(AFFINITY_DIR + "/x").find()) {
        groupTopo = g.topology;
        break;
      }
    }
    assertNotNull(groupTopo, "Affinity group topology must exist for " + AFFINITY_DIR);
    String path = NodeBase.getPath(original);
    assertSame(original, groupTopo.getNode(path),
        "Group topology must initially hold the original descriptor");

    // Build a replacement descriptor: SAME transfer address / host / port, but
    // a DIFFERENT storage UUID and a distinct object, mimicking a reimaged DN.
    DatanodeID replacementId = new DatanodeID(original.getIpAddr(),
        original.getHostName(), "replacement-" + UUID.randomUUID(),
        original.getXferPort(), original.getInfoPort(),
        original.getInfoSecurePort(), original.getIpcPort());
    DatanodeDescriptor replacement = new DatanodeDescriptor(replacementId);
    replacement.setNetworkLocation(original.getNetworkLocation());
    assertEquals(original.getXferAddrWithHostname(),
        replacement.getXferAddrWithHostname(),
        "Replacement must reuse the same affinity address");

    affinityManager.onDatanodeRegistered(replacement);

    // The group topology must now hold the LIVE (replacement) descriptor object,
    // not the stale original, so placement never selects a dead descriptor.
    Node stored = groupTopo.getNode(path);
    assertSame(replacement, stored, "Group topology must be reconciled to the live replacement "
            + "descriptor after re-registration with a new UUID");
    assertFalse(groupTopo.contains(original),
        "Stale original descriptor must no longer be in the group "
            + "topology");
    // Address-keyed membership is unchanged: the address stays isolated.
    assertTrue(affinityManager.getIsolatedDatanodes()
            .contains(original.getXferAddrWithHostname()),
        "Replaced node's address must remain isolated");
  }

  /**
   * Variant of {@link #testReplacementDescriptorReconciledInGroupTopology}
   * where the replacement DataNode resolves to a DIFFERENT network location
   * (rack/IP change), so the stale descriptor lives at a DIFFERENT topology
   * path than the live one. Reconciliation must still remove the stale leaf --
   * matched by transfer endpoint ({@code getXferAddrWithHostname()}), not by
   * exact path -- so no dead descriptor lingers and exactly one live descriptor
   * remains for the endpoint.
   */
  @Test
  public void testReplacementWithDifferentRackReconciled() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    DatanodeDescriptor original = allDNs.get(0);

    String datanodeRegex = "("
        + escapeForRegex(original.getXferAddrWithHostname()) + ")";
    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));

    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    affinityManager.refresh();

    NetworkTopology groupTopo = null;
    for (DatanodeAffinityManager.AffinityGroupTopology g :
        affinityManager.getAffinityGroupTopologies()) {
      if (g.pathPattern.matcher(AFFINITY_DIR + "/x").find()) {
        groupTopo = g.topology;
        break;
      }
    }
    assertNotNull(groupTopo, "Affinity group topology must exist for " + AFFINITY_DIR);
    String oldPath = NodeBase.getPath(original);
    assertSame(original, groupTopo.getNode(oldPath),
        "Original descriptor must start in the group topology");

    // Replacement reuses the same host:port (same affinity identity) but has a
    // different IP and a different rack, so it publishes at a different topology
    // path than the original.
    DatanodeID replacementId = new DatanodeID("10.99.99.99",
        original.getHostName(), "replacement-" + UUID.randomUUID(),
        original.getXferPort(), original.getInfoPort(),
        original.getInfoSecurePort(), original.getIpcPort());
    DatanodeDescriptor replacement = new DatanodeDescriptor(replacementId);
    replacement.setNetworkLocation("/rack-new");
    assertEquals(original.getXferAddrWithHostname(),
        replacement.getXferAddrWithHostname(),
        "Replacement must reuse the same affinity endpoint");
    String newPath = NodeBase.getPath(replacement);
    assertNotEquals(oldPath, newPath, "Replacement must resolve to a different topology path");

    affinityManager.onDatanodeRegistered(replacement);

    assertNull(groupTopo.getNode(oldPath),
        "Stale original descriptor at the old path must be removed even "
            + "though the replacement is at a different path");
    assertSame(replacement, groupTopo.getNode(newPath),
        "Live replacement descriptor must be present in the group "
            + "topology at its new path");
    long endpointLeaves = groupTopo.getLeaves(NodeBase.ROOT).stream()
        .filter(n -> n instanceof DatanodeDescriptor
            && original.getXferAddrWithHostname()
                .equals(((DatanodeDescriptor) n).getXferAddrWithHostname()))
        .count();
    assertEquals(1, endpointLeaves, "Exactly one (live) descriptor must remain for the endpoint");
  }

  /**
   * Regression guard for the storage-count-staleness defect: the per-group
   * restricted topology MUST be a plain {@link NetworkTopology}, never a
   * storage-type-aware {@link DFSNetworkTopology}. A DFSNetworkTopology captures
   * each node's storage-type counts at {@code add()} time and thereafter updates
   * them only through the descriptor's parent back-pointer. An affinity node's
   * shared descriptor has its parent nulled when it is removed from the default
   * topology, so those counts could never update from later storage reports --
   * freezing them at the ZERO value captured during a fresh (post-NameNode-
   * restart) registration and silently breaking group placement until a manual
   * {@code dfsadmin -refreshNodes}. See {@code DatanodeManager#createEmptyTopology}.
   */
  @Test
  public void testAffinityGroupTopologyIsPlainNetworkTopology()
      throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    DatanodeDescriptor original = allDNs.get(0);

    String datanodeRegex = "("
        + escapeForRegex(original.getXferAddrWithHostname()) + ")";
    writeJson(buildAffinityJson("tenant-a",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));

    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    affinityManager.refresh();

    List<DatanodeAffinityManager.AffinityGroupTopology> topos =
        affinityManager.getAffinityGroupTopologies();
    assertFalse(topos.isEmpty(), "At least one affinity group topology must exist");
    for (DatanodeAffinityManager.AffinityGroupTopology g : topos) {
      assertNotNull(g.topology, "Group topology must be non-null");
      assertFalse(g.topology instanceof DFSNetworkTopology,
          "Affinity group topology must NOT be a storage-type-aware "
              + "DFSNetworkTopology (its captured storage counts go stale for "
              + "affinity nodes and break placement after a NameNode restart)");
      assertSame(NetworkTopology.class, g.topology.getClass(),
          "Affinity group topology must be exactly a plain "
              + "NetworkTopology");
    }

    // The factory itself must never yield a DFSNetworkTopology for an affinity
    // group, regardless of the cluster topology implementation.
    assertFalse(dnManager.createEmptyTopology() instanceof DFSNetworkTopology,
        "createEmptyTopology() must return a plain NetworkTopology");
  }

  private void writeJson(String content) throws Exception {
    try (FileWriter fw = new FileWriter(affinityJsonFile)) {
      fw.write(content);
    }
  }

  /**
   * Build a single-entry affinity JSON array.
   *
   * @param groupName     human-readable label
   * @param fileRegex     regex matched against HDFS source path
   * @param datanodeRegex regex matched against DataNode "hostname:port"
   */
  private static String buildAffinityJson(String groupName,
      String fileRegex, String datanodeRegex) {
    // Escape backslashes and quotes for valid JSON string literals.
    String escapedFileRegex  = fileRegex.replace("\\", "\\\\");
    String escapedDnRegex    = datanodeRegex.replace("\\", "\\\\");
    return String.format(
        "[{\"affinityGroupName\":\"%s\","
            + "\"regexPattern\":\"%s\","
            + "\"datanodeRegex\":\"%s\"}]",
        groupName, escapedFileRegex, escapedDnRegex);
  }

  /**
   * Escape a literal string so it is safe to embed as a Java regex pattern.
   * Replaces {@code .} with {@code \.} and {@code :} with the literal colon
   * (already safe in regex).  Uses simple character-by-character escaping for
   * the small set of chars that appear in {@code "127.0.0.1:PORT"} strings.
   */
  private static String escapeForRegex(String literal) {
    return literal.replace(".", "\\.").replace("+", "\\+");
  }

  // ---------------------------------------------------------------------------
  // fsck -favored-nodes integration
  // ---------------------------------------------------------------------------

  /**
   * When an affinity rule matches the file path, {@code hdfs fsck -files
   * -favored-nodes} must print the resolved affinity DataNode list rather than
   * reporting "none".  This exercises the {@code -favored-nodes} option added to
   * {@link DFSck} and the {@code printFavoredNodes()} output in
   * {@code NamenodeFsck}.
   *
   * <p>The affinity rule uses {@code datanodeRegex = ".*"} so every DataNode in
   * the cluster matches, and {@code regexPattern = "^/tenant-data/.*"} so files
   * written under {@code /tenant-data} are covered.  After the JSON is rewritten
   * we re-trigger {@link DatanodeAffinityManager#refresh()} so the resolved map
   * is populated before fsck runs.
   */
  @Test
  public void testFsckFavoredNodesMatchingRule() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    // Rule matches every DataNode (".*") for paths under AFFINITY_DIR.
    writeJson(buildAffinityJson("tenant-a", "^" + AFFINITY_DIR + "/.*", ".*"));

    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    assertNotNull(affinityManager, "DatanodeAffinityManager must be configured");
    affinityManager.refresh();

    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, new Path(AFFINITY_DIR + "/test.parquet"),
        512, REPLICATION, 0L);

    String out = runFsck(conf, AFFINITY_DIR, "-files", "-favored-nodes");
    assertTrue(out.contains("Affinity favored nodes"),
        "Output must contain 'Affinity favored nodes'");
    assertFalse(out.contains("Affinity favored nodes: none"),
        "Favored nodes must not be empty when the affinity rule matches");
  }

  /**
   * Run {@code hdfs fsck} for {@code path} with the supplied extra arguments and
   * return the captured stdout.  A minimal in-process equivalent of the helper
   * used by {@code TestFsck}, kept local so this affinity test is self-contained.
   */
  private static String runFsck(Configuration conf, String path,
      String... args) throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    String[] fsckArgs = new String[args.length + 1];
    fsckArgs[0] = path;
    System.arraycopy(args, 0, fsckArgs, 1, args.length);
    ToolRunner.run(new DFSck(conf, out), fsckArgs);
    return bStream.toString();
  }
}
