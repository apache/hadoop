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
package org.apache.hadoop.hdfs.server.namenode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.util.Shell.getMemlockLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class for testing {@link NameNodeMXBean} implementation
 */
public class TestNameNodeMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestNameNodeMXBean.class);

  /**
   * Used to assert equality between doubles
   */
  private static final double DELTA = 0.000001;

  static {
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());
  }

  @SuppressWarnings({ "unchecked" })
  @Test
  public void testNameNodeMXBeanInfo() throws Exception {
    Configuration conf = new Configuration();
    Long maxLockedMemory = getMemlockLimit(
        NativeIO.POSIX.getCacheManipulator().getMemlockLimit());
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        maxLockedMemory);
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();

      // Set upgrade domain on the first DN.
      String upgradeDomain = "abcd";
      DatanodeManager dm = cluster.getNameNode().getNamesystem().
          getBlockManager().getDatanodeManager();
      DatanodeDescriptor dd = dm.getDatanode(
          cluster.getDataNodes().get(0).getDatanodeId());
      dd.setUpgradeDomain(upgradeDomain);
      String dnXferAddrWithUpgradeDomainSet = dd.getXferAddr();

      // Put the second DN to maintenance state.
      DatanodeDescriptor maintenanceNode = dm.getDatanode(
          cluster.getDataNodes().get(1).getDatanodeId());
      maintenanceNode.setInMaintenance();
      String dnXferAddrInMaintenance = maintenanceNode.getXferAddr();

      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");
      // get attribute "ClusterId"
      String clusterId = (String) mbs.getAttribute(mxbeanName, "ClusterId");
      assertEquals(fsn.getClusterId(), clusterId);
      // get attribute "BlockPoolId"
      String blockpoolId = (String) mbs.getAttribute(mxbeanName, 
          "BlockPoolId");
      assertEquals(fsn.getBlockPoolId(), blockpoolId);
      // get attribute "Version"
      String version = (String) mbs.getAttribute(mxbeanName, "Version");
      assertEquals(fsn.getVersion(), version);
      assertTrue(version.equals(VersionInfo.getVersion()
          + ", r" + VersionInfo.getRevision()));
      // get attribute "Used"
      Long used = (Long) mbs.getAttribute(mxbeanName, "Used");
      assertEquals(fsn.getUsed(), used.longValue());
      // get attribute "Total"
      Long total = (Long) mbs.getAttribute(mxbeanName, "Total");
      assertEquals(fsn.getTotal(), total.longValue());
      // get attribute "safemode"
      String safemode = (String) mbs.getAttribute(mxbeanName, "Safemode");
      assertEquals(fsn.getSafemode(), safemode);
      // get attribute nondfs
      Long nondfs = (Long) (mbs.getAttribute(mxbeanName, "NonDfsUsedSpace"));
      assertEquals(fsn.getNonDfsUsedSpace(), nondfs.longValue());
      // get attribute percentremaining
      Float percentremaining = (Float) (mbs.getAttribute(mxbeanName,
          "PercentRemaining"));
      assertEquals(fsn.getPercentRemaining(), percentremaining, DELTA);
      // get attribute Totalblocks
      Long totalblocks = (Long) (mbs.getAttribute(mxbeanName, "TotalBlocks"));
      assertEquals(fsn.getTotalBlocks(), totalblocks.longValue());
      // get attribute alivenodeinfo
      String alivenodeinfo = (String) (mbs.getAttribute(mxbeanName,
          "LiveNodes"));
      Map<String, Map<String, Object>> liveNodes =
          (Map<String, Map<String, Object>>) JSON.parse(alivenodeinfo);
      assertTrue(liveNodes.size() == 2);
      for (Map<String, Object> liveNode : liveNodes.values()) {
        assertTrue(liveNode.containsKey("nonDfsUsedSpace"));
        assertTrue(((Long)liveNode.get("nonDfsUsedSpace")) >= 0);
        assertTrue(liveNode.containsKey("capacity"));
        assertTrue(((Long)liveNode.get("capacity")) > 0);
        assertTrue(liveNode.containsKey("numBlocks"));
        assertTrue(((Long)liveNode.get("numBlocks")) == 0);
        assertTrue(liveNode.containsKey("lastBlockReport"));
        // a. By default the upgrade domain isn't defined on any DN.
        // b. If the upgrade domain is set on a DN, JMX should have the same
        // value.
        String xferAddr = (String)liveNode.get("xferaddr");
        if (!xferAddr.equals(dnXferAddrWithUpgradeDomainSet)) {
          assertTrue(!liveNode.containsKey("upgradeDomain"));
        } else {
          assertTrue(liveNode.get("upgradeDomain").equals(upgradeDomain));
        }
        // "adminState" is set to maintenance only for the specific dn.
        boolean inMaintenance = liveNode.get("adminState").equals(
            DatanodeInfo.AdminStates.IN_MAINTENANCE.toString());
        assertFalse(xferAddr.equals(dnXferAddrInMaintenance) ^ inMaintenance);
      }
      assertEquals(fsn.getLiveNodes(), alivenodeinfo);
      // get attributes DeadNodes
      String deadNodeInfo = (String) (mbs.getAttribute(mxbeanName,
          "DeadNodes"));
      assertEquals(fsn.getDeadNodes(), deadNodeInfo);
      // get attribute NodeUsage
      String nodeUsage = (String) (mbs.getAttribute(mxbeanName,
          "NodeUsage"));
      assertEquals("Bad value for NodeUsage", fsn.getNodeUsage(), nodeUsage);
      // get attribute NameJournalStatus
      String nameJournalStatus = (String) (mbs.getAttribute(mxbeanName,
          "NameJournalStatus"));
      assertEquals("Bad value for NameJournalStatus",
          fsn.getNameJournalStatus(), nameJournalStatus);
      // get attribute JournalTransactionInfo
      String journalTxnInfo = (String) mbs.getAttribute(mxbeanName,
          "JournalTransactionInfo");
      assertEquals("Bad value for NameTxnIds", fsn.getJournalTransactionInfo(),
          journalTxnInfo);
      // get attribute "CompileInfo"
      String compileInfo = (String) mbs.getAttribute(mxbeanName, "CompileInfo");
      assertEquals("Bad value for CompileInfo", fsn.getCompileInfo(),
          compileInfo);
      // get attribute CorruptFiles
      String corruptFiles = (String) (mbs.getAttribute(mxbeanName,
          "CorruptFiles"));
      assertEquals("Bad value for CorruptFiles", fsn.getCorruptFiles(),
          corruptFiles);
      // get attribute CorruptFilesCount
      int corruptFilesCount = (int) (mbs.getAttribute(mxbeanName,
          "CorruptFilesCount"));
      assertEquals("Bad value for CorruptFilesCount",
          fsn.getCorruptFilesCount(), corruptFilesCount);
      // get attribute NameDirStatuses
      String nameDirStatuses = (String) (mbs.getAttribute(mxbeanName,
          "NameDirStatuses"));
      assertEquals(fsn.getNameDirStatuses(), nameDirStatuses);
      Map<String, Map<String, String>> statusMap =
        (Map<String, Map<String, String>>) JSON.parse(nameDirStatuses);
      Collection<URI> nameDirUris = cluster.getNameDirs(0);
      for (URI nameDirUri : nameDirUris) {
        File nameDir = new File(nameDirUri);
        System.out.println("Checking for the presence of " + nameDir +
            " in active name dirs.");
        assertTrue(statusMap.get("active").containsKey(
            nameDir.getAbsolutePath()));
      }
      assertEquals(2, statusMap.get("active").size());
      assertEquals(0, statusMap.get("failed").size());

      // This will cause the first dir to fail.
      File failedNameDir = new File(nameDirUris.iterator().next());
      assertEquals(0, FileUtil.chmod(
          new File(failedNameDir, "current").getAbsolutePath(), "000"));
      cluster.getNameNodeRpc().rollEditLog();
      
      nameDirStatuses = (String) (mbs.getAttribute(mxbeanName,
          "NameDirStatuses"));
      statusMap = (Map<String, Map<String, String>>) JSON.parse(nameDirStatuses);
      for (URI nameDirUri : nameDirUris) {
        File nameDir = new File(nameDirUri);
        String expectedStatus =
            nameDir.equals(failedNameDir) ? "failed" : "active";
        System.out.println("Checking for the presence of " + nameDir +
            " in " + expectedStatus + " name dirs.");
        assertTrue(statusMap.get(expectedStatus).containsKey(
            nameDir.getAbsolutePath()));
      }
      assertEquals(1, statusMap.get("active").size());
      assertEquals(1, statusMap.get("failed").size());
      assertEquals(0L, mbs.getAttribute(mxbeanName, "CacheUsed"));
      assertEquals(maxLockedMemory *
          cluster.getDataNodes().size(),
              mbs.getAttribute(mxbeanName, "CacheCapacity"));
      assertNull("RollingUpgradeInfo should be null when there is no rolling"
          + " upgrade", mbs.getAttribute(mxbeanName, "RollingUpgradeStatus"));
    } finally {
      if (cluster != null) {
        for (URI dir : cluster.getNameDirs(0)) {
          FileUtil.chmod(
            new File(new File(dir), "current").getAbsolutePath(), "755");
        }
        cluster.shutdown();
      }
    }
  }

  @SuppressWarnings({ "unchecked" })
  @Test
  public void testLastContactTime() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    MiniDFSCluster cluster = null;
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "temp/TestNameNodeMXBean");

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=NameNode,name=NameNodeInfo");

      List<String> hosts = new ArrayList<>();
      for(DataNode dn : cluster.getDataNodes()) {
        hosts.add(dn.getDisplayName());
      }
      hostsFileWriter.initIncludeHosts(hosts.toArray(
          new String[hosts.size()]));
      fsn.getBlockManager().getDatanodeManager().refreshNodes(conf);

      cluster.stopDataNode(0);
      while (fsn.getBlockManager().getDatanodeManager().getNumLiveDataNodes()
        != 2 ) {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      }

      // get attribute DeadNodes
      String deadNodeInfo = (String) (mbs.getAttribute(mxbeanName,
        "DeadNodes"));
      assertEquals(fsn.getDeadNodes(), deadNodeInfo);
      Map<String, Map<String, Object>> deadNodes =
          (Map<String, Map<String, Object>>) JSON.parse(deadNodeInfo);
      assertTrue(deadNodes.size() > 0);
      for (Map<String, Object> deadNode : deadNodes.values()) {
        assertTrue(deadNode.containsKey("lastContact"));
        assertTrue(deadNode.containsKey("adminState"));
        assertTrue(deadNode.containsKey("xferaddr"));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      hostsFileWriter.cleanup();
    }
  }

  @Test (timeout = 120000)
  public void testDecommissioningNodes() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 30);
    MiniDFSCluster cluster = null;
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "temp/TestNameNodeMXBean");

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");

      List<String> hosts = new ArrayList<>();
      for(DataNode dn : cluster.getDataNodes()) {
        hosts.add(dn.getDisplayName());
      }
      hostsFileWriter.initIncludeHosts(hosts.toArray(
          new String[hosts.size()]));
      fsn.getBlockManager().getDatanodeManager().refreshNodes(conf);

      // 1. Verify Live nodes
      String liveNodesInfo = (String) (mbs.getAttribute(mxbeanName,
          "LiveNodes"));
      Map<String, Map<String, Object>> liveNodes =
          (Map<String, Map<String, Object>>) JSON.parse(liveNodesInfo);
      assertEquals(fsn.getLiveNodes(), liveNodesInfo);
      assertEquals(fsn.getNumLiveDataNodes(), liveNodes.size());

      for (Map<String, Object> liveNode : liveNodes.values()) {
        assertTrue(liveNode.containsKey("lastContact"));
        assertTrue(liveNode.containsKey("xferaddr"));
      }

      // Add the 1st DataNode to Decommission list
      hostsFileWriter.initExcludeHost(
          cluster.getDataNodes().get(0).getDisplayName());
      fsn.getBlockManager().getDatanodeManager().refreshNodes(conf);

      // Wait for the DatanodeAdminManager to complete refresh nodes
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            String decomNodesInfo = (String) (mbs.getAttribute(mxbeanName,
                "DecomNodes"));
            Map<String, Map<String, Object>> decomNodes =
                (Map<String, Map<String, Object>>) JSON.parse(decomNodesInfo);
            if (decomNodes.size() > 0) {
              return true;
            }
          } catch (Exception e) {
            return false;
          }
          return false;
        }
      }, 1000, 60000);

      // 2. Verify Decommission InProgress nodes
      String decomNodesInfo = (String) (mbs.getAttribute(mxbeanName,
          "DecomNodes"));
      Map<String, Map<String, Object>> decomNodes =
          (Map<String, Map<String, Object>>) JSON.parse(decomNodesInfo);
      assertEquals(fsn.getDecomNodes(), decomNodesInfo);
      assertEquals(fsn.getNumDecommissioningDataNodes(), decomNodes.size());
      assertEquals(0, fsn.getNumDecomLiveDataNodes());
      assertEquals(0, fsn.getNumDecomDeadDataNodes());

      // Wait for the DatanodeAdminManager to complete check
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          if (fsn.getNumDecomLiveDataNodes() == 1) {
            return true;
          }
          return false;
        }
      }, 1000, 60000);

      // 3. Verify Decommissioned nodes
      decomNodesInfo = (String) (mbs.getAttribute(mxbeanName, "DecomNodes"));
      decomNodes =
          (Map<String, Map<String, Object>>) JSON.parse(decomNodesInfo);
      assertEquals(0, decomNodes.size());
      assertEquals(fsn.getDecomNodes(), decomNodesInfo);
      assertEquals(1, fsn.getNumDecomLiveDataNodes());
      assertEquals(0, fsn.getNumDecomDeadDataNodes());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      hostsFileWriter.cleanup();
    }
  }

  @Test (timeout = 120000)
  public void testMaintenanceNodes() throws Exception {
    LOG.info("Starting testMaintenanceNodes");
    int expirationInMs = 30 * 1000;
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        expirationInMs);
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        CombinedHostFileManager.class, HostConfigManager.class);
    MiniDFSCluster cluster = null;
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "temp/TestNameNodeMXBean");

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");

      List<String> hosts = new ArrayList<>();
      for(DataNode dn : cluster.getDataNodes()) {
        hosts.add(dn.getDisplayName());
      }
      hostsFileWriter.initIncludeHosts(hosts.toArray(
          new String[hosts.size()]));
      fsn.getBlockManager().getDatanodeManager().refreshNodes(conf);

      // 1. Verify nodes for DatanodeReportType.LIVE state
      String liveNodesInfo = (String) (mbs.getAttribute(mxbeanName,
          "LiveNodes"));
      LOG.info("Live Nodes: " + liveNodesInfo);
      Map<String, Map<String, Object>> liveNodes =
          (Map<String, Map<String, Object>>) JSON.parse(liveNodesInfo);
      assertEquals(fsn.getLiveNodes(), liveNodesInfo);
      assertEquals(fsn.getNumLiveDataNodes(), liveNodes.size());

      for (Map<String, Object> liveNode : liveNodes.values()) {
        assertTrue(liveNode.containsKey("lastContact"));
        assertTrue(liveNode.containsKey("xferaddr"));
      }

      // Add the 1st DataNode to Maintenance list
      Map<String, Long> maintenanceNodes = new HashMap<>();
      maintenanceNodes.put(cluster.getDataNodes().get(0).getDisplayName(),
          Time.now() + expirationInMs);
      hostsFileWriter.initOutOfServiceHosts(null, maintenanceNodes);
      fsn.getBlockManager().getDatanodeManager().refreshNodes(conf);

      boolean recheck = true;
      while (recheck) {
        // 2. Verify nodes for DatanodeReportType.ENTERING_MAINTENANCE state
        String enteringMaintenanceNodesInfo =
            (String) (mbs.getAttribute(mxbeanName, "EnteringMaintenanceNodes"));
        Map<String, Map<String, Object>> enteringMaintenanceNodes =
            (Map<String, Map<String, Object>>) JSON.parse(
                enteringMaintenanceNodesInfo);
        if (enteringMaintenanceNodes.size() <= 0) {
          LOG.info("Waiting for a node to Enter Maintenance state!");
          Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
          continue;
        }
        LOG.info("Nodes entering Maintenance: " + enteringMaintenanceNodesInfo);
        recheck = false;
        assertEquals(fsn.getEnteringMaintenanceNodes(),
            enteringMaintenanceNodesInfo);
        assertEquals(fsn.getNumEnteringMaintenanceDataNodes(),
            enteringMaintenanceNodes.size());
        assertEquals(0, fsn.getNumInMaintenanceLiveDataNodes());
        assertEquals(0, fsn.getNumInMaintenanceDeadDataNodes());
      }

      // Wait for the DatanodeAdminManager to complete check
      // and perform state transition
      while (fsn.getNumInMaintenanceLiveDataNodes() != 1) {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      }

      // 3. Verify nodes for AdminStates.IN_MAINTENANCE state
      String enteringMaintenanceNodesInfo =
          (String) (mbs.getAttribute(mxbeanName, "EnteringMaintenanceNodes"));
      Map<String, Map<String, Object>> enteringMaintenanceNodes =
          (Map<String, Map<String, Object>>) JSON.parse(
              enteringMaintenanceNodesInfo);
      assertEquals(0, enteringMaintenanceNodes.size());
      assertEquals(fsn.getEnteringMaintenanceNodes(),
          enteringMaintenanceNodesInfo);
      assertEquals(1, fsn.getNumInMaintenanceLiveDataNodes());
      assertEquals(0, fsn.getNumInMaintenanceDeadDataNodes());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      hostsFileWriter.cleanup();
    }
  }

  @Test(timeout=120000)
  @SuppressWarnings("unchecked")
  public void testTopUsers() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> map = mapper.readValue(topUsers, Map.class);
      assertTrue("Could not find map key timestamp", 
          map.containsKey("timestamp"));
      assertTrue("Could not find map key windows", map.containsKey("windows"));
      List<Map<String, List<Map<String, Object>>>> windows =
          (List<Map<String, List<Map<String, Object>>>>) map.get("windows");
      assertEquals("Unexpected num windows", 3, windows.size());
      for (Map<String, List<Map<String, Object>>> window : windows) {
        final List<Map<String, Object>> ops = window.get("ops");
        assertEquals("Unexpected num ops", 4, ops.size());
        for (Map<String, Object> op: ops) {
          if (op.get("opType").equals("datanodeReport")) {
            continue;
          }
          final long count = Long.parseLong(op.get("totalCount").toString());
          final String opType = op.get("opType").toString();
          final int expected;
          if (opType.equals(TopConf.ALL_CMDS)) {
            expected = 2 * NUM_OPS + 2;
          } else if (opType.equals("datanodeReport")) {
            expected = 2;
          } else {
            expected = NUM_OPS;
          }
          assertEquals("Unexpected total count", expected, count);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testTopUsersDisabled() throws Exception {
    final Configuration conf = new Configuration();
    // Disable nntop
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, false);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      assertNull("Did not expect to find TopUserOpCounts bean!", topUsers);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=120000)
  public void testTopUsersNoPeriods() throws Exception {
    final Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "");
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFsns = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");
      FileSystem fs = cluster.getFileSystem();
      final Path path = new Path("/");
      final int NUM_OPS = 10;
      for (int i=0; i< NUM_OPS; i++) {
        fs.listStatus(path);
        fs.setTimes(path, 0, 1);
      }
      String topUsers =
          (String) (mbs.getAttribute(mxbeanNameFsns, "TopUserOpCounts"));
      assertNotNull("Expected TopUserOpCounts bean!", topUsers);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testQueueLength() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanNameFs =
          new ObjectName("Hadoop:service=NameNode,name=FSNamesystem");
      int queueLength = (int) mbs.getAttribute(mxbeanNameFs, "LockQueueLength");
      assertEquals(0, queueLength);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 120000)
  public void testNNDirectorySize() throws Exception{
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    MiniDFSCluster cluster = null;
    for (int i = 0; i < 5; i++) {
      try{
        // Have to specify IPC ports so the NNs can talk to each other.
        int[] ports = ServerSocketUtil.getPorts(2);
        MiniDFSNNTopology topology = new MiniDFSNNTopology()
            .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
                .addNN(new MiniDFSNNTopology.NNConf("nn1").setIpcPort(ports[0]))
                .addNN(
                    new MiniDFSNNTopology.NNConf("nn2").setIpcPort(ports[1])));

        cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(topology).numDataNodes(0)
            .build();
        break;
      } catch (BindException e) {
        // retry if race on ports given by ServerSocketUtil#getPorts
        continue;
      }
    }
    if (cluster == null) {
      fail("failed to start mini cluster.");
    }
    FileSystem fs = null;
    try {
      cluster.waitActive();

      FSNamesystem nn0 = cluster.getNamesystem(0);
      FSNamesystem nn1 = cluster.getNamesystem(1);
      checkNNDirSize(cluster.getNameDirs(0), nn0.getNameDirSize());
      checkNNDirSize(cluster.getNameDirs(1), nn1.getNameDirSize());
      cluster.transitionToActive(0);
      fs = cluster.getFileSystem(0);
      DFSTestUtil.createFile(fs, new Path("/file"), 0, (short) 1, 0L);

      //rollEditLog
      HATestUtil.waitForStandbyToCatchUp(cluster.getNameNode(0),
          cluster.getNameNode(1));
      checkNNDirSize(cluster.getNameDirs(0), nn0.getNameDirSize());
      checkNNDirSize(cluster.getNameDirs(1), nn1.getNameDirSize());

      //Test metric after call saveNamespace
      DFSTestUtil.createFile(fs, new Path("/file"), 0, (short) 1, 0L);
      nn0.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      nn0.saveNamespace(0, 0);
      checkNNDirSize(cluster.getNameDirs(0), nn0.getNameDirSize());
    } finally {
      cluster.shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  private void checkNNDirSize(Collection<URI> nameDirUris, String metric){
    Map<String, Long> nnDirMap =
        (Map<String, Long>) JSON.parse(metric);
    assertEquals(nameDirUris.size(), nnDirMap.size());
    for (URI dirUrl : nameDirUris) {
      File dir = new File(dirUrl);
      assertEquals(nnDirMap.get(dir.getAbsolutePath()).longValue(),
          FileUtils.sizeOfDirectory(dir));
    }
  }

  @Test
  public void testEnabledEcPoliciesMetric() throws Exception {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();

      ErasureCodingPolicy defaultPolicy =
          StripedFileTestUtil.getDefaultECPolicy();
      int dataBlocks = defaultPolicy.getNumDataUnits();
      int parityBlocks = defaultPolicy.getNumParityUnits();
      int totalSize = dataBlocks + parityBlocks;
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(totalSize).build();
      fs = cluster.getFileSystem();

      final String defaultPolicyName = defaultPolicy.getName();
      final String rs104PolicyName = "RS-10-4-1024k";

      assertEquals("Enabled EC policies metric should return with " +
          "the default EC policy", defaultPolicyName,
          getEnabledEcPoliciesMetric());

      fs.enableErasureCodingPolicy(rs104PolicyName);
      assertEquals("Enabled EC policies metric should return with " +
              "both enabled policies separated by a comma",
          rs104PolicyName + ", " + defaultPolicyName,
          getEnabledEcPoliciesMetric());

      fs.disableErasureCodingPolicy(defaultPolicyName);
      fs.disableErasureCodingPolicy(rs104PolicyName);
      assertEquals("Enabled EC policies metric should return with " +
          "an empty string if there is no enabled policy",
          "", getEnabledEcPoliciesMetric());
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testVerifyMissingBlockGroupsMetrics() throws Exception {
    MiniDFSCluster cluster = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      int dataBlocks = StripedFileTestUtil.getDefaultECPolicy()
          .getNumDataUnits();
      int parityBlocks =
          StripedFileTestUtil.getDefaultECPolicy().getNumParityUnits();
      int cellSize = StripedFileTestUtil.getDefaultECPolicy().getCellSize();
      int totalSize = dataBlocks + parityBlocks;
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(totalSize).build();
      fs = cluster.getFileSystem();
      fs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());

      // create file
      Path ecDirPath = new Path("/striped");
      fs.mkdir(ecDirPath, FsPermission.getDirDefault());
      fs.getClient().setErasureCodingPolicy(ecDirPath.toString(),
          StripedFileTestUtil.getDefaultECPolicy().getName());
      Path file = new Path(ecDirPath, "corrupted");
      final int length = cellSize * dataBlocks;
      final byte[] bytes = StripedFileTestUtil.generateBytes(length);
      DFSTestUtil.writeFile(fs, file, bytes);

      LocatedStripedBlock lsb = (LocatedStripedBlock)fs.getClient()
          .getLocatedBlocks(file.toString(), 0, cellSize * dataBlocks).get(0);
      final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(lsb,
          cellSize, dataBlocks, parityBlocks);

      // make an unrecoverable ec file with corrupted blocks
      for(int i = 0; i < parityBlocks + 1; i++) {
        int ipcPort = blks[i].getLocations()[0].getIpcPort();
        cluster.corruptReplica(cluster.getDataNode(ipcPort),
            blks[i].getBlock());
      }

      // disable the heart beat from DN so that the corrupted block record is
      // kept in NameNode
      for (DataNode dn : cluster.getDataNodes()) {
        DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      }

      // Read the file to trigger reportBadBlocks
      try {
        IOUtils.copyBytes(fs.open(file), new IOUtils.NullOutputStream(), conf,
            true);
      } catch (IOException ie) {
        assertTrue(ie.getMessage().contains(
            "missingChunksNum=" + (parityBlocks + 1)));
      }

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName replStateMBeanName = new ObjectName(
          "Hadoop:service=NameNode,name=ReplicatedBlocksState");
      ObjectName ecBlkGrpStateMBeanName = new ObjectName(
          "Hadoop:service=NameNode,name=ECBlockGroupsState");
      ObjectName namenodeMXBeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeInfo");

      // Wait for the metrics to discover the unrecoverable block group
      long expectedMissingBlockCount = 1L;
      long expectedCorruptBlockCount = 1L;
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            Long numMissingBlocks =
                (Long) mbs.getAttribute(namenodeMXBeanName,
                    "NumberOfMissingBlocks");
            if (numMissingBlocks == expectedMissingBlockCount) {
              return true;
            }
          } catch (Exception e) {
            Assert.fail("Caught unexpected exception.");
          }
          return false;
        }
      }, 1000, 60000);

      BlockManagerTestUtil.updateState(
          cluster.getNamesystem().getBlockManager());

      // Verification of missing blocks
      long totalMissingBlocks = cluster.getNamesystem().getMissingBlocksCount();
      Long replicaMissingBlocks =
          (Long) mbs.getAttribute(replStateMBeanName,
              "MissingReplicatedBlocks");
      Long ecMissingBlocks =
          (Long) mbs.getAttribute(ecBlkGrpStateMBeanName,
              "MissingECBlockGroups");
      assertEquals("Unexpected total missing blocks!",
          expectedMissingBlockCount, totalMissingBlocks);
      assertEquals("Unexpected total missing blocks!",
          totalMissingBlocks,
          (replicaMissingBlocks + ecMissingBlocks));
      assertEquals("Unexpected total ec missing blocks!",
          expectedMissingBlockCount, ecMissingBlocks.longValue());

      // Verification of corrupt blocks
      long totalCorruptBlocks =
          cluster.getNamesystem().getCorruptReplicaBlocks();
      Long replicaCorruptBlocks =
          (Long) mbs.getAttribute(replStateMBeanName,
              "CorruptReplicatedBlocks");
      Long ecCorruptBlocks =
          (Long) mbs.getAttribute(ecBlkGrpStateMBeanName,
              "CorruptECBlockGroups");
      assertEquals("Unexpected total corrupt blocks!",
          expectedCorruptBlockCount, totalCorruptBlocks);
      assertEquals("Unexpected total corrupt blocks!",
          totalCorruptBlocks,
          (replicaCorruptBlocks + ecCorruptBlocks));
      assertEquals("Unexpected total ec corrupt blocks!",
          expectedCorruptBlockCount, ecCorruptBlocks.longValue());

      String corruptFiles = (String) (mbs.getAttribute(namenodeMXBeanName,
          "CorruptFiles"));
      int numCorruptFiles = ((Object[]) JSON.parse(corruptFiles)).length;
      assertEquals(1, numCorruptFiles);
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (Exception e) {
          throw e;
        }
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testTotalBlocksMetrics() throws Exception {
    MiniDFSCluster cluster = null;
    FSNamesystem namesystem = null;
    DistributedFileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      int dataBlocks = StripedFileTestUtil.getDefaultECPolicy()
          .getNumDataUnits();
      int parityBlocks =
          StripedFileTestUtil.getDefaultECPolicy().getNumParityUnits();
      int totalSize = dataBlocks + parityBlocks;
      int cellSize = StripedFileTestUtil.getDefaultECPolicy().getCellSize();
      int stripesPerBlock = 2;
      int blockSize = stripesPerBlock * cellSize;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);

      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(totalSize).build();
      namesystem = cluster.getNamesystem();
      fs = cluster.getFileSystem();
      fs.enableErasureCodingPolicy(
          StripedFileTestUtil.getDefaultECPolicy().getName());
      verifyTotalBlocksMetrics(0L, 0L, namesystem.getTotalBlocks());

      // create small file
      Path replDirPath = new Path("/replicated");
      Path replFileSmall = new Path(replDirPath, "replfile_small");
      final short factor = 3;
      DFSTestUtil.createFile(fs, replFileSmall, blockSize, factor, 0);
      DFSTestUtil.waitReplication(fs, replFileSmall, factor);

      Path ecDirPath = new Path("/striped");
      fs.mkdir(ecDirPath, FsPermission.getDirDefault());
      fs.getClient().setErasureCodingPolicy(ecDirPath.toString(),
          StripedFileTestUtil.getDefaultECPolicy().getName());
      Path ecFileSmall = new Path(ecDirPath, "ecfile_small");
      final int smallLength = cellSize * dataBlocks;
      final byte[] smallBytes = StripedFileTestUtil.generateBytes(smallLength);
      DFSTestUtil.writeFile(fs, ecFileSmall, smallBytes);
      verifyTotalBlocksMetrics(1L, 1L, namesystem.getTotalBlocks());

      // create learge file
      Path replFileLarge = new Path(replDirPath, "replfile_large");
      DFSTestUtil.createFile(fs, replFileLarge, 2 * blockSize, factor, 0);
      DFSTestUtil.waitReplication(fs, replFileLarge, factor);

      Path ecFileLarge = new Path(ecDirPath, "ecfile_large");
      final int largeLength = blockSize * totalSize + smallLength;
      final byte[] largeBytes = StripedFileTestUtil.generateBytes(largeLength);
      DFSTestUtil.writeFile(fs, ecFileLarge, largeBytes);
      verifyTotalBlocksMetrics(3L, 3L, namesystem.getTotalBlocks());

      // delete replicated files
      fs.delete(replDirPath, true);
      verifyTotalBlocksMetrics(0L, 3L, namesystem.getTotalBlocks());

      // delete ec files
      fs.delete(ecDirPath, true);
      verifyTotalBlocksMetrics(0L, 0L, namesystem.getTotalBlocks());
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (Exception e) {
          throw e;
        }
      }
      if (namesystem != null) {
        try {
          namesystem.close();
        } catch (Exception e) {
          throw e;
        }
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  void verifyTotalBlocksMetrics(long expectedTotalReplicatedBlocks,
      long expectedTotalECBlockGroups, long actualTotalBlocks)
      throws Exception {
    long expectedTotalBlocks = expectedTotalReplicatedBlocks
        + expectedTotalECBlockGroups;
    assertEquals("Unexpected total blocks!", expectedTotalBlocks,
        actualTotalBlocks);

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName replStateMBeanName = new ObjectName(
        "Hadoop:service=NameNode,name=ReplicatedBlocksState");
    ObjectName ecBlkGrpStateMBeanName = new ObjectName(
        "Hadoop:service=NameNode,name=ECBlockGroupsState");
    Long totalReplicaBlocks = (Long) mbs.getAttribute(replStateMBeanName,
        "TotalReplicatedBlocks");
    Long totalECBlockGroups = (Long) mbs.getAttribute(ecBlkGrpStateMBeanName,
        "TotalECBlockGroups");
    assertEquals("Unexpected total replicated blocks!",
        expectedTotalReplicatedBlocks, totalReplicaBlocks.longValue());
    assertEquals("Unexpected total ec block groups!",
        expectedTotalECBlockGroups, totalECBlockGroups.longValue());
    verifyEcClusterSetupVerifyResult(mbs);
  }

  private String getEnabledEcPoliciesMetric() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=NameNode,name=ECBlockGroupsState");
    return (String) (mbs.getAttribute(mxbeanName,
        "EnabledEcPolicies"));
  }

  private void verifyEcClusterSetupVerifyResult(MBeanServer mbs)
      throws Exception{
    ObjectName namenodeMXBeanName = new ObjectName(
        "Hadoop:service=NameNode,name=NameNodeInfo");
    String result = (String) mbs.getAttribute(namenodeMXBeanName,
        "VerifyECWithTopologyResult");
    ObjectMapper mapper = new ObjectMapper();
    Map<String, String> resultMap = mapper.readValue(result, Map.class);
    Boolean isSupported = Boolean.parseBoolean(resultMap.get("isSupported"));
    String resultMessage = resultMap.get("resultMessage");

    assertFalse("Test cluster does not support all enabled " +
        "erasure coding policies.", isSupported);
    assertTrue(resultMessage.contains("3 racks are required for " +
        "the erasure coding policies: RS-6-3-1024k. " +
        "The number of racks is only 1."));
  }
}
