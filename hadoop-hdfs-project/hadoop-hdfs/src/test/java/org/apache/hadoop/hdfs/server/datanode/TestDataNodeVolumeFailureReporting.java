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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.event.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test reporting of DN volume failure counts and metrics.
 */
public class TestDataNodeVolumeFailureReporting {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataNodeVolumeFailureReporting.class);
  {
    GenericTestUtils.setLogLevel(TestDataNodeVolumeFailureReporting.LOG,
        Level.TRACE);
  }

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private Configuration conf;
  private long volumeCapacity;

  // Sleep at least 3 seconds (a 1s heartbeat plus padding) to allow
  // for heartbeats to propagate from the datanodes to the namenode.
  final int WAIT_FOR_HEARTBEATS = 3000;

  // Wait at least (2 * re-check + 10 * heartbeat) seconds for
  // a datanode to be considered dead by the namenode.  
  final int WAIT_FOR_DEATH = 15000;

  // specific the timeout for entire test class
  @Rule
  public Timeout timeout = new Timeout(120 * 1000);

  @Before
  public void setUp() throws Exception {
    // These tests use DataNodeTestUtils#injectDataDirFailure() to simulate
    // volume failures which is currently not supported on Windows.
    assumeNotWindows();
    // Allow a single volume failure (there are two volumes)
    initCluster(1, 2, 1);
  }

  @After
  public void tearDown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test that individual volume failures do not cause DNs to fail, that
   * all volumes failed on a single datanode do cause it to fail, and
   * that the capacities and liveliness is adjusted correctly in the NN.
   */
  @Test
  public void testSuccessiveVolumeFailures() throws Exception {
    // Bring up two more datanodes
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    /*
     * Calculate the total capacity of all the datanodes. Sleep for
     * three seconds to be sure the datanodes have had a chance to
     * heartbeat their capacities.
     */
    Thread.sleep(WAIT_FOR_HEARTBEATS);
    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
        ).getDatanodeManager();

    final long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);

    File dn1Vol1 = cluster.getInstanceStorageDir(0, 0);
    File dn2Vol1 = cluster.getInstanceStorageDir(1, 0);
    File dn3Vol1 = cluster.getInstanceStorageDir(2, 0);
    File dn3Vol2 = cluster.getInstanceStorageDir(2, 1);

    /*
     * Make the 1st volume directories on the first two datanodes
     * non-accessible.  We don't make all three 1st volume directories
     * readonly since that would cause the entire pipeline to
     * fail. The client does not retry failed nodes even though
     * perhaps they could succeed because just a single volume failed.
     */
    DataNodeTestUtils.injectDataDirFailure(dn1Vol1, dn2Vol1);

    /*
     * Create file1 and wait for 3 replicas (ie all DNs can still
     * store a block).  Then assert that all DNs are up, despite the
     * volume failures.
     */
    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)3);
    ArrayList<DataNode> dns = cluster.getDataNodes();
    assertTrue("DN1 should be up", dns.get(0).isDatanodeUp());
    assertTrue("DN2 should be up", dns.get(1).isDatanodeUp());
    assertTrue("DN3 should be up", dns.get(2).isDatanodeUp());

    /*
     * The metrics should confirm the volume failures.
     */
    checkFailuresAtDataNode(dns.get(0), 1, true, dn1Vol1.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(1), 1, true, dn2Vol1.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(2), 0, true);

    // Ensure we wait a sufficient amount of time
    assert (WAIT_FOR_HEARTBEATS * 10) > WAIT_FOR_DEATH;

    // Eventually the NN should report two volume failures
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2, 
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(2), true);

    /*
     * Now fail a volume on the third datanode. We should be able to get
     * three replicas since we've already identified the other failures.
     */
    DataNodeTestUtils.injectDataDirFailure(dn3Vol1);
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)3);
    assertTrue("DN3 should still be up", dns.get(2).isDatanodeUp());
    checkFailuresAtDataNode(dns.get(2), 1, true, dn3Vol1.getAbsolutePath());

    DataNodeTestUtils.triggerHeartbeat(dns.get(2));
    checkFailuresAtNameNode(dm, dns.get(2), true, dn3Vol1.getAbsolutePath());

    /*
     * Once the datanodes have a chance to heartbeat their new capacity the
     * total capacity should be down by three volumes (assuming the host
     * did not grow or shrink the data volume while the test was running).
     */
    dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 3, 
        origCapacity - (3*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 3);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(2), true, dn3Vol1.getAbsolutePath());

    /*
     * Now fail the 2nd volume on the 3rd datanode. All its volumes
     * are now failed and so it should report two volume failures
     * and that it's no longer up. Only wait for two replicas since
     * we'll never get a third.
     */
    DataNodeTestUtils.injectDataDirFailure(dn3Vol2);
    Path file3 = new Path("/test3");
    DFSTestUtil.createFile(fs, file3, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file3, (short)2);

    // And report two failed volumes
    checkFailuresAtDataNode(dns.get(2), 2, true, dn3Vol1.getAbsolutePath(),
        dn3Vol2.getAbsolutePath());

    // The DN should consider itself dead
    DFSTestUtil.waitForDatanodeDeath(dns.get(2));

    // The NN considers the DN dead
    DFSTestUtil.waitForDatanodeStatus(dm, 2, 1, 2, 
        origCapacity - (4*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());

    /*
     * The datanode never tries to restore the failed volume, even if
     * it's subsequently repaired, but it should see this volume on
     * restart, so file creation should be able to succeed after
     * restoring the data directories and restarting the datanodes.
     */
    DataNodeTestUtils.restoreDataDirFromFailure(
        dn1Vol1, dn2Vol1, dn3Vol1, dn3Vol2);
    cluster.restartDataNodes();
    cluster.waitActive();
    Path file4 = new Path("/test4");
    DFSTestUtil.createFile(fs, file4, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file4, (short)3);

    /*
     * Eventually the capacity should be restored to its original value,
     * and that the volume failure count should be reported as zero by
     * both the metrics and the NN.
     */
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 0, origCapacity, 
        WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 0);
    dns = cluster.getDataNodes();
    checkFailuresAtNameNode(dm, dns.get(0), true);
    checkFailuresAtNameNode(dm, dns.get(1), true);
    checkFailuresAtNameNode(dm, dns.get(2), true);
  }

  /**
   * Test that the NN re-learns of volume failures after restart.
   */
  @Test
  public void testVolFailureStatsPreservedOnNNRestart() throws Exception {
    // Bring up two more datanodes that can tolerate 1 failure
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
        ).getDatanodeManager();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);

    // Fail the first volume on both datanodes (we have to keep the 
    // third healthy so one node in the pipeline will not fail). 
    File dn1Vol1 = cluster.getInstanceStorageDir(0, 0);
    File dn2Vol1 = cluster.getInstanceStorageDir(1, 0);
    DataNodeTestUtils.injectDataDirFailure(dn1Vol1, dn2Vol1);

    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)2, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);
    ArrayList<DataNode> dns = cluster.getDataNodes();

    // The NN reports two volumes failures
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2, 
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());

    // After restarting the NN it still see the two failures
    cluster.restartNameNode(0);
    cluster.waitActive();
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2,
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());
  }

  @Test
  public void testMultipleVolFailuresOnNode() throws Exception {
    // Reinitialize the cluster, configured with 4 storage locations per DataNode
    // and tolerating up to 2 failures.
    tearDown();
    initCluster(3, 4, 2);

    // Calculate the total capacity of all the datanodes. Sleep for three seconds
    // to be sure the datanodes have had a chance to heartbeat their capacities.
    Thread.sleep(WAIT_FOR_HEARTBEATS);
    DatanodeManager dm = cluster.getNamesystem().getBlockManager()
        .getDatanodeManager();

    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);

    File dn1Vol1 = cluster.getInstanceStorageDir(0, 0);
    File dn1Vol2 = cluster.getInstanceStorageDir(0, 1);
    File dn2Vol1 = cluster.getInstanceStorageDir(1, 0);
    File dn2Vol2 = cluster.getInstanceStorageDir(1, 1);

    // Make the first two volume directories on the first two datanodes
    // non-accessible.
    DataNodeTestUtils.injectDataDirFailure(dn1Vol1, dn1Vol2, dn2Vol1, dn2Vol2);

    // Create file1 and wait for 3 replicas (ie all DNs can still store a block).
    // Then assert that all DNs are up, despite the volume failures.
    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)3);

    // Create additional file to trigger failure based volume check on dn1Vol2
    // and dn2Vol2.
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)3);

    ArrayList<DataNode> dns = cluster.getDataNodes();
    assertTrue("DN1 should be up", dns.get(0).isDatanodeUp());
    assertTrue("DN2 should be up", dns.get(1).isDatanodeUp());
    assertTrue("DN3 should be up", dns.get(2).isDatanodeUp());

    checkFailuresAtDataNode(dns.get(0), 1, true, dn1Vol1.getAbsolutePath(),
        dn1Vol2.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(1), 1, true, dn2Vol1.getAbsolutePath(),
        dn2Vol2.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(2), 0, true);

    // Ensure we wait a sufficient amount of time
    assert (WAIT_FOR_HEARTBEATS * 10) > WAIT_FOR_DEATH;

    // Eventually the NN should report four volume failures
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 4,
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 4);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath(),
        dn1Vol2.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath(),
        dn2Vol2.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(2), true);
  }

  @Test
  public void testDataNodeReconfigureWithVolumeFailures() throws Exception {
    // Bring up two more datanodes
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
        ).getDatanodeManager();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);

    // Fail the first volume on both datanodes (we have to keep the
    // third healthy so one node in the pipeline will not fail).
    File dn1Vol1 = cluster.getInstanceStorageDir(0, 0);
    File dn1Vol2 = cluster.getInstanceStorageDir(0, 1);
    File dn2Vol1 = cluster.getInstanceStorageDir(1, 0);
    File dn2Vol2 = cluster.getInstanceStorageDir(1, 1);
    DataNodeTestUtils.injectDataDirFailure(dn1Vol1);
    DataNodeTestUtils.injectDataDirFailure(dn2Vol1);

    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)2, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);

    ArrayList<DataNode> dns = cluster.getDataNodes();
    assertTrue("DN1 should be up", dns.get(0).isDatanodeUp());
    assertTrue("DN2 should be up", dns.get(1).isDatanodeUp());
    assertTrue("DN3 should be up", dns.get(2).isDatanodeUp());

    checkFailuresAtDataNode(dns.get(0), 1, true, dn1Vol1.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(1), 1, true, dn2Vol1.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(2), 0, true);

    // Ensure we wait a sufficient amount of time
    assert (WAIT_FOR_HEARTBEATS * 10) > WAIT_FOR_DEATH;

    // The NN reports two volume failures
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2,
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());

    // Reconfigure again to try to add back the failed volumes.
    DataNodeTestUtils.reconfigureDataNode(dns.get(0), dn1Vol1, dn1Vol2);
    DataNodeTestUtils.reconfigureDataNode(dns.get(1), dn2Vol1, dn2Vol2);

    DataNodeTestUtils.triggerHeartbeat(dns.get(0));
    DataNodeTestUtils.triggerHeartbeat(dns.get(1));

    checkFailuresAtDataNode(dns.get(0), 1, true, dn1Vol1.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(1), 1, true, dn2Vol1.getAbsolutePath());

    // Ensure we wait a sufficient amount of time.
    assert (WAIT_FOR_HEARTBEATS * 10) > WAIT_FOR_DEATH;

    // The NN reports two volume failures again.
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2,
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());

    // Reconfigure a third time with the failed volumes.  Afterwards, we expect
    // the same volume failures to be reported.  (No double-counting.)
    DataNodeTestUtils.reconfigureDataNode(dns.get(0), dn1Vol1, dn1Vol2);
    DataNodeTestUtils.reconfigureDataNode(dns.get(1), dn2Vol1, dn2Vol2);

    DataNodeTestUtils.triggerHeartbeat(dns.get(0));
    DataNodeTestUtils.triggerHeartbeat(dns.get(1));

    checkFailuresAtDataNode(dns.get(0), 1, true, dn1Vol1.getAbsolutePath());
    checkFailuresAtDataNode(dns.get(1), 1, true, dn2Vol1.getAbsolutePath());

    // Ensure we wait a sufficient amount of time.
    assert (WAIT_FOR_HEARTBEATS * 10) > WAIT_FOR_DEATH;

    // The NN reports two volume failures again.
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2,
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 2);
    checkFailuresAtNameNode(dm, dns.get(0), true, dn1Vol1.getAbsolutePath());
    checkFailuresAtNameNode(dm, dns.get(1), true, dn2Vol1.getAbsolutePath());

    // Replace failed volume with healthy volume and run reconfigure DataNode.
    // The failed volume information should be cleared.
    DataNodeTestUtils.restoreDataDirFromFailure(dn1Vol1, dn2Vol1);
    DataNodeTestUtils.reconfigureDataNode(dns.get(0), dn1Vol1, dn1Vol2);
    DataNodeTestUtils.reconfigureDataNode(dns.get(1), dn2Vol1, dn2Vol2);

    DataNodeTestUtils.triggerHeartbeat(dns.get(0));
    DataNodeTestUtils.triggerHeartbeat(dns.get(1));

    checkFailuresAtDataNode(dns.get(0), 1, true);
    checkFailuresAtDataNode(dns.get(1), 1, true);

    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 0,
        origCapacity, WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(true, 0);
    checkFailuresAtNameNode(dm, dns.get(0), true);
    checkFailuresAtNameNode(dm, dns.get(1), true);
  }

  @Test
  public void testAutoFormatEmptyDirectory() throws Exception {
    // remove the version file
    File dn1Vol1 = cluster.getStorageDir(0, 0);
    File current = new File(dn1Vol1, "current");
    File currentVersion = new File(current, "VERSION");
    currentVersion.delete();
    // restart the data node
    assertTrue(cluster.restartDataNodes(true));
    // the DN should tolerate one volume failure.
    cluster.waitActive();
    ArrayList<DataNode> dns = cluster.getDataNodes();
    DataNode dn = dns.get(0);
    assertFalse("DataNode should not reformat if VERSION is missing",
        currentVersion.exists());

    // Make sure DN's JMX sees the failed volume
    final String[] expectedFailedVolumes = {dn1Vol1.getAbsolutePath()};
    DataNodeTestUtils.triggerHeartbeat(dn);
    FsDatasetSpi<?> fsd = dn.getFSDataset();
    assertEquals(expectedFailedVolumes.length, fsd.getNumFailedVolumes());
    assertArrayEquals(expectedFailedVolumes,
        convertToAbsolutePaths(fsd.getFailedStorageLocations()));
    // there shouldn't be any more volume failures due to I/O failure
    checkFailuresAtDataNode(dn, 0, false, expectedFailedVolumes);

    // The NN reports one volume failures
    final DatanodeManager dm = cluster.getNamesystem().getBlockManager().
        getDatanodeManager();
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);
    DFSTestUtil.waitForDatanodeStatus(dm, 1, 0, 1,
        (1*dnCapacity), WAIT_FOR_HEARTBEATS);
    checkAggregateFailuresAtNameNode(false, 1);
    checkFailuresAtNameNode(dm, dns.get(0), false, dn1Vol1.getAbsolutePath());
  }

  @Test
  public void testAutoFormatEmptyBlockPoolDirectory() throws Exception {
    // remove the version file
    DataNode dn = cluster.getDataNodes().get(0);
    String bpid = cluster.getNamesystem().getBlockPoolId();
    BlockPoolSliceStorage bps = dn.getStorage().getBPStorage(bpid);
    Storage.StorageDirectory dir = bps.getStorageDir(0);
    File current = dir.getCurrentDir();

    File currentVersion = new File(current, "VERSION");
    currentVersion.delete();
    // restart the data node
    assertTrue(cluster.restartDataNodes(true));
    // the DN should tolerate one volume failure.
    cluster.waitActive();
    assertFalse("DataNode should not reformat if VERSION is missing",
        currentVersion.exists());
  }

  /**
   * Verify DataNode NumFailedVolumes and FailedStorageLocations
   * after hot swap out of failed volume.
   */
  @Test
  public void testHotSwapOutFailedVolumeAndReporting()
          throws Exception {
    final File dn0Vol1 = cluster.getInstanceStorageDir(0, 0);
    final File dn0Vol2 = cluster.getInstanceStorageDir(0, 1);
    final DataNode dn0 = cluster.getDataNodes().get(0);
    final String oldDataDirs = dn0.getConf().get(
            DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=DataNode,name=FSDatasetState-" + dn0.getDatanodeUuid());
    int numFailedVolumes = (int) mbs.getAttribute(mxbeanName,
        "NumFailedVolumes");
    Assert.assertEquals(dn0.getFSDataset().getNumFailedVolumes(),
        numFailedVolumes);
    checkFailuresAtDataNode(dn0, 0, false, new String[] {});

    // Fail dn0Vol1 first.
    // Verify NumFailedVolumes and FailedStorageLocations are empty.
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    DataNodeTestUtils.waitForDiskError(dn0,
        DataNodeTestUtils.getVolume(dn0, dn0Vol1));
    numFailedVolumes = (int) mbs.getAttribute(mxbeanName, "NumFailedVolumes");
    Assert.assertEquals(1, numFailedVolumes);
    Assert.assertEquals(dn0.getFSDataset().getNumFailedVolumes(),
            numFailedVolumes);
    checkFailuresAtDataNode(dn0, 1, true,
        new String[] {dn0Vol1.getAbsolutePath()});

    // Reconfigure disks without fixing the failed disk.
    // Verify NumFailedVolumes and FailedStorageLocations haven't changed.
    try {
      dn0.reconfigurePropertyImpl(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
          oldDataDirs);
      fail("Reconfigure with failed disk should throw exception.");
    } catch (ReconfigurationException e) {
      Assert.assertTrue("Reconfigure exception doesn't have expected path!",
          e.getCause().getMessage().contains(dn0Vol1.getAbsolutePath()));
    }
    numFailedVolumes = (int) mbs.getAttribute(mxbeanName, "NumFailedVolumes");
    Assert.assertEquals(1, numFailedVolumes);
    Assert.assertEquals(dn0.getFSDataset().getNumFailedVolumes(),
        numFailedVolumes);
    checkFailuresAtDataNode(dn0, 1, true,
        new String[] {dn0Vol1.getAbsolutePath()});

    // Hot swap out the failed volume.
    // Verify NumFailedVolumes and FailedStorageLocations are reset.
    String dataDirs = dn0Vol2.getPath();
    dn0.reconfigurePropertyImpl(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
            dataDirs);
    numFailedVolumes = (int) mbs.getAttribute(mxbeanName, "NumFailedVolumes");
    Assert.assertEquals(0, numFailedVolumes);
    Assert.assertEquals(dn0.getFSDataset().getNumFailedVolumes(),
            numFailedVolumes);
    checkFailuresAtDataNode(dn0, 0, true, new String[] {});

    // Fix failure volume dn0Vol1 and remount it back.
    // Verify NumFailedVolumes and FailedStorageLocations are empty.
    DataNodeTestUtils.restoreDataDirFromFailure(dn0Vol1);
    dn0.reconfigurePropertyImpl(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
            oldDataDirs);
    numFailedVolumes = (int) mbs.getAttribute(mxbeanName, "NumFailedVolumes");
    Assert.assertEquals(0, numFailedVolumes);
    Assert.assertEquals(dn0.getFSDataset().getNumFailedVolumes(),
        numFailedVolumes);
    checkFailuresAtDataNode(dn0, 0, true, new String[] {});

    // Fail dn0Vol2.
    // Verify NumFailedVolumes and FailedStorageLocations are updated.
    DataNodeTestUtils.injectDataDirFailure(dn0Vol2);
    DataNodeTestUtils.waitForDiskError(dn0,
        DataNodeTestUtils.getVolume(dn0, dn0Vol2));
    numFailedVolumes = (int) mbs.getAttribute(mxbeanName, "NumFailedVolumes");
    Assert.assertEquals(1, numFailedVolumes);
    Assert.assertEquals(dn0.getFSDataset().getNumFailedVolumes(),
        numFailedVolumes);
    checkFailuresAtDataNode(dn0, 1, true,
        new String[] {dn0Vol2.getAbsolutePath()});

    // Verify DataNode tolerating one disk failure.
    assertTrue(dn0.shouldRun());
  }

  /**
   * Checks the NameNode for correct values of aggregate counters tracking failed
   * volumes across all DataNodes.
   *
   * @param expectCapacityKnown if true, then expect that the capacities of the
   *     volumes were known before the failures, and therefore the lost capacity
   *     can be reported
   * @param expectedVolumeFailuresTotal expected number of failed volumes
   */
  private void checkAggregateFailuresAtNameNode(boolean expectCapacityKnown,
      int expectedVolumeFailuresTotal) {
    FSNamesystem ns = cluster.getNamesystem();
    assertEquals(expectedVolumeFailuresTotal, ns.getVolumeFailuresTotal());
    long expectedCapacityLost = getExpectedCapacityLost(expectCapacityKnown,
        expectedVolumeFailuresTotal);
    assertEquals(expectedCapacityLost, ns.getEstimatedCapacityLostTotal());
  }

  /**
   * Checks a DataNode for correct reporting of failed volumes.
   *
   * @param dn DataNode to check
   * @param expectedVolumeFailuresCounter metric counter value for
   *     VolumeFailures.  The current implementation actually counts the number
   *     of failed disk checker cycles, which may be different from the length of
   *     expectedFailedVolumes if multiple disks fail in the same disk checker
   *     cycle
   * @param expectCapacityKnown if true, then expect that the capacities of the
   *     volumes were known before the failures, and therefore the lost capacity
   *     can be reported
   * @param expectedFailedVolumes expected locations of failed volumes
   * @throws Exception if there is any failure
   */
  private void checkFailuresAtDataNode(DataNode dn,
      long expectedVolumeFailuresCounter, boolean expectCapacityKnown,
      String... expectedFailedVolumes) throws Exception {
    FsDatasetSpi<?> fsd = dn.getFSDataset();
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("expectedFailedVolumes is ");
    for (String expected: expectedFailedVolumes) {
      strBuilder.append(expected + ",");
    }
    strBuilder.append(" fsd.getFailedStorageLocations() is ");
    for (String expected: fsd.getFailedStorageLocations()) {
      strBuilder.append(expected + ",");
    }
    LOG.info(strBuilder.toString());
    final long actualVolumeFailures =
        getLongCounter("VolumeFailures", getMetrics(dn.getMetrics().name()));
    assertTrue("Actual async detected volume failures should be greater or " +
        "equal than " + expectedFailedVolumes,
        actualVolumeFailures >= expectedVolumeFailuresCounter);
    assertEquals(expectedFailedVolumes.length, fsd.getNumFailedVolumes());
    assertArrayEquals(expectedFailedVolumes,
        convertToAbsolutePaths(fsd.getFailedStorageLocations()));
    if (expectedFailedVolumes.length > 0) {
      assertTrue(fsd.getLastVolumeFailureDate() > 0);
      long expectedCapacityLost = getExpectedCapacityLost(expectCapacityKnown,
          expectedFailedVolumes.length);
      assertEquals(expectedCapacityLost, fsd.getEstimatedCapacityLostTotal());
    } else {
      assertEquals(0, fsd.getLastVolumeFailureDate());
      assertEquals(0, fsd.getEstimatedCapacityLostTotal());
    }
  }

  /**
   * Checks NameNode tracking of a particular DataNode for correct reporting of
   * failed volumes.
   *
   * @param dm DatanodeManager to check
   * @param dn DataNode to check
   * @param expectCapacityKnown if true, then expect that the capacities of the
   *     volumes were known before the failures, and therefore the lost capacity
   *     can be reported
   * @param expectedFailedVolumes expected locations of failed volumes
   * @throws Exception if there is any failure
   */
  private void checkFailuresAtNameNode(DatanodeManager dm, DataNode dn,
      boolean expectCapacityKnown, String... expectedFailedVolumes)
      throws Exception {
    DatanodeDescriptor dd = cluster.getNamesystem().getBlockManager()
        .getDatanodeManager().getDatanode(dn.getDatanodeId());
    assertEquals(expectedFailedVolumes.length, dd.getVolumeFailures());
    VolumeFailureSummary volumeFailureSummary = dd.getVolumeFailureSummary();
    if (expectedFailedVolumes.length > 0) {
      assertArrayEquals(expectedFailedVolumes,
          convertToAbsolutePaths(volumeFailureSummary
              .getFailedStorageLocations()));
      assertTrue(volumeFailureSummary.getLastVolumeFailureDate() > 0);
      long expectedCapacityLost = getExpectedCapacityLost(expectCapacityKnown,
          expectedFailedVolumes.length);
      assertEquals(expectedCapacityLost,
          volumeFailureSummary.getEstimatedCapacityLostTotal());
    } else {
      assertNull(volumeFailureSummary);
    }
  }

  /**
   * Converts the provided paths to absolute file paths.
   * @param locations the array of paths
   * @return array of absolute paths
   */
  private String[] convertToAbsolutePaths(String[] locations) {
    if (locations == null || locations.length == 0) {
      return new String[0];
    }

    String[] absolutePaths = new String[locations.length];
    for (int count = 0; count < locations.length; count++) {
      try {
        absolutePaths[count] = new File(new URI(locations[count]))
            .getAbsolutePath();
      } catch (URISyntaxException e) {
        //if the provided location is not an URI,
        //we use it as the absolute path
        absolutePaths[count] = locations[count];
      }
    }
    return absolutePaths;
  }

  /**
   * Returns expected capacity lost for use in assertions.  The return value is
   * dependent on whether or not it is expected that the volume capacities were
   * known prior to the failures.
   *
   * @param expectCapacityKnown if true, then expect that the capacities of the
   *     volumes were known before the failures, and therefore the lost capacity
   *     can be reported
   * @param expectedVolumeFailuresTotal expected number of failed volumes
   * @return estimated capacity lost in bytes
   */
  private long getExpectedCapacityLost(boolean expectCapacityKnown,
      int expectedVolumeFailuresTotal) {
    return expectCapacityKnown ? expectedVolumeFailuresTotal * volumeCapacity :
        0;
  }

  /**
   * Initializes the cluster.
   *
   * @param numDataNodes number of datanodes
   * @param storagesPerDatanode number of storage locations on each datanode
   * @param failedVolumesTolerated number of acceptable volume failures
   * @throws Exception if there is any failure
   */
  private void initCluster(int numDataNodes, int storagesPerDatanode,
      int failedVolumesTolerated) throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512L);
    /*
     * Lower the DN heartbeat, DF rate, and recheck interval to one second
     * so state about failures and datanode death propagates faster.
     */
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_DF_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
        failedVolumesTolerated);
    conf.setTimeDuration(DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
        0, TimeUnit.MILLISECONDS);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes)
        .storagesPerDatanode(storagesPerDatanode).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(
        cluster.getNamesystem().getBlockManager().getDatanodeManager(), 0);
    volumeCapacity = dnCapacity / cluster.getStoragesPerDatanode();
  }
}
