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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test reporting of DN volume failure counts and metrics.
 */
public class TestDataNodeVolumeFailureReporting {

  private static final Log LOG = LogFactory.getLog(TestDataNodeVolumeFailureReporting.class);
  {
    ((Log4JLogger)TestDataNodeVolumeFailureReporting.LOG).getLogger().setLevel(Level.ALL);
  }

  private FileSystem fs;
  private MiniDFSCluster cluster;
  private Configuration conf;
  private String dataDir;

  // Sleep at least 3 seconds (a 1s heartbeat plus padding) to allow
  // for heartbeats to propagate from the datanodes to the namenode.
  final int WAIT_FOR_HEARTBEATS = 3000;

  // Wait at least (2 * re-check + 10 * heartbeat) seconds for
  // a datanode to be considered dead by the namenode.  
  final int WAIT_FOR_DEATH = 15000;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512L);
    /*
     * Lower the DN heartbeat, DF rate, and recheck interval to one second
     * so state about failures and datanode death propagates faster.
     */
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_DF_INTERVAL_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    // Allow a single volume failure (there are two volumes)
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dataDir = cluster.getDataDirectory();
  }

  @After
  public void tearDown() throws Exception {
    for (int i = 0; i < 3; i++) {
      FileUtil.setExecutable(new File(dataDir, "data"+(2*i+1)), true);
      FileUtil.setExecutable(new File(dataDir, "data"+(2*i+2)), true);
    }
    cluster.shutdown();
  }

  /**
   * Test that individual volume failures do not cause DNs to fail, that
   * all volumes failed on a single datanode do cause it to fail, and
   * that the capacities and liveliness is adjusted correctly in the NN.
   */
  @Test
  public void testSuccessiveVolumeFailures() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

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

    File dn1Vol1 = new File(dataDir, "data"+(2*0+1));
    File dn2Vol1 = new File(dataDir, "data"+(2*1+1));
    File dn3Vol1 = new File(dataDir, "data"+(2*2+1));
    File dn3Vol2 = new File(dataDir, "data"+(2*2+2));

    /*
     * Make the 1st volume directories on the first two datanodes
     * non-accessible.  We don't make all three 1st volume directories
     * readonly since that would cause the entire pipeline to
     * fail. The client does not retry failed nodes even though
     * perhaps they could succeed because just a single volume failed.
     */
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn1Vol1, false));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn2Vol1, false));

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
    assertCounter("VolumeFailures", 1L, 
        getMetrics(dns.get(0).getMetrics().name()));
    assertCounter("VolumeFailures", 1L, 
        getMetrics(dns.get(1).getMetrics().name()));
    assertCounter("VolumeFailures", 0L, 
        getMetrics(dns.get(2).getMetrics().name()));

    // Ensure we wait a sufficient amount of time
    assert (WAIT_FOR_HEARTBEATS * 10) > WAIT_FOR_DEATH;

    // Eventually the NN should report two volume failures
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2, 
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);

    /*
     * Now fail a volume on the third datanode. We should be able to get
     * three replicas since we've already identified the other failures.
     */
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn3Vol1, false));
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)3);
    assertTrue("DN3 should still be up", dns.get(2).isDatanodeUp());
    assertCounter("VolumeFailures", 1L, 
        getMetrics(dns.get(2).getMetrics().name()));

    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    dm.fetchDatanodes(live, dead, false);
    live.clear();
    dead.clear();
    dm.fetchDatanodes(live, dead, false);
    assertEquals("DN3 should have 1 failed volume",
        1, live.get(2).getVolumeFailures());

    /*
     * Once the datanodes have a chance to heartbeat their new capacity the
     * total capacity should be down by three volumes (assuming the host
     * did not grow or shrink the data volume while the test was running).
     */
    dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 3, 
        origCapacity - (3*dnCapacity), WAIT_FOR_HEARTBEATS);

    /*
     * Now fail the 2nd volume on the 3rd datanode. All its volumes
     * are now failed and so it should report two volume failures
     * and that it's no longer up. Only wait for two replicas since
     * we'll never get a third.
     */
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn3Vol2, false));
    Path file3 = new Path("/test3");
    DFSTestUtil.createFile(fs, file3, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file3, (short)2);

    // The DN should consider itself dead
    DFSTestUtil.waitForDatanodeDeath(dns.get(2));

    // And report two failed volumes
    assertCounter("VolumeFailures", 2L, 
        getMetrics(dns.get(2).getMetrics().name()));

    // The NN considers the DN dead
    DFSTestUtil.waitForDatanodeStatus(dm, 2, 1, 2, 
        origCapacity - (4*dnCapacity), WAIT_FOR_HEARTBEATS);

    /*
     * The datanode never tries to restore the failed volume, even if
     * it's subsequently repaired, but it should see this volume on
     * restart, so file creation should be able to succeed after
     * restoring the data directories and restarting the datanodes.
     */
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn1Vol1, true));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn2Vol1, true));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn3Vol1, true));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn3Vol2, true));
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
  }

  /**
   * Test that the NN re-learns of volume failures after restart.
   */
  @Test
  public void testVolFailureStatsPreservedOnNNRestart() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    // Bring up two more datanodes that can tolerate 1 failure
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
        ).getDatanodeManager();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);

    // Fail the first volume on both datanodes (we have to keep the 
    // third healthy so one node in the pipeline will not fail). 
    File dn1Vol1 = new File(dataDir, "data"+(2*0+1));
    File dn2Vol1 = new File(dataDir, "data"+(2*1+1));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn1Vol1, false));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn2Vol1, false));

    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)2, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);

    // The NN reports two volumes failures
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2, 
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);

    // After restarting the NN it still see the two failures
    cluster.restartNameNode(0);
    cluster.waitActive();
    DFSTestUtil.waitForDatanodeStatus(dm, 3, 0, 2,
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);
  }
}
