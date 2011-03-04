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

import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test successive volume failures, failure metrics and capacity reporting.
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
    cluster.shutdown();
  }

  /**
   * Test that individual volume failures do not cause DNs to fail, that
   * all volumes failed on a single datanode do cause it to fail, and
   * that the capacities and liveliness is adjusted correctly in the NN.
   */
  @Test
  public void testSuccessiveVolumeFailures() throws Exception {
    if (System.getProperty("os.name").startsWith("Windows")) {
      // See above
      return;
    }
    // Bring up two more datanodes
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    /*
     * Sleep at least 3 seconds (a 1s heartbeat plus padding) to allow
     * for heartbeats to propagate from the datanodes to the namenode.
     * Sleep  at least (2 * re-check + 10 * heartbeat) 12 seconds for
     * a datanode  to be called dead by the namenode.
     */
    final int WAIT_FOR_HEARTBEATS = 3000;
    final int WAIT_FOR_DEATH = 15000;

    /*
     * Calculate the total capacity of all the datanodes. Sleep for
     * three seconds to be sure the datanodes have had a chance to
     * heartbeat their capacities.
     */
    Thread.sleep(WAIT_FOR_HEARTBEATS);
    FSNamesystem namesystem = cluster.getNamesystem();
    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    namesystem.DFSNodesStatus(live, dead);
    assertEquals("All DNs should be live", 3, live.size());
    assertEquals("All DNs should be live", 0, dead.size());
    long origCapacity = 0;
    for (final DatanodeDescriptor dn : live) {
      origCapacity += dn.getCapacity();
      assertEquals("DN "+dn+" vols should be healthy",
          0, dn.getVolumeFailures());
    }

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
    assertTrue("Couldn't chmod local vol", dn1Vol1.setExecutable(false));
    assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(false));

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
    DataNodeMetrics metrics1 = dns.get(0).getMetrics();
    DataNodeMetrics metrics2 = dns.get(1).getMetrics();
    DataNodeMetrics metrics3 = dns.get(2).getMetrics();
    assertEquals("Vol1 should report 1 failure",
        1, metrics1.volumesFailed.getCurrentIntervalValue());
    assertEquals("Vol2 should report 1 failure",
        1, metrics2.volumesFailed.getCurrentIntervalValue());
    assertEquals("Vol3 should have no failures",
        0, metrics3.volumesFailed.getCurrentIntervalValue());

    // Eventually the NN should report two volume failures as well
    while (true) {
      Thread.sleep(WAIT_FOR_HEARTBEATS);
      live.clear();
      dead.clear();
      namesystem.DFSNodesStatus(live, dead);
      int volumeFailures = 0;
      for (final DatanodeDescriptor dn : live) {
        volumeFailures += dn.getVolumeFailures();
      }
      if (2 == volumeFailures) {
        break;
      }
      LOG.warn("Still waiting for volume failures: "+volumeFailures);
    }

    /*
     * Now fail a volume on the third datanode. We should be able to get
     * three replicas since we've already identified the other failures.
     */
    assertTrue("Couldn't chmod local vol", dn3Vol1.setExecutable(false));
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)3);
    assertTrue("DN3 should still be up", dns.get(2).isDatanodeUp());
    assertEquals("Vol3 should report 1 failure",
        1, metrics3.volumesFailed.getCurrentIntervalValue());
    live.clear();
    dead.clear();
    namesystem.DFSNodesStatus(live, dead);
    assertEquals("DN3 should have 1 failed volume",
        1, live.get(2).getVolumeFailures());

    /*
     * Once the datanodes have a chance to heartbeat their new capacity the
     * total capacity should be down by three volumes (assuming the host
     * did not grow or shrink the data volume while the test was running).
     */
    while (true) {
      Thread.sleep(WAIT_FOR_HEARTBEATS);
      live.clear();
      dead.clear();
      namesystem.DFSNodesStatus(live, dead);
      long currCapacity = 0;
      long singleVolCapacity = live.get(0).getCapacity();
      for (final DatanodeDescriptor dn : live) {
        currCapacity += dn.getCapacity();
      }
      LOG.info("Live: "+live.size()+" Dead: "+dead.size());
      LOG.info("Original capacity: "+origCapacity);
      LOG.info("Current capacity: "+currCapacity);
      LOG.info("Volume capacity: "+singleVolCapacity);
      if (3 == live.size() && 0 == dead.size() &&
          origCapacity == (currCapacity + (3 * singleVolCapacity))) {
        break;
      }
    }

    /*
     * Now fail the 2nd volume on the 3rd datanode. All its volumes
     * are now failed and so it should report two volume failures
     * and that it's no longer up. Only wait for two replicas since
     * we'll never get a third.
     */
    assertTrue("Couldn't chmod local vol", dn3Vol2.setExecutable(false));
    Path file3 = new Path("/test3");
    DFSTestUtil.createFile(fs, file3, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file3, (short)2);
    // Eventually the DN should go down
    while (dns.get(2).isDatanodeUp()) {
      Thread.sleep(1000);
    }
    // and report two failed volumes
    metrics3 = dns.get(2).getMetrics();
    assertEquals("DN3 should report 2 vol failures",
        2, metrics3.volumesFailed.getCurrentIntervalValue());
    // and eventually be seen as dead by the NN.
    while (true) {
      Thread.sleep(WAIT_FOR_DEATH);
      live.clear();
      dead.clear();
      namesystem.DFSNodesStatus(live, dead);
      if (1 == dead.size() && 2 == live.size()) {
        break;
      }
      LOG.warn("Still waiting for dn to die: "+dead.size());
    }

    /*
     * The datanode never tries to restore the failed volume, even if
     * it's subsequently repaired, but it should see this volume on
     * restart, so file creation should be able to succeed after
     * restoring the data directories and restarting the datanodes.
     */
    assertTrue("Couldn't chmod local vol", dn1Vol1.setExecutable(true));
    assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(true));
    assertTrue("Couldn't chmod local vol", dn3Vol1.setExecutable(true));
    assertTrue("Couldn't chmod local vol", dn3Vol2.setExecutable(true));
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
    while (true) {
      Thread.sleep(WAIT_FOR_DEATH);
      live.clear();
      dead.clear();
      namesystem.DFSNodesStatus(live, dead);
      assertEquals("All DNs should be live", 3, live.size());
      assertEquals("All DNs should be live", 0, dead.size());
      long currCapacity = 0;
      long volFailures = 0;
      for (final DatanodeDescriptor dn : live) {
        currCapacity += dn.getCapacity();
        volFailures += dn.getVolumeFailures();
      }
      if (3 == live.size() && 0 == dead.size() && 0 == volFailures &&
          origCapacity == currCapacity) {
        break;
      }
      LOG.warn("Waiting for capacity: original="+origCapacity+" current="+
          currCapacity+" live="+live.size()+" dead="+dead.size()+
          " vols="+volFailures);
    }
  }

  /**
   * Test the DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY configuration
   * option, ie the DN shuts itself down when the number of failures
   * experienced drops below the tolerated amount.
   */
  @Test
  public void testConfigureMinValidVolumes() throws Exception {
    if (System.getProperty("os.name").startsWith("Windows")) {
      // See above
      return;
    }

    // Bring up two additional datanodes that need both of their volumes
    // functioning in order to stay up.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 0);
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();

    // Fail a volume on the 2nd DN
    File dn2Vol1 = new File(dataDir, "data"+(2*1+1));
    assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(false));

    // Should only get two replicas (the first DN and the 3rd)
    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);

    // Check that this single failure caused a DN to die.
    while (true) {
      final int WAIT_FOR_DEATH = 15000;
      Thread.sleep(WAIT_FOR_DEATH);
      FSNamesystem namesystem = cluster.getNamesystem();
      ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      namesystem.DFSNodesStatus(live, dead);
      if (1 == dead.size()) {
        break;
      }
      LOG.warn("Waiting for datanode to die: "+dead.size());
    }

    // If we restore the volume we should still only be able to get
    // two replicas since the DN is still considered dead.
    assertTrue("Couldn't chmod local vol", dn2Vol1.setExecutable(true));
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)2);
  }

  /**
   * Test invalid DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY values.
   */
  @Test
  public void testInvalidFailedVolumesConfig() throws Exception {
    if (System.getProperty("os.name").startsWith("Windows")) {
      // See above
      return;
    }
    /*
     * Bring up another datanode that has an invalid value set.
     * We should still be able to create a file with two replicas
     * since the minimum valid volume parameter is only checked
     * when we experience a disk error.
     */
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, -1);
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();
    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)2, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);
    // Ditto if the value is too big.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 100);
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();
    Path file2 = new Path("/test1");
    DFSTestUtil.createFile(fs, file2, 1024, (short)2, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)2);
  }
}
