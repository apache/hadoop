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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the ability of a DN to tolerate volume failures.
 */
public class TestDataNodeVolumeFailureToleration {
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
   * Test the DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY configuration
   * option, ie the DN tolerates a failed-to-use scenario during
   * its start-up.
   */
  @Test
  public void testValidVolumesAtStartup() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    // Make sure no DNs are running.
    cluster.shutdownDataNodes();

    // Bring up a datanode with two default data dirs, but with one bad one.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);

    // We use subdirectories 0 and 1 in order to have only a single
    // data dir's parent inject a failure.
    File tld = new File(MiniDFSCluster.getBaseDirectory(), "badData");
    File dataDir1 = new File(tld, "data1");
    File dataDir1Actual = new File(dataDir1, "1");
    dataDir1Actual.mkdirs();
    // Force an IOE to occur on one of the dfs.data.dir.
    File dataDir2 = new File(tld, "data2");
    prepareDirToFail(dataDir2);
    File dataDir2Actual = new File(dataDir2, "2");

    // Start one DN, with manually managed DN dir
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        dataDir1Actual.getPath() + "," + dataDir2Actual.getPath());
    cluster.startDataNodes(conf, 1, false, null, null);
    cluster.waitActive();

    try {
      assertTrue("The DN should have started up fine.",
          cluster.isDataNodeUp());
      DataNode dn = cluster.getDataNodes().get(0);
      String si = DataNodeTestUtils.getFSDataset(dn).getStorageInfo();
      assertTrue("The DN should have started with this directory",
          si.contains(dataDir1Actual.getPath()));
      assertFalse("The DN shouldn't have a bad directory.",
          si.contains(dataDir2Actual.getPath()));
    } finally {
      cluster.shutdownDataNodes();
      FileUtil.chmod(dataDir2.toString(), "755");
    }

  }

  /**
   * Test the DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY configuration
   * option, ie the DN shuts itself down when the number of failures
   * experienced drops below the tolerated amount.
   */
  @Test
  public void testConfigureMinValidVolumes() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));

    // Bring up two additional datanodes that need both of their volumes
    // functioning in order to stay up.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 0);
    cluster.startDataNodes(conf, 2, true, null, null);
    cluster.waitActive();
    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
        ).getDatanodeManager();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    long dnCapacity = DFSTestUtil.getDatanodeCapacity(dm, 0);

    // Fail a volume on the 2nd DN
    File dn2Vol1 = new File(dataDir, "data"+(2*1+1));
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn2Vol1, false));

    // Should only get two replicas (the first DN and the 3rd)
    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)2);

    // Check that this single failure caused a DN to die.
    DFSTestUtil.waitForDatanodeStatus(dm, 2, 1, 0, 
        origCapacity - (1*dnCapacity), WAIT_FOR_HEARTBEATS);

    // If we restore the volume we should still only be able to get
    // two replicas since the DN is still considered dead.
    assertTrue("Couldn't chmod local vol", FileUtil.setExecutable(dn2Vol1, true));
    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)2);
  }

  /** 
   * Restart the datanodes with a new volume tolerated value.
   * @param volTolerated number of dfs data dir failures to tolerate
   * @param manageDfsDirs whether the mini cluster should manage data dirs
   * @throws IOException
   */
  private void restartDatanodes(int volTolerated, boolean manageDfsDirs)
      throws IOException {
    // Make sure no datanode is running
    cluster.shutdownDataNodes();
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, volTolerated);
    cluster.startDataNodes(conf, 1, manageDfsDirs, null, null);
    cluster.waitActive();
  }

  /**
   * Test for different combination of volume configs and volumes tolerated 
   * values.
   */
  @Test
  public void testVolumeAndTolerableConfiguration() throws Exception {
    // Check if Block Pool Service exit for an invalid conf value.
    testVolumeConfig(-1, 0, false, true);

    // Ditto if the value is too big.
    testVolumeConfig(100, 0, false, true);
    
    // Test for one failed volume
    testVolumeConfig(0, 1, false, false);
    
    // Test for one failed volume with 1 tolerable volume
    testVolumeConfig(1, 1, true, false);
    
    // Test all good volumes
    testVolumeConfig(0, 0, true, false);
    
    // Test all failed volumes
    testVolumeConfig(0, 2, false, false);
  }

  /**
   * Tests for a given volumes to be tolerated and volumes failed.
   */
  private void testVolumeConfig(int volumesTolerated, int volumesFailed,
      boolean expectedBPServiceState, boolean manageDfsDirs)
      throws IOException, InterruptedException {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));
    final int dnIndex = 0;
    // Fail the current directory since invalid storage directory perms
    // get fixed up automatically on datanode startup.
    File[] dirs = {
        new File(cluster.getInstanceStorageDir(dnIndex, 0), "current"),
        new File(cluster.getInstanceStorageDir(dnIndex, 1), "current") };

    try {
      for (int i = 0; i < volumesFailed; i++) {
        prepareDirToFail(dirs[i]);
      }
      restartDatanodes(volumesTolerated, manageDfsDirs);
      assertEquals(expectedBPServiceState, cluster.getDataNodes().get(0)
          .isBPServiceAlive(cluster.getNamesystem().getBlockPoolId()));
    } finally {
      for (File dir : dirs) {
        FileUtil.chmod(dir.toString(), "755");
      }
    }
  }

  /** 
   * Prepare directories for a failure, set dir permission to 000
   * @param dir
   * @throws IOException
   * @throws InterruptedException
   */
  private void prepareDirToFail(File dir) throws IOException,
      InterruptedException {
    dir.mkdirs();
    assertEquals("Couldn't chmod local vol", 0,
        FileUtil.chmod(dir.toString(), "000"));
  }

  /**
   * Test that a volume that is considered failed on startup is seen as
   *  a failed volume by the NN.
   */
  @Test
  public void testFailedVolumeOnStartupIsCounted() throws Exception {
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));
    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
    ).getDatanodeManager();
    long origCapacity = DFSTestUtil.getLiveDatanodeCapacity(dm);
    File dir = new File(cluster.getInstanceStorageDir(0, 0), "current");

    try {
      prepareDirToFail(dir);
      restartDatanodes(1, false);
      // The cluster is up..
      assertEquals(true, cluster.getDataNodes().get(0)
          .isBPServiceAlive(cluster.getNamesystem().getBlockPoolId()));
      // but there has been a single volume failure
      DFSTestUtil.waitForDatanodeStatus(dm, 1, 0, 1,
          origCapacity / 2, WAIT_FOR_HEARTBEATS);
    } finally {
      FileUtil.chmod(dir.toString(), "755");
    }
  }
}
