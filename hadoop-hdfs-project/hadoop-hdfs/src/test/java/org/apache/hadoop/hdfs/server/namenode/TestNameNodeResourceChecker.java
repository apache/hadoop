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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NameNodeResourceMonitor;
import org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker.CheckedVolume;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNameNodeResourceChecker {
  private final static File BASE_DIR = PathUtils.getTestDir(TestNameNodeResourceChecker.class);
  private Configuration conf;
  private File baseDir;
  private File nameDir;

  @Before
  public void setUp () throws IOException {
    conf = new Configuration();
    nameDir = new File(BASE_DIR, "resource-check-name-dir");
    nameDir.mkdirs();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameDir.getAbsolutePath());
  }

  /**
   * Tests that hasAvailableDiskSpace returns true if disk usage is below
   * threshold.
   */
  @Test
  public void testCheckAvailability()
      throws IOException {
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, 0);
    NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);
    assertTrue(
        "isResourceAvailable must return true if " +
            "disk usage is lower than threshold",
        nb.hasAvailableDiskSpace());
  }

  /**
   * Tests that hasAvailableDiskSpace returns false if disk usage is above
   * threshold.
   */
  @Test
  public void testCheckAvailabilityNeg() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, Long.MAX_VALUE);
    NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);
    assertFalse(
        "isResourceAvailable must return false if " +
            "disk usage is higher than threshold",
        nb.hasAvailableDiskSpace());
  }

  /**
   * Tests that NameNode resource monitor causes the NN to enter safe mode when
   * resources are low.
   */
  @Test
  public void testCheckThatNameNodeResourceMonitorIsRunning()
      throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameDir.getAbsolutePath());
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, 1);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1).build();

      MockNameNodeResourceChecker mockResourceChecker =
          new MockNameNodeResourceChecker(conf);
      cluster.getNameNode()
          .getNamesystem().nnResourceChecker = mockResourceChecker;

      cluster.waitActive();

      String name = NameNodeResourceMonitor.class.getName();

      boolean isNameNodeMonitorRunning = false;
      Set<Thread> runningThreads = Thread.getAllStackTraces().keySet();
      for (Thread runningThread : runningThreads) {
        if (runningThread.toString().startsWith("Thread[" + name)) {
          isNameNodeMonitorRunning = true;
          break;
        }
      }
      assertTrue("NN resource monitor should be running",
          isNameNodeMonitorRunning);
      assertFalse("NN should not presently be in safe mode",
          cluster.getNameNode().isInSafeMode());

      mockResourceChecker.setResourcesAvailable(false);

      // Make sure the NNRM thread has a chance to run.
      long startMillis = Time.now();
      while (!cluster.getNameNode().isInSafeMode() &&
          Time.now() < startMillis + (60 * 1000)) {
        Thread.sleep(1000);
      }

      assertTrue("NN should be in safe mode after resources crossed threshold",
          cluster.getNameNode().isInSafeMode());
    } finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  /**
   * Tests that only a single space check is performed if two name dirs are
   * supplied which are on the same volume.
   */
  @Test
  public void testChecking2NameDirsOnOneVolume() throws IOException {
    Configuration conf = new Configuration();
    File nameDir1 = new File(BASE_DIR, "name-dir1");
    File nameDir2 = new File(BASE_DIR, "name-dir2");
    nameDir1.mkdirs();
    nameDir2.mkdirs();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        nameDir1.getAbsolutePath() + "," + nameDir2.getAbsolutePath());
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, Long.MAX_VALUE);

    NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);

    assertEquals("Should not check the same volume more than once.",
        1, nb.getVolumesLowOnSpace().size());
  }

  /**
   * Tests that only a single space check is performed if extra volumes are
   * configured manually which also coincide with a volume the name dir is on.
   */
  @Test
  public void testCheckingExtraVolumes() throws IOException {
    Configuration conf = new Configuration();
    File nameDir = new File(BASE_DIR, "name-dir");
    nameDir.mkdirs();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY, nameDir.getAbsolutePath());
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, Long.MAX_VALUE);

    NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);

    assertEquals("Should not check the same volume more than once.",
        1, nb.getVolumesLowOnSpace().size());
  }

  /**
   * Test that the NN is considered to be out of resources only once all
   * redundant configured volumes are low on resources, or when any required
   * volume is low on resources. 
   */
  @Test
  public void testLowResourceVolumePolicy() throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    File nameDir1 = new File(BASE_DIR, "name-dir1");
    File nameDir2 = new File(BASE_DIR, "name-dir2");
    nameDir1.mkdirs();
    nameDir2.mkdirs();
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        nameDir1.getAbsolutePath() + "," + nameDir2.getAbsolutePath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, 2);
    
    NameNodeResourceChecker nnrc = new NameNodeResourceChecker(conf);
    
    // For the purpose of this test, we need to force the name dirs to appear to
    // be on different volumes.
    Map<String, CheckedVolume> volumes = new HashMap<String, CheckedVolume>();
    CheckedVolume volume1 = Mockito.mock(CheckedVolume.class);
    CheckedVolume volume2 = Mockito.mock(CheckedVolume.class);
    CheckedVolume volume3 = Mockito.mock(CheckedVolume.class);
    CheckedVolume volume4 = Mockito.mock(CheckedVolume.class);
    CheckedVolume volume5 = Mockito.mock(CheckedVolume.class);
    Mockito.when(volume1.isResourceAvailable()).thenReturn(true);
    Mockito.when(volume2.isResourceAvailable()).thenReturn(true);
    Mockito.when(volume3.isResourceAvailable()).thenReturn(true);
    Mockito.when(volume4.isResourceAvailable()).thenReturn(true);
    Mockito.when(volume5.isResourceAvailable()).thenReturn(true);
    
    // Make volumes 4 and 5 required.
    Mockito.when(volume4.isRequired()).thenReturn(true);
    Mockito.when(volume5.isRequired()).thenReturn(true);
    
    volumes.put("volume1", volume1);
    volumes.put("volume2", volume2);
    volumes.put("volume3", volume3);
    volumes.put("volume4", volume4);
    volumes.put("volume5", volume5);
    nnrc.setVolumes(volumes);
    
    // Initially all dirs have space.
    assertTrue(nnrc.hasAvailableDiskSpace());
    
    // 1/3 redundant dir is low on space.
    Mockito.when(volume1.isResourceAvailable()).thenReturn(false);
    assertTrue(nnrc.hasAvailableDiskSpace());
    
    // 2/3 redundant dirs are low on space.
    Mockito.when(volume2.isResourceAvailable()).thenReturn(false);
    assertFalse(nnrc.hasAvailableDiskSpace());
    
    // Lower the minimum number of redundant volumes that must be available.
    nnrc.setMinimumReduntdantVolumes(1);
    assertTrue(nnrc.hasAvailableDiskSpace());
    
    // Just one required dir is low on space.
    Mockito.when(volume3.isResourceAvailable()).thenReturn(false);
    assertFalse(nnrc.hasAvailableDiskSpace());
    
    // Just the other required dir is low on space.
    Mockito.when(volume3.isResourceAvailable()).thenReturn(true);
    Mockito.when(volume4.isResourceAvailable()).thenReturn(false);
    assertFalse(nnrc.hasAvailableDiskSpace());
  }
}
