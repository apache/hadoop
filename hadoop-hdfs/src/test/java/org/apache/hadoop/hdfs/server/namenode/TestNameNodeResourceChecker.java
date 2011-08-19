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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NameNodeResourceMonitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestNameNodeResourceChecker {
  private Configuration conf;
  private File baseDir;
  private File nameDir;

  @Before
  public void setUp () throws IOException {
    conf = new Configuration();
    baseDir = new File(conf.get("hadoop.tmp.dir"));
    nameDir = new File(baseDir, "resource-check-name-dir");
    nameDir.mkdirs();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());
  }

  /**
   * Tests that hasAvailableDiskSpace returns true if disk usage is below
   * threshold.
   *
   * @throws IOException in case of errors
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
   * 
   * @throws IOException in case of errors
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
   * 
   * @throws IOException in case of errors
   * @throws InterruptedException 
   */
  @Test
  public void testCheckThatNameNodeResourceMonitorIsRunning()
      throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, 1);
      
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1).build();

      NameNodeResourceChecker mockResourceChecker = Mockito.mock(NameNodeResourceChecker.class);
      Mockito.when(mockResourceChecker.hasAvailableDiskSpace()).thenReturn(true);
      cluster.getNameNode().getNamesystem().nnResourceChecker = mockResourceChecker;

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
      
      Mockito.when(mockResourceChecker.hasAvailableDiskSpace()).thenReturn(false);

      // Make sure the NNRM thread has a chance to run.
      long startMillis = System.currentTimeMillis();
      while (!cluster.getNameNode().isInSafeMode() &&
          System.currentTimeMillis() < startMillis + (60 * 1000)) {
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
   * 
   * @throws IOException
   */
  @Test
  public void testChecking2NameDirsOnOneVolume() throws IOException {
    Configuration conf = new Configuration();
    File nameDir1 = new File(conf.get("hadoop.tmp.dir", "name-dir1"));
    File nameDir2 = new File(conf.get("hadoop.tmp.dir", "name-dir2"));
    nameDir1.mkdirs();
    nameDir2.mkdirs();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir1.getAbsolutePath() + "," + nameDir2.getAbsolutePath());
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, Long.MAX_VALUE);

    NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);

    assertEquals("Should not check the same volume more than once.",
        1, nb.getVolumesLowOnSpace().size());
  }

  /**
   * Tests that only a single space check is performed if extra volumes are
   * configured manually which also coincide with a volume the name dir is on.
   * 
   * @throws IOException
   */
  @Test
  public void testCheckingExtraVolumes() throws IOException {
    Configuration conf = new Configuration();
    File nameDir = new File(conf.get("hadoop.tmp.dir", "name-dir"));
    nameDir.mkdirs();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY, nameDir.getAbsolutePath());
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, Long.MAX_VALUE);

    NameNodeResourceChecker nb = new NameNodeResourceChecker(conf);

    assertEquals("Should not check the same volume more than once.",
        1, nb.getVolumesLowOnSpace().size());
  }
}
