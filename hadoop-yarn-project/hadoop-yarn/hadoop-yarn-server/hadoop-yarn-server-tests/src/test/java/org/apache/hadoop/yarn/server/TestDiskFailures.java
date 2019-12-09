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

package org.apache.hadoop.yarn.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify if NodeManager's in-memory good local dirs list and good log dirs list
 * get updated properly when disks(nm-local-dirs and nm-log-dirs) fail. Also
 * verify if the overall health status of the node gets updated properly when
 * specified percentage of disks fail.
 */
public class TestDiskFailures {

  private static final Logger LOG = LoggerFactory.getLogger(TestDiskFailures.class);

  private static final long DISK_HEALTH_CHECK_INTERVAL = 1000;//1 sec

  private static FileContext localFS = null;
  private static final File testDir = new File("target",
      TestDiskFailures.class.getName()).getAbsoluteFile();
  private static final File localFSDirBase = new File(testDir,
      TestDiskFailures.class.getName() + "-localDir");
  private static final int numLocalDirs = 4;
  private static final int numLogDirs = 4;

  private static MiniYARNCluster yarnCluster;
  LocalDirsHandlerService dirsHandler;

  @BeforeClass
  public static void setup() throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    localFS = FileContext.getLocalFSFileContext();
    localFS.delete(new Path(localFSDirBase.getAbsolutePath()), true);
    localFSDirBase.mkdirs();
    // Do not start cluster here
  }

  @AfterClass
  public static void teardown() {
    if (yarnCluster != null) {
      yarnCluster.stop();
      yarnCluster = null;
    }
    FileUtil.fullyDelete(localFSDirBase);
  }

  /**
   * Make local-dirs fail/inaccessible and verify if NodeManager can
   * recognize the disk failures properly and can update the list of
   * local-dirs accordingly with good disks. Also verify the overall
   * health status of the node.
   * @throws IOException
   */
  @Test
  public void testLocalDirsFailures() throws IOException {
    testDirsFailures(true);
  }

  /**
   * Make log-dirs fail/inaccessible and verify if NodeManager can
   * recognize the disk failures properly and can update the list of
   * log-dirs accordingly with good disks. Also verify the overall health
   * status of the node.
   * @throws IOException
   */  
  @Test
  public void testLogDirsFailures() throws IOException {
    testDirsFailures(false);
  }

  /**
   * Make a local and log directory inaccessible during initialization
   * and verify those bad directories are recognized and removed from
   * the list of available local and log directories.
   * @throws IOException
   */
  @Test
  public void testDirFailuresOnStartup() throws IOException {
    Configuration conf = new YarnConfiguration();
    String localDir1 = new File(testDir, "localDir1").getPath();
    String localDir2 = new File(testDir, "localDir2").getPath();
    String logDir1 = new File(testDir, "logDir1").getPath();
    String logDir2 = new File(testDir, "logDir2").getPath();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir1 + "," + localDir2);
    conf.set(YarnConfiguration.NM_LOG_DIRS, logDir1 + "," + logDir2);

    prepareDirToFail(localDir1);
    prepareDirToFail(logDir2);

    LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
    dirSvc.init(conf);
    List<String> localDirs = dirSvc.getLocalDirs();
    Assert.assertEquals(1, localDirs.size());
    Assert.assertEquals(new Path(localDir2).toString(), localDirs.get(0));
    List<String> logDirs = dirSvc.getLogDirs();
    Assert.assertEquals(1, logDirs.size());
    Assert.assertEquals(new Path(logDir1).toString(), logDirs.get(0));
  }

  private void testDirsFailures(boolean localORLogDirs) throws IOException {
    String dirType = localORLogDirs ? "local" : "log";
    String dirsProperty = localORLogDirs ? YarnConfiguration.NM_LOCAL_DIRS
                                         : YarnConfiguration.NM_LOG_DIRS;

    Configuration conf = new Configuration();
    // set disk health check interval to a small value (say 1 sec).
    conf.setLong(YarnConfiguration.NM_DISK_HEALTH_CHECK_INTERVAL_MS,
                 DISK_HEALTH_CHECK_INTERVAL);

    // If 2 out of the total 4 local-dirs fail OR if 2 Out of the total 4
    // log-dirs fail, then the node's health status should become unhealthy.
    conf.setFloat(YarnConfiguration.NM_MIN_HEALTHY_DISKS_FRACTION, 0.60F);

    if (yarnCluster != null) {
      yarnCluster.stop();
      FileUtil.fullyDelete(localFSDirBase);
      localFSDirBase.mkdirs();
    }
    LOG.info("Starting up YARN cluster");
    yarnCluster = new MiniYARNCluster(TestDiskFailures.class.getName(),
        1, numLocalDirs, numLogDirs);
    yarnCluster.init(conf);
    yarnCluster.start();

    NodeManager nm = yarnCluster.getNodeManager(0);
    LOG.info("Configured nm-" + dirType + "-dirs="
             + nm.getConfig().get(dirsProperty));
    dirsHandler = nm.getNodeHealthChecker().getDiskHandler();
    List<String> list = localORLogDirs ? dirsHandler.getLocalDirs()
                                       : dirsHandler.getLogDirs();
    String[] dirs = list.toArray(new String[list.size()]);
    Assert.assertEquals("Number of nm-" + dirType + "-dirs is wrong.",
                        numLocalDirs, dirs.length);
    String expectedDirs = StringUtils.join(",", list);
    // validate the health of disks initially
    verifyDisksHealth(localORLogDirs, expectedDirs, true);

    // Make 1 nm-local-dir fail and verify if "the nodemanager can identify
    // the disk failure(s) and can update the list of good nm-local-dirs.
    prepareDirToFail(dirs[2]);
    expectedDirs = dirs[0] + "," + dirs[1] + ","
                                 + dirs[3];
    verifyDisksHealth(localORLogDirs, expectedDirs, true);

    // Now, make 1 more nm-local-dir/nm-log-dir fail and verify if "the
    // nodemanager can identify the disk failures and can update the list of
    // good nm-local-dirs/nm-log-dirs and can update the overall health status
    // of the node to unhealthy".
    prepareDirToFail(dirs[0]);
    expectedDirs = dirs[1] + "," + dirs[3];
    verifyDisksHealth(localORLogDirs, expectedDirs, false);

    // Fail the remaining 2 local-dirs/log-dirs and verify if NM remains with
    // empty list of local-dirs/log-dirs and the overall health status is
    // unhealthy.
    prepareDirToFail(dirs[1]);
    prepareDirToFail(dirs[3]);
    expectedDirs = "";
    verifyDisksHealth(localORLogDirs, expectedDirs, false);
  }

  /**
   * Wait for the NodeManger to go for the disk-health-check at least once.
   */
  private void waitForDiskHealthCheck() {
    long lastDisksCheckTime = dirsHandler.getLastDisksCheckTime();
    long time = lastDisksCheckTime;
    for (int i = 0; i < 10 && (time <= lastDisksCheckTime); i++) {
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        LOG.error(
            "Interrupted while waiting for NodeManager's disk health check.");
      }
      time = dirsHandler.getLastDisksCheckTime();
    }
  }

  /**
   * Verify if the NodeManager could identify disk failures.
   * @param localORLogDirs <em>true</em> represent nm-local-dirs and <em>false
   *                       </em> means nm-log-dirs
   * @param expectedDirs expected nm-local-dirs/nm-log-dirs as a string
   * @param isHealthy <em>true</em> if the overall node should be healthy
   */
  private void verifyDisksHealth(boolean localORLogDirs, String expectedDirs,
      boolean isHealthy) {
    // Wait for the NodeManager to identify disk failures.
    waitForDiskHealthCheck();

    List<String> list = localORLogDirs ? dirsHandler.getLocalDirs()
                                       : dirsHandler.getLogDirs();
    String seenDirs = StringUtils.join(",", list);
    LOG.info("ExpectedDirs=" + expectedDirs);
    LOG.info("SeenDirs=" + seenDirs);
    Assert.assertTrue("NodeManager could not identify disk failure.",
                      expectedDirs.equals(seenDirs));

    Assert.assertEquals("Node's health in terms of disks is wrong",
                        isHealthy, dirsHandler.areDisksHealthy());
    for (int i = 0; i < 10; i++) {
      Iterator<RMNode> iter = yarnCluster.getResourceManager().getRMContext()
                              .getRMNodes().values().iterator();
      // RMNode # might be zero because of timing related issue.
      if (iter.hasNext() &&
          (iter.next().getState() != NodeState.UNHEALTHY) == isHealthy) {
        break;
      }
      // wait for the node health info to go to RM
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        LOG.error("Interrupted while waiting for NM->RM heartbeat.");
      }
    }
    Iterator<RMNode> iter = yarnCluster.getResourceManager().getRMContext()
                            .getRMNodes().values().iterator();
    Assert.assertEquals("RM is not updated with the health status of a node",
        isHealthy, iter.next().getState() != NodeState.UNHEALTHY);
  }

  /**
   * Prepare directory for a failure: Replace the given directory on the
   * local FileSystem with a regular file with the same name.
   * This would cause failure of creation of directory in DiskChecker.checkDir()
   * with the same name.
   * @param dir the directory to be failed
   * @throws IOException 
   */
  private void prepareDirToFail(String dir) throws IOException {
    File file = new File(dir);
    FileUtil.fullyDelete(file);
    file.createNewFile();
    LOG.info("Prepared " + dir + " to fail.");
  }
}
