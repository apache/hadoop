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

package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController.MyLinuxTaskController;
import org.apache.hadoop.filecache.TestTrackerDistributedCacheManager;

/**
 * Test the DistributedCacheManager when LinuxTaskController is used.
 * 
 */
public class TestTrackerDistributedCacheManagerWithLinuxTaskController extends
    TestTrackerDistributedCacheManager {

  private File configFile;
  private MyLinuxTaskController taskController;
  private String taskTrackerSpecialGroup;

  private static final Log LOG =
      LogFactory
          .getLog(TestTrackerDistributedCacheManagerWithLinuxTaskController.class);

  @Override
  protected void setUp()
      throws IOException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data", "/tmp"),
            TestTrackerDistributedCacheManagerWithLinuxTaskController.class
                .getSimpleName()).getAbsolutePath();

    super.setUp();

    taskController = new MyLinuxTaskController();
    String path =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_PATH);
    configFile =
        ClusterWithLinuxTaskController.createTaskControllerConf(path, conf
            .getStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
    String execPath = path + "/task-controller";
    taskController.setTaskControllerExe(execPath);
    taskController.setConf(conf);
    taskController.setup();

    taskTrackerSpecialGroup =
        TestTaskTrackerLocalization.getFilePermissionAttrs(execPath)[2];
  }

  @Override
  protected void tearDown()
      throws IOException {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    if (configFile != null) {
      configFile.delete();
    }
    super.tearDown();
  }

  /**
   * Test the control flow of distributed cache manager when LinuxTaskController
   * is used.
   */
  @Override
  public void testManagerFlow()
      throws IOException,
      LoginException {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    super.testManagerFlow();
  }

  @Override
  protected String getJobOwnerName() {
    String ugi =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    String userName = ugi.split(",")[0];
    return userName;
  }

  @Override
  protected TaskController getTaskController() {
    return taskController;
  }

  @Override
  protected void checkFilePermissions(Path[] localCacheFiles)
      throws IOException {
    String cachedFirstFile = localCacheFiles[0].toUri().getPath();
    String cachedSecondFile = localCacheFiles[1].toUri().getPath();
    String userName = getJobOwnerName();

    // First make sure that the cache files have proper permissions.
    TestTaskTrackerLocalization.checkFilePermissions(cachedFirstFile,
        "-r-xrwx---", userName, taskTrackerSpecialGroup);
    TestTaskTrackerLocalization.checkFilePermissions(cachedSecondFile,
        "-r-xrwx---", userName, taskTrackerSpecialGroup);

    // Now. make sure that all the path components also have proper
    // permissions.
    checkPermissionOnPathComponents(cachedFirstFile, userName);
    checkPermissionOnPathComponents(cachedSecondFile, userName);
  }

  /**
   * @param cachedFilePath
   * @param userName
   * @throws IOException
   */
  private void checkPermissionOnPathComponents(String cachedFilePath,
      String userName)
      throws IOException {
    // The trailing distcache/file/... string
    String trailingStringForFirstFile =
        cachedFilePath.replaceFirst(ROOT_MAPRED_LOCAL_DIR.getAbsolutePath()
            + Path.SEPARATOR + "0_[0-" + (numLocalDirs - 1) + "]"
            + Path.SEPARATOR + TaskTracker.getDistributedCacheDir(userName),
            "");
    LOG.info("Leading path for cacheFirstFile is : "
        + trailingStringForFirstFile);
    // The leading mapred.local.dir/0_[0-n]/taskTracker/$user string.
    String leadingStringForFirstFile =
        cachedFilePath.substring(0, cachedFilePath
            .lastIndexOf(trailingStringForFirstFile));
    LOG.info("Leading path for cacheFirstFile is : "
        + leadingStringForFirstFile);

    // Now check path permissions, starting with cache file's parent dir.
    File path = new File(cachedFilePath).getParentFile();
    while (!path.getAbsolutePath().equals(leadingStringForFirstFile)) {
      TestTaskTrackerLocalization.checkFilePermissions(path.getAbsolutePath(),
          "dr-xrws---", userName, taskTrackerSpecialGroup);
      path = path.getParentFile();
    }
  }

  @Override
  public void testDeleteCache()
      throws Exception {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    super.testDeleteCache();
  }

  @Override
  public void testFileSystemOtherThanDefault()
      throws Exception {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    super.testFileSystemOtherThanDefault();
  }
  
  @Override
  public void testFreshness()  throws Exception { 
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    super.testFreshness();
  }
}
