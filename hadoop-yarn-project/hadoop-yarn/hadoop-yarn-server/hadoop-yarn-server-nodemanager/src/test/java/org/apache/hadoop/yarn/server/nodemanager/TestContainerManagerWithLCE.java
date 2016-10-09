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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.junit.After;

public class TestContainerManagerWithLCE extends TestContainerManager {

  private static final Log LOG = LogFactory
      .getLog(TestContainerManagerWithLCE.class);

  public TestContainerManagerWithLCE() throws UnsupportedFileSystemException {
    super();
  }

  static {
    localDir =
        new File("target",
            TestContainerManagerWithLCE.class.getName() + "-localDir")
            .getAbsoluteFile();
    tmpDir = new File("target",
        TestContainerManagerWithLCE.class.getName() + "-tmpDir");
  }

  @Override
  public void setup() throws IOException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    super.setup();
    localFS.setPermission(new Path(localDir.getCanonicalPath()),
        new FsPermission(
            (short) 0777));
    localFS.setPermission(new Path(tmpDir.getCanonicalPath()),
        new FsPermission(
            (short) 0777));
  }

  @After
  @Override
  public void tearDown() throws IOException, InterruptedException {
    if (shouldRunTest()) {
      super.tearDown();
    }
  }

  @Override
  public void testContainerSetup() throws Exception, InterruptedException,
      YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerSetup");
    super.testContainerSetup();
  }

  @Override
  public void testContainerManagerInitialization() throws IOException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerManagerInitialization");
    super.testContainerManagerInitialization();
  }

  @Override
  public void testContainerLaunchAndStop() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchAndStop");
    super.testContainerLaunchAndStop();
  }
  
  @Override
  public void testContainerLaunchAndExitSuccess() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchAndExitSuccess");
    super.testContainerLaunchAndExitSuccess();
  }

  @Override
  public void testContainerLaunchAndExitFailure() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchAndExitFailure");
    super.testContainerLaunchAndExitFailure();
  }

  @Override
  public void testLocalingResourceWhileContainerRunning()
      throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    super.testLocalingResourceWhileContainerRunning();
  }

  @Override
  public void testLocalFilesCleanup() throws InterruptedException,
      IOException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testLocalFilesCleanup");
    super.testLocalFilesCleanup();
  }

  @Override
  public void testContainerLaunchFromPreviousRM() throws InterruptedException,
      IOException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchFromPreviousRM");
    super.testContainerLaunchFromPreviousRM();
  }

  @Override
  public void testMultipleContainersLaunch() throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchFromPreviousRM");
    super.testMultipleContainersLaunch();
  }

  @Override
  public void testMultipleContainersStopAndGetStatus() throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchFromPreviousRM");
    super.testMultipleContainersStopAndGetStatus();
  }

  @Override
  public void testStartContainerFailureWithUnknownAuxService() throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerLaunchFromPreviousRM");
    super.testStartContainerFailureWithUnknownAuxService();
  }

  @Override
  public void testIncreaseContainerResourceWithInvalidRequests() throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testIncreaseContainerResourceWithInvalidRequests");
    super.testIncreaseContainerResourceWithInvalidRequests();
  }

  @Override
  public void testIncreaseContainerResourceWithInvalidResource() throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testIncreaseContainerResourceWithInvalidResource");
    super.testIncreaseContainerResourceWithInvalidResource();
  }

  @Override
  public void testChangeContainerResource() throws Exception {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testChangeContainerResource");
    super.testChangeContainerResource();
  }

  @Override
  public void testOutputThreadDumpSignal() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testOutputThreadDumpSignal");
    super.testOutputThreadDumpSignal();
  }

  @Override
  public void testGracefulShutdownSignal() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testGracefulShutdownSignal");
    super.testGracefulShutdownSignal();
  }

  @Override
  public void testForcefulShutdownSignal() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testForcefulShutdownSignal");
    super.testForcefulShutdownSignal();
  }

  @Override
  public void testContainerUpgradeSuccessAutoCommit() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerUpgradeSuccessAutoCommit");
    super.testContainerUpgradeSuccessAutoCommit();
  }

  @Override
  public void testContainerUpgradeLocalizationFailure() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerUpgradeLocalizationFailure");
    super.testContainerUpgradeLocalizationFailure();
  }

  @Override
  public void testContainerUpgradeSuccessExplicitCommit() throws IOException,
    InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerUpgradeSuccessExplicitCommit");
    super.testContainerUpgradeSuccessExplicitCommit();
  }

  @Override
  public void testContainerUpgradeSuccessExplicitRollback() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerUpgradeSuccessExplicitRollback");
    super.testContainerUpgradeSuccessExplicitRollback();
  }

  @Override
  public void testContainerUpgradeRollbackDueToFailure() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerUpgradeRollbackDueToFailure");
    super.testContainerUpgradeRollbackDueToFailure();
  }

  @Override
  public void testContainerUpgradeProcessFailure() throws IOException,
      InterruptedException, YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerUpgradeProcessFailure");
    super.testContainerUpgradeProcessFailure();
  }

  @Override
  public void testContainerRestart() throws IOException, InterruptedException,
      YarnException {
    // Don't run the test if the binary is not available.
    if (!shouldRunTest()) {
      LOG.info("LCE binary path is not passed. Not running the test");
      return;
    }
    LOG.info("Running testContainerRestart");
    super.testContainerRestart();
  }

  private boolean shouldRunTest() {
    return System
        .getProperty(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH) != null;
  }

  @Override
  protected ContainerExecutor createContainerExecutor() {
    super.conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, System
        .getProperty(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH));
    LinuxContainerExecutor linuxContainerExecutor =
        new LinuxContainerExecutor();
    linuxContainerExecutor.setConf(super.conf);
    return linuxContainerExecutor;
  }
}
