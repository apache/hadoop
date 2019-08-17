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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Supplier;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.TestProcfsBasedProcessTree;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

public class TestContainersMonitor extends BaseContainerManagerTest {

  public TestContainersMonitor() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LoggerFactory.getLogger(TestContainersMonitor.class);
  }
  @Before
  public void setup() throws IOException {
    conf.setClass(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR,
        LinuxResourceCalculatorPlugin.class, ResourceCalculatorPlugin.class);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED, false);
    super.setup();
  }

  @Test
  public void testMetricsUpdate() throws Exception {
    // This test doesn't verify the correction of those metrics
    // updated by the monitor, it only verifies that the monitor
    // do publish these info to node manager metrics system in
    // each monitor interval.
    Context spyContext = spy(context);
    ContainersMonitorImpl cm =
        new ContainersMonitorImpl(mock(ContainerExecutor.class),
            mock(AsyncDispatcher.class), spyContext);
    cm.init(getConfForCM(false, true, 1024, 2.1f));
    cm.start();
    Mockito.verify(spyContext, timeout(500).atLeastOnce())
        .getNodeManagerMetrics();
  }

  /**
   * Test to verify the check for whether a process tree is over limit or not.
   *
   * @throws IOException
   *           if there was a problem setting up the fake procfs directories or
   *           files.
   */
  @Test
  public void testProcessTreeLimits() throws IOException {

    // set up a dummy proc file system
    File procfsRootDir = new File(localDir, "proc");
    String[] pids = { "100", "200", "300", "400", "500", "600", "700" };
    try {
      TestProcfsBasedProcessTree.setupProcfsRootDir(procfsRootDir);

      // create pid dirs.
      TestProcfsBasedProcessTree.setupPidDirs(procfsRootDir, pids);

      // create process infos.
      TestProcfsBasedProcessTree.ProcessStatInfo[] procs =
          new TestProcfsBasedProcessTree.ProcessStatInfo[7];

      // assume pids 100, 500 are in 1 tree
      // 200,300,400 are in another
      // 600,700 are in a third
      procs[0] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "100", "proc1", "1", "100", "100", "100000" });
      procs[1] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "200", "proc2", "1", "200", "200", "200000" });
      procs[2] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "300", "proc3", "200", "200", "200", "300000" });
      procs[3] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "400", "proc4", "200", "200", "200", "400000" });
      procs[4] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "500", "proc5", "100", "100", "100", "1500000" });
      procs[5] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "600", "proc6", "1", "600", "600", "100000" });
      procs[6] = new TestProcfsBasedProcessTree.ProcessStatInfo(
          new String[] { "700", "proc7", "600", "600", "600", "100000" });
      // write stat files.
      TestProcfsBasedProcessTree.writeStatFiles(procfsRootDir, pids, procs, null);

      // vmem limit
      long limit = 700000;

      ContainersMonitorImpl test = new ContainersMonitorImpl(null, null, null);

      // create process trees
      // tree rooted at 100 is over limit immediately, as it is
      // twice over the mem limit.
      ProcfsBasedProcessTree pTree = new ProcfsBasedProcessTree(
                                          "100",
                                          procfsRootDir.getAbsolutePath());
      pTree.updateProcessTree();
      assertTrue("tree rooted at 100 should be over limit " +
                    "after first iteration.",
                  test.isProcessTreeOverLimit(pTree, "dummyId", limit));

      // the tree rooted at 200 is initially below limit.
      pTree = new ProcfsBasedProcessTree("200",
                                          procfsRootDir.getAbsolutePath());
      pTree.updateProcessTree();
      assertFalse("tree rooted at 200 shouldn't be over limit " +
                    "after one iteration.",
                  test.isProcessTreeOverLimit(pTree, "dummyId", limit));
      // second iteration - now the tree has been over limit twice,
      // hence it should be declared over limit.
      pTree.updateProcessTree();
      assertTrue(
          "tree rooted at 200 should be over limit after 2 iterations",
                  test.isProcessTreeOverLimit(pTree, "dummyId", limit));

      // the tree rooted at 600 is never over limit.
      pTree = new ProcfsBasedProcessTree("600",
                                            procfsRootDir.getAbsolutePath());
      pTree.updateProcessTree();
      assertFalse("tree rooted at 600 should never be over limit.",
                    test.isProcessTreeOverLimit(pTree, "dummyId", limit));

      // another iteration does not make any difference.
      pTree.updateProcessTree();
      assertFalse("tree rooted at 600 should never be over limit.",
                    test.isProcessTreeOverLimit(pTree, "dummyId", limit));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  // Test that even if VMEM_PMEM_CHECK is not enabled, container monitor will
  // run.
  @Test
  public void testContainerMonitor() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
    containerManager.start();
    ContainerLaunchContext context =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    context.setCommands(Arrays.asList("sleep 6"));
    ContainerId cId = createContainerId(1705);

    // start the container
    StartContainerRequest scRequest = StartContainerRequest.newInstance(context,
        createContainerToken(cId, DUMMY_RM_IDENTIFIER, this.context.getNodeId(),
            user, this.context.getContainerTokenSecretManager()));
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(Arrays.asList(scRequest));
    containerManager.startContainers(allRequests);
    BaseContainerManagerTest
        .waitForContainerState(containerManager, cId, ContainerState.RUNNING);
    Thread.sleep(2000);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        try {
          return containerManager.getContainerStatuses(
              GetContainerStatusesRequest.newInstance(Arrays.asList(cId)))
              .getContainerStatuses().get(0).getHost() != null;
        } catch (Exception e) {
          return false;
        }
      }

    }, 300, 10000);
  }

  @Test
  public void testContainerKillOnMemoryOverflow() throws IOException,
      InterruptedException, YarnException {

    if (!ProcfsBasedProcessTree.isAvailable()) {
      return;
    }

    containerManager.start();

    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    File processStartFile =
        new File(tmpDir, "start_file.txt").getAbsoluteFile();
    fileWriter.write("\numask 0"); // So that start file is readable by the
                                   // test.
    fileWriter.write("\necho Hello World! > " + processStartFile);
    fileWriter.write("\necho $$ >> " + processStartFile);
    fileWriter.write("\nsleep 15");
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    // ////// Construct the Container-id
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId cId = ContainerId.newContainerId(appAttemptId, 0);

    URL resource_alpha =
        URL.fromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/bash");
    commands.add(scriptFile.getAbsolutePath());
    containerLaunchContext.setCommands(commands);
    Resource r = BuilderUtils.newResource(0, 0);
    ContainerTokenIdentifier containerIdentifier =
        new ContainerTokenIdentifier(cId, context.getNodeId().toString(), user,
          r, System.currentTimeMillis() + 120000, 123, DUMMY_RM_IDENTIFIER,
          Priority.newInstance(0), 0);
    Token containerToken =
        BuilderUtils.newContainerToken(context.getNodeId(),
          containerManager.getContext().getContainerTokenSecretManager()
            .createPassword(containerIdentifier), containerIdentifier);
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          containerToken);
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);
    
    int timeoutSecs = 0;
    while (!processStartFile.exists() && timeoutSecs++ < 20) {
      Thread.sleep(1000);
      LOG.info("Waiting for process start-file to be created");
    }
    Assert.assertTrue("ProcessStartFile doesn't exist!",
        processStartFile.exists());

    // Now verify the contents of the file
    BufferedReader reader =
        new BufferedReader(new FileReader(processStartFile));
    Assert.assertEquals("Hello World!", reader.readLine());
    // Get the pid of the process
    String pid = reader.readLine().trim();
    // No more lines
    Assert.assertEquals(null, reader.readLine());

    BaseContainerManagerTest.waitForContainerState(containerManager, cId,
        ContainerState.COMPLETE, 60);

    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);
    GetContainerStatusesRequest gcsRequest =
        GetContainerStatusesRequest.newInstance(containerIds);
    ContainerStatus containerStatus =
        containerManager.getContainerStatuses(gcsRequest).getContainerStatuses().get(0);
    Assert.assertEquals(ContainerExitStatus.KILLED_EXCEEDED_VMEM,
        containerStatus.getExitStatus());
    String expectedMsgPattern =
        "Container \\[pid=" + pid + ",containerID=" + cId + "\\] is running "
            + "[0-9]+B beyond the 'VIRTUAL' memory limit. Current usage: "
            + "[0-9.]+ ?[KMGTPE]?B of [0-9.]+ ?[KMGTPE]?B physical memory used; "
            + "[0-9.]+ ?[KMGTPE]?B of [0-9.]+ ?[KMGTPE]?B virtual memory used. "
            + "Killing container.\nDump of the process-tree for "
            + cId + " :\n";
    Pattern pat = Pattern.compile(expectedMsgPattern);
    Assert.assertEquals("Expected message pattern is: " + expectedMsgPattern
        + "\n\nObserved message is: " + containerStatus.getDiagnostics(),
        true, pat.matcher(containerStatus.getDiagnostics()).find());

    // Assert that the process is not alive anymore
    Assert.assertFalse("Process is still alive!",
        exec.signalContainer(new ContainerSignalContext.Builder()
            .setUser(user)
            .setPid(pid)
            .setSignal(Signal.NULL)
            .build()));
  }

  @Test(timeout = 20000)
  public void testContainerMonitorMemFlags() {
    ContainersMonitor cm = null;

    long expPmem = 8192 * 1024 * 1024l;
    long expVmem = (long) (expPmem * 2.1f);

    cm = new ContainersMonitorImpl(mock(ContainerExecutor.class),
        mock(AsyncDispatcher.class), mock(Context.class));
    cm.init(getConfForCM(false, false, 8192, 2.1f));
    assertEquals(expPmem, cm.getPmemAllocatedForContainers());
    assertEquals(expVmem, cm.getVmemAllocatedForContainers());
    assertEquals(false, cm.isPmemCheckEnabled());
    assertEquals(false, cm.isVmemCheckEnabled());

    cm = new ContainersMonitorImpl(mock(ContainerExecutor.class),
        mock(AsyncDispatcher.class), mock(Context.class));
    cm.init(getConfForCM(true, false, 8192, 2.1f));
    assertEquals(expPmem, cm.getPmemAllocatedForContainers());
    assertEquals(expVmem, cm.getVmemAllocatedForContainers());
    assertEquals(true, cm.isPmemCheckEnabled());
    assertEquals(false, cm.isVmemCheckEnabled());

    cm = new ContainersMonitorImpl(mock(ContainerExecutor.class),
        mock(AsyncDispatcher.class), mock(Context.class));
    cm.init(getConfForCM(true, true, 8192, 2.1f));
    assertEquals(expPmem, cm.getPmemAllocatedForContainers());
    assertEquals(expVmem, cm.getVmemAllocatedForContainers());
    assertEquals(true, cm.isPmemCheckEnabled());
    assertEquals(true, cm.isVmemCheckEnabled());

    cm = new ContainersMonitorImpl(mock(ContainerExecutor.class),
        mock(AsyncDispatcher.class), mock(Context.class));
    cm.init(getConfForCM(false, true, 8192, 2.1f));
    assertEquals(expPmem, cm.getPmemAllocatedForContainers());
    assertEquals(expVmem, cm.getVmemAllocatedForContainers());
    assertEquals(false, cm.isPmemCheckEnabled());
    assertEquals(true, cm.isVmemCheckEnabled());
  }

  private YarnConfiguration getConfForCM(boolean pMemEnabled,
      boolean vMemEnabled, int nmPmem, float vMemToPMemRatio) {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_PMEM_MB, nmPmem);
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, pMemEnabled);
    conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, vMemEnabled);
    conf.setFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO, vMemToPMemRatio);
    return conf;
  }
}
