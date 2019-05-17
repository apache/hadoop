/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli.yarnservice;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.RunJobCli;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.ServiceSpecFileGenerator;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.ServiceWrapper;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceJobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.component.TensorBoardComponent;
import org.apache.hadoop.yarn.submarine.utils.ZipUtilities;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_CHECKPOINT_PATH;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_DOCKER_IMAGE;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_INPUT_PATH;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_PS_DOCKER_IMAGE;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_PS_LAUNCH_CMD;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_PS_RESOURCES;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_TENSORBOARD_DOCKER_IMAGE;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_TENSORBOARD_RESOURCES;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_WORKER_DOCKER_IMAGE;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_WORKER_LAUNCH_CMD;
import static org.apache.hadoop.yarn.submarine.client.cli.yarnservice.TestYarnServiceRunJobCliCommons.DEFAULT_WORKER_RESOURCES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class to test YarnService with the Run job CLI action.
 */
public class TestYarnServiceRunJobCli {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestYarnServiceRunJobCli.class);

  private TestYarnServiceRunJobCliCommons testCommons =
      new TestYarnServiceRunJobCliCommons();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void before() throws IOException, YarnException {
    testCommons.setup();
  }

  @After
  public void cleanup() throws IOException {
    testCommons.teardown();
  }

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    runJobCli.printUsages();
  }

  private ServiceWrapper getServiceWrapperFromJobSubmitter(
      JobSubmitter jobSubmitter) {
    return ((YarnServiceJobSubmitter) jobSubmitter).getServiceWrapper();
  }

  private void commonVerifyDistributedTrainingSpec(Service serviceSpec) {
    assertNotNull(serviceSpec.getComponent(TensorFlowRole.WORKER
        .getComponentName()));
    assertNotNull(
        serviceSpec.getComponent(TensorFlowRole.PRIMARY_WORKER
            .getComponentName()));
    assertNotNull(serviceSpec.getComponent(TensorFlowRole.PS
        .getComponentName()));
    Component primaryWorkerComp = serviceSpec.getComponent(
        TensorFlowRole.PRIMARY_WORKER.getComponentName());
    assertEquals(2048, primaryWorkerComp.getResource().calcMemoryMB());
    assertEquals(2,
        primaryWorkerComp.getResource().getCpus().intValue());

    Component workerComp = serviceSpec.getComponent(
        TensorFlowRole.WORKER.getComponentName());
    assertEquals(2048, workerComp.getResource().calcMemoryMB());
    assertEquals(2, workerComp.getResource().getCpus().intValue());

    Component psComp = serviceSpec.getComponent(TensorFlowRole.PS
        .getComponentName());
    assertEquals(4096, psComp.getResource().calcMemoryMB());
    assertEquals(4, psComp.getResource().getCpus().intValue());

    assertEquals(DEFAULT_WORKER_DOCKER_IMAGE, workerComp.getArtifact().getId());
    assertEquals(DEFAULT_PS_DOCKER_IMAGE, psComp.getArtifact().getId());

    assertTrue(SubmarineLogs.isVerbose());
  }

  private void verifyQuicklink(Service serviceSpec,
      Map<String, String> expectedQuicklinks) {
    Map<String, String> actualQuicklinks = serviceSpec.getQuicklinks();
    if (actualQuicklinks == null || actualQuicklinks.isEmpty()) {
      assertTrue(
          expectedQuicklinks == null || expectedQuicklinks.isEmpty());
      return;
    }

    assertEquals(expectedQuicklinks.size(), actualQuicklinks.size());
    for (Map.Entry<String, String> expectedEntry : expectedQuicklinks
        .entrySet()) {
      assertTrue(actualQuicklinks.containsKey(expectedEntry.getKey()));

      // $USER could be changed in different environment. so replace $USER by
      // "user"
      String expectedValue = expectedEntry.getValue();
      String actualValue = actualQuicklinks.get(expectedEntry.getKey());

      String userName = System.getProperty("user.name");
      actualValue = actualValue.replaceAll(userName, "username");

      assertEquals(expectedValue, actualValue);
    }
  }

  private void saveServiceSpecJsonToTempFile(Service service)
      throws IOException {
    String jsonFileName = ServiceSpecFileGenerator.generateJson(service);
//    testCommons.getFileUtils().addTrackedFile(new File(jsonFileName));
    LOG.info("Saved json file: " + jsonFileName);
  }

  @Test
  public void testBasicRunJobForDistributedTraining() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(testName.getMethodName())
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(3)
        .withWorkerDockerImage(DEFAULT_WORKER_DOCKER_IMAGE)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withNumberOfPs(2)
        .withPsDockerImage(DEFAULT_PS_DOCKER_IMAGE)
        .withPsLaunchCommand(DEFAULT_PS_LAUNCH_CMD)
        .withPsResources(DEFAULT_PS_RESOURCES)
        .withVerbose()
        .build();
    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(3, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyQuicklink(serviceSpec, null);
  }

  @Test
  public void testBasicRunJobForDistributedTrainingWithTensorboard()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String jobName = testName.getMethodName();
    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(jobName)
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(3)
        .withWorkerDockerImage(DEFAULT_WORKER_DOCKER_IMAGE)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withNumberOfPs(2)
        .withPsDockerImage(DEFAULT_PS_DOCKER_IMAGE)
        .withPsLaunchCommand(DEFAULT_PS_LAUNCH_CMD)
        .withPsResources(DEFAULT_PS_RESOURCES)
        .withVerbose()
        .withTensorboard()
        .build();
    runJobCli.run(params);
    ServiceWrapper serviceWrapper = getServiceWrapperFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Service serviceSpec = serviceWrapper.getService();
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(4, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyTensorboardComponent(runJobCli, serviceWrapper,
        Resources.createResource(4096, 1));

    verifyQuicklink(serviceSpec, ImmutableMap
        .of(TensorBoardComponent.TENSORBOARD_QUICKLINK_LABEL,
            String.format("http://tensorboard-0.%s.username.null:6006",
                jobName)));
  }

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(testName.getMethodName())
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(1)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withVerbose()
        .build();
    runJobCli.run(params);

    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(1, serviceSpec.getComponents().size());

    commonTestSingleNodeTraining(serviceSpec);
  }

  @Test
  public void testTensorboardOnlyService() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(testName.getMethodName())
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(0)
        .withTensorboard()
        .withVerbose()
        .build();
    runJobCli.run(params);

    ServiceWrapper serviceWrapper = getServiceWrapperFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);

    assertEquals(1, serviceWrapper.getService().getComponents().size());

    verifyTensorboardComponent(runJobCli, serviceWrapper,
        Resources.createResource(4096, 1));
  }

  @Test
  public void testTensorboardOnlyServiceWithCustomDockerImageAndCheckpointPath()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(testName.getMethodName())
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(0)
        .withTensorboard()
        .withTensorboardResources(DEFAULT_TENSORBOARD_RESOURCES)
        .withTensorboardDockerImage(DEFAULT_TENSORBOARD_DOCKER_IMAGE)
        .withVerbose()
        .build();
    runJobCli.run(params);

    ServiceWrapper serviceWrapper = getServiceWrapperFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(1, serviceWrapper.getService().getComponents().size());

    verifyTensorboardComponent(runJobCli, serviceWrapper,
        Resources.createResource(2048, 2));
  }

  @Test
  public void testTensorboardOnlyServiceWithCustomizedDockerImageAndResource()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String jobName = testName.getMethodName();
    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(jobName)
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withNumberOfWorkers(0)
        .withTensorboard()
        .withTensorboardResources(DEFAULT_TENSORBOARD_RESOURCES)
        .withTensorboardDockerImage(DEFAULT_TENSORBOARD_DOCKER_IMAGE)
        .withVerbose()
        .build();
    runJobCli.run(params);

    ServiceWrapper serviceWrapper = getServiceWrapperFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(1, serviceWrapper.getService().getComponents().size());

    verifyTensorboardComponent(runJobCli, serviceWrapper,
        Resources.createResource(2048, 2));
    verifyQuicklink(serviceWrapper.getService(), ImmutableMap
        .of(TensorBoardComponent.TENSORBOARD_QUICKLINK_LABEL,
            String.format("http://tensorboard-0.%s.username.null:6006",
                jobName)));
  }

  private void commonTestSingleNodeTraining(Service serviceSpec) {
    assertNotNull(
        serviceSpec.getComponent(TensorFlowRole.PRIMARY_WORKER
            .getComponentName()));
    Component primaryWorkerComp = serviceSpec.getComponent(
        TensorFlowRole.PRIMARY_WORKER.getComponentName());
    assertEquals(2048, primaryWorkerComp.getResource().calcMemoryMB());
    assertEquals(2,
        primaryWorkerComp.getResource().getCpus().intValue());

    assertTrue(SubmarineLogs.isVerbose());
  }

  private void verifyTensorboardComponent(RunJobCli runJobCli,
      ServiceWrapper serviceWrapper, Resource resource) throws Exception {
    Service serviceSpec = serviceWrapper.getService();
    assertNotNull(
        serviceSpec.getComponent(TensorFlowRole.TENSORBOARD
            .getComponentName()));
    Component tensorboardComp = serviceSpec.getComponent(
        TensorFlowRole.TENSORBOARD.getComponentName());
    assertEquals(1, tensorboardComp.getNumberOfContainers().intValue());
    assertEquals(resource.getMemorySize(),
        tensorboardComp.getResource().calcMemoryMB());
    assertEquals(resource.getVirtualCores(),
        tensorboardComp.getResource().getCpus().intValue());

    assertEquals("./run-TENSORBOARD.sh",
        tensorboardComp.getLaunchCommand());

    RunJobParameters runJobParameters = runJobCli.getRunJobParameters();
    assertTrue(RunJobParameters.class + " must be an instance of " +
            TensorFlowRunJobParameters.class,
        runJobParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) runJobParameters;

    // Check docker image
    if (tensorFlowParams.getTensorboardDockerImage() != null) {
      assertEquals(
          tensorFlowParams.getTensorboardDockerImage(),
          tensorboardComp.getArtifact().getId());
    } else {
      assertNull(tensorboardComp.getArtifact());
    }

    String expectedLaunchScript =
        "#!/bin/bash\n" + "echo \"CLASSPATH:$CLASSPATH\"\n"
            + "echo \"HADOOP_CONF_DIR:$HADOOP_CONF_DIR\"\n"
            + "echo \"HADOOP_TOKEN_FILE_LOCATION:" +
            "$HADOOP_TOKEN_FILE_LOCATION\"\n"
            + "echo \"JAVA_HOME:$JAVA_HOME\"\n"
            + "echo \"LD_LIBRARY_PATH:$LD_LIBRARY_PATH\"\n"
            + "echo \"HADOOP_HDFS_HOME:$HADOOP_HDFS_HOME\"\n"
            + "export LC_ALL=C && tensorboard --logdir=" +
            runJobParameters.getCheckpointPath() + "\n";

    verifyLaunchScriptForComponent(serviceWrapper,
        TensorFlowRole.TENSORBOARD, expectedLaunchScript);
  }

  private void verifyLaunchScriptForComponent(ServiceWrapper serviceWrapper,
      TensorFlowRole taskType, String expectedLaunchScriptContent)
      throws Exception {

    String path = serviceWrapper
        .getLocalLaunchCommandPathForComponent(taskType.getComponentName());

    byte[] encoded = Files.readAllBytes(Paths.get(path));
    String scriptContent = new String(encoded, Charset.defaultCharset());

    assertEquals(expectedLaunchScriptContent, scriptContent);
  }

  @Test
  public void testBasicRunJobForSingleNodeTrainingWithTensorboard()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(testName.getMethodName())
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(1)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withTensorboard()
        .withVerbose()
        .build();
    runJobCli.run(params);
    ServiceWrapper serviceWrapper = getServiceWrapperFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Service serviceSpec = serviceWrapper.getService();
    saveServiceSpecJsonToTempFile(serviceSpec);

    assertEquals(2, serviceSpec.getComponents().size());

    commonTestSingleNodeTraining(serviceSpec);
    verifyTensorboardComponent(runJobCli, serviceWrapper,
        Resources.createResource(4096, 1));
  }

  @Test
  public void testBasicRunJobForSingleNodeTrainingWithGeneratedCheckpoint()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(testName.getMethodName())
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withNumberOfWorkers(1)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withTensorboard()
        .withVerbose()
        .build();
    runJobCli.run(params);
    ServiceWrapper serviceWrapper = getServiceWrapperFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Service serviceSpec = serviceWrapper.getService();
    saveServiceSpecJsonToTempFile(serviceSpec);

    assertEquals(2, serviceSpec.getComponents().size());

    commonTestSingleNodeTraining(serviceSpec);
    verifyTensorboardComponent(runJobCli, serviceWrapper,
        Resources.createResource(4096, 1));
  }

  @Test
  public void testParameterStorageForTrainingJob() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String jobName = testName.getMethodName();
    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(jobName)
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(1)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withTensorboard()
        .withVerbose()
        .build();
    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);

    SubmarineStorage storage =
        mockClientContext.getRuntimeFactory().getSubmarineStorage();
    Map<String, String> jobInfo = storage.getJobInfoByName(jobName);
    assertTrue(jobInfo.size() > 0);
    assertEquals(jobInfo.get(StorageKeyConstants.INPUT_PATH),
        DEFAULT_INPUT_PATH);
  }

  @Test
  public void testAddQuicklinksWithoutTensorboard() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String jobName = testName.getMethodName();
    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(jobName)
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(3)
        .withWorkerDockerImage(DEFAULT_WORKER_DOCKER_IMAGE)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withNumberOfPs(2)
        .withPsDockerImage(DEFAULT_PS_DOCKER_IMAGE)
        .withPsLaunchCommand(DEFAULT_PS_LAUNCH_CMD)
        .withPsResources(DEFAULT_PS_RESOURCES)
        .withQuickLink("AAA=http://master-0:8321")
        .withQuickLink("BBB=http://worker-0:1234")
        .withVerbose()
        .build();
    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(3, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyQuicklink(serviceSpec, ImmutableMap
        .of("AAA", String.format("http://master-0.%s.username.null:8321",
            jobName), "BBB",
            String.format("http://worker-0.%s.username.null:1234", jobName)));
  }

  @Test
  public void testAddQuicklinksWithTensorboard() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    assertFalse(SubmarineLogs.isVerbose());

    String jobName = testName.getMethodName();
    String[] params = ParamBuilderForTest.create()
        .withFramework("tensorflow")
        .withJobName(jobName)
        .withDockerImage(DEFAULT_DOCKER_IMAGE)
        .withInputPath(DEFAULT_INPUT_PATH)
        .withCheckpointPath(DEFAULT_CHECKPOINT_PATH)
        .withNumberOfWorkers(3)
        .withWorkerDockerImage(DEFAULT_WORKER_DOCKER_IMAGE)
        .withWorkerLaunchCommand(DEFAULT_WORKER_LAUNCH_CMD)
        .withWorkerResources(DEFAULT_WORKER_RESOURCES)
        .withNumberOfPs(2)
        .withPsDockerImage(DEFAULT_PS_DOCKER_IMAGE)
        .withPsLaunchCommand(DEFAULT_PS_LAUNCH_CMD)
        .withPsResources(DEFAULT_PS_RESOURCES)
        .withQuickLink("AAA=http://master-0:8321")
        .withQuickLink("BBB=http://worker-0:1234")
        .withTensorboard()
        .withVerbose()
        .build();

    runJobCli.run(params);
    Service serviceSpec = testCommons.getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    saveServiceSpecJsonToTempFile(serviceSpec);
    assertEquals(4, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyQuicklink(serviceSpec, ImmutableMap
        .of("AAA", String.format("http://master-0.%s.username.null:8321",
            jobName), "BBB",
            String.format("http://worker-0.%s.username.null:1234", jobName),
            TensorBoardComponent.TENSORBOARD_QUICKLINK_LABEL,
            String.format("http://tensorboard-0.%s.username.null:6006",
                jobName)));
  }

  /**
   * Test zip function.
   * A dir "/user/yarn/mydir" has two files and one subdir
   * */
  @Test
  public void testYarnServiceSubmitterZipFunction()
      throws Exception {
    String localUrl = "/user/yarn/mydir";
    String localSubDirName = "subdir1";

    // create local file
    File localDir1 = testCommons.getFileUtils().createDirInTempDir(localUrl);
    testCommons.getFileUtils().createFileInDir(localDir1, "1.py");
    testCommons.getFileUtils().createFileInDir(localDir1, "2.py");

    File localSubDir =
        testCommons.getFileUtils().createDirectory(localDir1, localSubDirName);
    testCommons.getFileUtils().createFileInDir(localSubDir, "3.py");

    String tempDir = localDir1.getParent();
    String zipFilePath = ZipUtilities.zipDir(localDir1.getAbsolutePath(),
        new File(tempDir, "mydir.zip").getAbsolutePath());
    File zipFile = new File(zipFilePath);
    File unzipTargetDir = new File(tempDir, "unzipDir");
    FileUtil.unZip(zipFile, unzipTargetDir);
    assertTrue(
        new File(tempDir + "/unzipDir/1.py").exists());
    assertTrue(
        new File(tempDir + "/unzipDir/2.py").exists());
    assertTrue(
        new File(tempDir + "/unzipDir/subdir1").exists());
    assertTrue(
        new File(tempDir + "/unzipDir/subdir1/3.py").exists());
  }

}
