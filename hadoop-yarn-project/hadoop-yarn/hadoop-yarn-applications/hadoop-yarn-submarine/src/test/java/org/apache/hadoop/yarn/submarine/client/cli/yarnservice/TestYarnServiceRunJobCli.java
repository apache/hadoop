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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.submarine.client.cli.RunJobCli;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceJobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestYarnServiceRunJobCli {
  @Before
  public void before() throws IOException, YarnException {
    SubmarineLogs.verboseOff();
    ServiceClient serviceClient = mock(ServiceClient.class);
    when(serviceClient.actionCreate(any(Service.class))).thenReturn(
        ApplicationId.newInstance(1234L, 1));
    YarnServiceUtils.setStubServiceClient(serviceClient);
  }

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    runJobCli.printUsages();
  }

  private Service getServiceSpecFromJobSubmitter(JobSubmitter jobSubmitter) {
    return ((YarnServiceJobSubmitter) jobSubmitter).getServiceSpec();
  }

  private void commonVerifyDistributedTrainingSpec(Service serviceSpec)
      throws Exception {
    Assert.assertTrue(
        serviceSpec.getComponent(TaskType.WORKER.getComponentName()) != null);
    Assert.assertTrue(
        serviceSpec.getComponent(TaskType.PRIMARY_WORKER.getComponentName())
            != null);
    Assert.assertTrue(
        serviceSpec.getComponent(TaskType.PS.getComponentName()) != null);
    Component primaryWorkerComp = serviceSpec.getComponent(
        TaskType.PRIMARY_WORKER.getComponentName());
    Assert.assertEquals(2048, primaryWorkerComp.getResource().calcMemoryMB());
    Assert.assertEquals(2,
        primaryWorkerComp.getResource().getCpus().intValue());

    Component workerComp = serviceSpec.getComponent(
        TaskType.WORKER.getComponentName());
    Assert.assertEquals(2048, workerComp.getResource().calcMemoryMB());
    Assert.assertEquals(2, workerComp.getResource().getCpus().intValue());

    Component psComp = serviceSpec.getComponent(TaskType.PS.getComponentName());
    Assert.assertEquals(4096, psComp.getResource().calcMemoryMB());
    Assert.assertEquals(4, psComp.getResource().getCpus().intValue());

    Assert.assertEquals("worker.image", workerComp.getArtifact().getId());
    Assert.assertEquals("ps.image", psComp.getArtifact().getId());

    Assert.assertTrue(SubmarineLogs.isVerbose());
  }

  private void verifyQuicklink(Service serviceSpec,
      Map<String, String> expectedQuicklinks) {
    Map<String, String> actualQuicklinks = serviceSpec.getQuicklinks();
    if (actualQuicklinks == null || actualQuicklinks.isEmpty()) {
      Assert.assertTrue(
          expectedQuicklinks == null || expectedQuicklinks.isEmpty());
      return;
    }

    Assert.assertEquals(expectedQuicklinks.size(), actualQuicklinks.size());
    for (Map.Entry<String, String> expectedEntry : expectedQuicklinks
        .entrySet()) {
      Assert.assertTrue(actualQuicklinks.containsKey(expectedEntry.getKey()));

      // $USER could be changed in different environment. so replace $USER by
      // "user"
      String expectedValue = expectedEntry.getValue();
      String actualValue = actualQuicklinks.get(expectedEntry.getKey());

      String userName = System.getProperty("user.name");
      actualValue = actualValue.replaceAll(userName, "username");

      Assert.assertEquals(expectedValue, actualValue);
    }
  }

  @Test
  public void testBasicRunJobForDistributedTraining() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyQuicklink(serviceSpec, null);
  }

  @Test
  public void testBasicRunJobForDistributedTrainingWithTensorboard()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--tensorboard", "--ps_launch_cmd", "python run-ps.py",
            "--verbose" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(4, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyTensorboardComponent(runJobCli, serviceSpec,
        Resources.createResource(4096, 1));

    verifyQuicklink(serviceSpec, ImmutableMap
        .of(YarnServiceJobSubmitter.TENSORBOARD_QUICKLINK_LABEL,
            "http://tensorboard-0.my-job.username.null:6006"));
  }

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=2G,vcores=2", "--verbose" });

    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(1, serviceSpec.getComponents().size());

    commonTestSingleNodeTraining(serviceSpec);
  }

  @Test
  public void testTensorboardOnlyService() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "0", "--tensorboard", "--verbose" });

    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(1, serviceSpec.getComponents().size());

    verifyTensorboardComponent(runJobCli, serviceSpec,
        Resources.createResource(4096, 1));
  }

  @Test
  public void testTensorboardOnlyServiceWithCustomizedDockerImageAndResourceCkptPath()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "0", "--tensorboard", "--verbose",
            "--tensorboard_resources", "memory=2G,vcores=2",
            "--tensorboard_docker_image", "tb_docker_image:001" });

    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(1, serviceSpec.getComponents().size());

    verifyTensorboardComponent(runJobCli, serviceSpec,
        Resources.createResource(2048, 2));
  }

  @Test
  public void testTensorboardOnlyServiceWithCustomizedDockerImageAndResource()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--num_workers", "0", "--tensorboard", "--verbose",
            "--tensorboard_resources", "memory=2G,vcores=2",
            "--tensorboard_docker_image", "tb_docker_image:001" });

    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(1, serviceSpec.getComponents().size());

    verifyTensorboardComponent(runJobCli, serviceSpec,
        Resources.createResource(2048, 2));
    verifyQuicklink(serviceSpec, ImmutableMap
        .of(YarnServiceJobSubmitter.TENSORBOARD_QUICKLINK_LABEL,
            "http://tensorboard-0.my-job.username.null:6006"));
  }

  private void commonTestSingleNodeTraining(Service serviceSpec)
      throws Exception {
    Assert.assertTrue(
        serviceSpec.getComponent(TaskType.PRIMARY_WORKER.getComponentName())
            != null);
    Component primaryWorkerComp = serviceSpec.getComponent(
        TaskType.PRIMARY_WORKER.getComponentName());
    Assert.assertEquals(2048, primaryWorkerComp.getResource().calcMemoryMB());
    Assert.assertEquals(2,
        primaryWorkerComp.getResource().getCpus().intValue());

    Assert.assertTrue(SubmarineLogs.isVerbose());
  }

  private void verifyTensorboardComponent(RunJobCli runJobCli,
      Service serviceSpec, Resource resource) throws Exception {
    Assert.assertTrue(
        serviceSpec.getComponent(TaskType.TENSORBOARD.getComponentName())
            != null);
    Component tensorboardComp = serviceSpec.getComponent(
        TaskType.TENSORBOARD.getComponentName());
    Assert.assertEquals(1, tensorboardComp.getNumberOfContainers().intValue());
    Assert.assertEquals(resource.getMemorySize(),
        tensorboardComp.getResource().calcMemoryMB());
    Assert.assertEquals(resource.getVirtualCores(),
        tensorboardComp.getResource().getCpus().intValue());

    Assert.assertEquals("./run-TENSORBOARD.sh",
        tensorboardComp.getLaunchCommand());

    // Check docker image
    if (runJobCli.getRunJobParameters().getTensorboardDockerImage() != null) {
      Assert.assertEquals(
          runJobCli.getRunJobParameters().getTensorboardDockerImage(),
          tensorboardComp.getArtifact().getId());
    } else{
      Assert.assertNull(tensorboardComp.getArtifact());
    }

    YarnServiceJobSubmitter yarnServiceJobSubmitter =
        (YarnServiceJobSubmitter) runJobCli.getJobSubmitter();

    String expectedLaunchScript =
        "#!/bin/bash\n" + "echo \"CLASSPATH:$CLASSPATH\"\n"
            + "echo \"HADOOP_CONF_DIR:$HADOOP_CONF_DIR\"\n"
            + "echo \"HADOOP_TOKEN_FILE_LOCATION:$HADOOP_TOKEN_FILE_LOCATION\"\n"
            + "echo \"JAVA_HOME:$JAVA_HOME\"\n"
            + "echo \"LD_LIBRARY_PATH:$LD_LIBRARY_PATH\"\n"
            + "echo \"HADOOP_HDFS_HOME:$HADOOP_HDFS_HOME\"\n"
            + "export LC_ALL=C && tensorboard --logdir=" + runJobCli
            .getRunJobParameters().getCheckpointPath() + "\n";

    verifyLaunchScriptForComponet(yarnServiceJobSubmitter, serviceSpec,
        TaskType.TENSORBOARD, expectedLaunchScript);
  }

  private void verifyLaunchScriptForComponet(
      YarnServiceJobSubmitter yarnServiceJobSubmitter, Service serviceSpec,
      TaskType taskType, String expectedLaunchScriptContent) throws Exception {
    Map<String, String> componentToLocalLaunchScriptMap =
        yarnServiceJobSubmitter.getComponentToLocalLaunchScriptPath();

    String path = componentToLocalLaunchScriptMap.get(
        taskType.getComponentName());

    byte[] encoded = Files.readAllBytes(Paths.get(path));
    String scriptContent = new String(encoded, Charset.defaultCharset());

    Assert.assertEquals(expectedLaunchScriptContent, scriptContent);
  }

  @Test
  public void testBasicRunJobForSingleNodeTrainingWithTensorboard()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=2G,vcores=2", "--tensorboard",
            "--verbose" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());

    Assert.assertEquals(2, serviceSpec.getComponents().size());

    commonTestSingleNodeTraining(serviceSpec);
    verifyTensorboardComponent(runJobCli, serviceSpec,
        Resources.createResource(4096, 1));
  }

  @Test
  public void testBasicRunJobForSingleNodeTrainingWithGeneratedCheckpoint()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--num_workers", "1",
            "--worker_launch_cmd", "python run-job.py", "--worker_resources",
            "memory=2G,vcores=2", "--tensorboard", "--verbose" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());

    Assert.assertEquals(2, serviceSpec.getComponents().size());

    commonTestSingleNodeTraining(serviceSpec);
    verifyTensorboardComponent(runJobCli, serviceSpec,
        Resources.createResource(4096, 1));
  }

  @Test
  public void testParameterStorageForTrainingJob() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=2G,vcores=2", "--tensorboard", "true",
            "--verbose" });
    SubmarineStorage storage =
        mockClientContext.getRuntimeFactory().getSubmarineStorage();
    Map<String, String> jobInfo = storage.getJobInfoByName("my-job");
    Assert.assertTrue(jobInfo.size() > 0);
    Assert.assertEquals(jobInfo.get(StorageKeyConstants.INPUT_PATH),
        "s3://input");
  }

  @Test
  public void testAddQuicklinksWithoutTensorboard() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose", "--quicklink",
            "AAA=http://master-0:8321", "--quicklink",
            "BBB=http://worker-0:1234" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyQuicklink(serviceSpec, ImmutableMap
        .of("AAA", "http://master-0.my-job.username.null:8321", "BBB",
            "http://worker-0.my-job.username.null:1234"));
  }

  @Test
  public void testAddQuicklinksWithTensorboard() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose", "--quicklink",
            "AAA=http://master-0:8321", "--quicklink",
            "BBB=http://worker-0:1234", "--tensorboard" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(4, serviceSpec.getComponents().size());

    commonVerifyDistributedTrainingSpec(serviceSpec);

    verifyQuicklink(serviceSpec, ImmutableMap
        .of("AAA", "http://master-0.my-job.username.null:8321", "BBB",
            "http://worker-0.my-job.username.null:1234",
            YarnServiceJobSubmitter.TENSORBOARD_QUICKLINK_LABEL,
            "http://tensorboard-0.my-job.username.null:6006"));
  }
}
