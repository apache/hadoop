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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.submarine.client.cli.RunJobCli;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineConfiguration;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.StorageKeyConstants;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceJobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_SUCCESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestYarnServiceRunJobCli {
  @Before
  public void before() throws IOException, YarnException {
    SubmarineLogs.verboseOff();
    AppAdminClient serviceClient = mock(AppAdminClient.class);
    when(serviceClient.actionLaunch(any(String.class), any(String.class),
        any(Long.class), any(String.class))).thenReturn(EXIT_SUCCESS);
    when(serviceClient.getStatusString(any(String.class))).thenReturn(
        "{\"id\": \"application_1234_1\"}");
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose"});
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--tensorboard", "--ps_launch_cmd", "python run-ps.py",
            "--verbose"});
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=2G,vcores=2", "--verbose"});

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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "0", "--tensorboard", "--verbose"});

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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "0", "--tensorboard", "--verbose",
            "--tensorboard_resources", "memory=2G,vcores=2",
            "--tensorboard_docker_image", "tb_docker_image:001"});

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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--num_workers", "0", "--tensorboard", "--verbose",
            "--tensorboard_resources", "memory=2G,vcores=2",
            "--tensorboard_docker_image", "tb_docker_image:001"});

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
    } else {
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=2G,vcores=2", "--tensorboard",
            "--verbose"});
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--num_workers", "1",
            "--worker_launch_cmd", "python run-job.py", "--worker_resources",
            "memory=2G,vcores=2", "--tensorboard", "--verbose"});
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=2G,vcores=2", "--tensorboard", "true",
            "--verbose"});
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose", "--quicklink",
            "AAA=http://master-0:8321", "--quicklink",
            "BBB=http://worker-0:1234"});
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
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose", "--quicklink",
            "AAA=http://master-0:8321", "--quicklink",
            "BBB=http://worker-0:1234", "--tensorboard"});
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

  /**
   * Basic test.
   * In one hand, create local temp file/dir for hdfs URI in
   * local staging dir.
   * In the other hand, use MockRemoteDirectoryManager mock
   * implementation when check FileStatus or exists of HDFS file/dir
   * --localization hdfs:///user/yarn/script1.py:.
   * --localization /temp/script2.py:./
   * --localization /temp/script2.py:/opt/script.py
   */
  @Test
  public void testRunJobWithBasicLocalization() throws Exception {
    String remoteUrl = "hdfs:///user/yarn/script1.py";
    String containerLocal1 = ".";
    String localUrl = "/temp/script2.py";
    String containerLocal2 = "./";
    String containerLocal3 = "/opt/script.py";
    String fakeLocalDir = System.getProperty("java.io.tmpdir");
    // create local file, we need to put it under local temp dir
    File localFile1 = new File(fakeLocalDir,
        new Path(localUrl).getName());
    localFile1.createNewFile();


    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    RemoteDirectoryManager spyRdm =
        spy(mockClientContext.getRemoteDirectoryManager());
    mockClientContext.setRemoteDirectoryMgr(spyRdm);

    // create remote file in local staging dir to simulate HDFS
    Path stagingDir = mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea("my-job", true);
    File remoteFile1 = new File(stagingDir.toUri().getPath()
        + "/" + new Path(remoteUrl).getName());
    remoteFile1.createNewFile();

    Assert.assertTrue(localFile1.exists());
    Assert.assertTrue(remoteFile1.exists());

    runJobCli.run(
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose",
            "--localization",
            remoteUrl + ":" + containerLocal1,
            "--localization",
            localFile1.getAbsolutePath() + ":" + containerLocal2,
            "--localization",
            localFile1.getAbsolutePath() + ":" + containerLocal3});
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());

    // No remote dir and hdfs file exists. Ensure download 0 times
    verify(spyRdm, times(0)).copyRemoteToLocal(
        anyString(), anyString());
    // Ensure local original files are not deleted
    Assert.assertTrue(localFile1.exists());

    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    Assert.assertEquals(3, files.size());
    ConfigFile file = files.get(0);
    Assert.assertEquals(ConfigFile.TypeEnum.STATIC, file.getType());
    String expectedSrcLocalization = remoteUrl;
    Assert.assertEquals(expectedSrcLocalization,
        file.getSrcFile());
    String expectedDstFileName = new Path(remoteUrl).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(1);
    Assert.assertEquals(ConfigFile.TypeEnum.STATIC, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(localUrl).getName();
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    expectedDstFileName = new Path(localUrl).getName();
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());

    file = files.get(2);
    Assert.assertEquals(ConfigFile.TypeEnum.STATIC, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(localUrl).getName();
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    expectedDstFileName = new Path(localUrl).getName();
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());

    // Ensure env value is correct
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");
    String expectedMounts = new Path(containerLocal3).getName()
        + ":" + containerLocal3 + ":rw";
    Assert.assertTrue(env.contains(expectedMounts));

    remoteFile1.delete();
    localFile1.delete();
  }

  /**
   * Non HDFS remote URI test.
   * --localization https://a/b/1.patch:.
   * --localization s3a://a/dir:/opt/mys3dir
   */
  @Test
  public void testRunJobWithNonHDFSRemoteLocalization() throws Exception {
    String remoteUri1 = "https://a/b/1.patch";
    String containerLocal1 = ".";
    String remoteUri2 = "s3a://a/s3dir";
    String containerLocal2 = "/opt/mys3dir";

    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    RemoteDirectoryManager spyRdm =
        spy(mockClientContext.getRemoteDirectoryManager());
    mockClientContext.setRemoteDirectoryMgr(spyRdm);

    // create remote file in local staging dir to simulate HDFS
    Path stagingDir = mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea("my-job", true);
    File remoteFile1 = new File(stagingDir.toUri().getPath()
        + "/" + new Path(remoteUri1).getName());
    remoteFile1.createNewFile();

    File remoteDir1 = new File(stagingDir.toUri().getPath()
        + "/" + new Path(remoteUri2).getName());
    remoteDir1.mkdir();
    File remoteDir1File1 = new File(remoteDir1, "afile");
    remoteDir1File1.createNewFile();

    Assert.assertTrue(remoteFile1.exists());
    Assert.assertTrue(remoteDir1.exists());
    Assert.assertTrue(remoteDir1File1.exists());

    String suffix1 = "_" + remoteDir1.lastModified()
        + "-" + mockClientContext.getRemoteDirectoryManager()
        .getRemoteFileSize(remoteUri2);
    runJobCli.run(
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose",
            "--localization",
            remoteUri1 + ":" + containerLocal1,
            "--localization",
            remoteUri2 + ":" + containerLocal2});
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());

    // Ensure download remote dir 2 times
    verify(spyRdm, times(2)).copyRemoteToLocal(
        anyString(), anyString());

    // Ensure downloaded temp files are deleted
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUri1).getName()).exists());
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUri2).getName()).exists());

    // Ensure zip file are deleted
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUri2).getName()
        + "_" + suffix1 + ".zip").exists());

    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    Assert.assertEquals(2, files.size());
    ConfigFile file = files.get(0);
    Assert.assertEquals(ConfigFile.TypeEnum.STATIC, file.getType());
    String expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(remoteUri1).getName();
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    String expectedDstFileName = new Path(remoteUri1).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(1);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(remoteUri2).getName() + suffix1 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());

    expectedDstFileName = new Path(containerLocal2).getName();
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());

    // Ensure env value is correct
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");
    String expectedMounts = new Path(remoteUri2).getName()
        + ":" + containerLocal2 + ":rw";
    Assert.assertTrue(env.contains(expectedMounts));

    remoteDir1File1.delete();
    remoteFile1.delete();
    remoteDir1.delete();
  }

  /**
   * Test HDFS dir localization.
   * --localization hdfs:///user/yarn/mydir:./mydir1
   * --localization hdfs:///user/yarn/mydir2:/opt/dir2:rw
   * --localization hdfs:///user/yarn/mydir:.
   * --localization hdfs:///user/yarn/mydir2:./
   */
  @Test
  public void testRunJobWithHdfsDirLocalization() throws Exception {
    String remoteUrl = "hdfs:///user/yarn/mydir";
    String containerPath = "./mydir1";
    String remoteUrl2 = "hdfs:///user/yarn/mydir2";
    String containPath2 = "/opt/dir2";
    String containerPath3 = ".";
    String containerPath4 = "./";
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    RemoteDirectoryManager spyRdm =
        spy(mockClientContext.getRemoteDirectoryManager());
    mockClientContext.setRemoteDirectoryMgr(spyRdm);
    // create remote file in local staging dir to simulate HDFS
    Path stagingDir = mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea("my-job", true);
    File remoteDir1 = new File(stagingDir.toUri().getPath().toString()
        + "/" + new Path(remoteUrl).getName());
    remoteDir1.mkdir();
    File remoteFile1 = new File(remoteDir1.getAbsolutePath() + "/1.py");
    File remoteFile2 = new File(remoteDir1.getAbsolutePath() + "/2.py");
    remoteFile1.createNewFile();
    remoteFile2.createNewFile();

    File remoteDir2 = new File(stagingDir.toUri().getPath().toString()
        + "/" + new Path(remoteUrl2).getName());
    remoteDir2.mkdir();
    File remoteFile3 = new File(remoteDir1.getAbsolutePath() + "/3.py");
    File remoteFile4 = new File(remoteDir1.getAbsolutePath() + "/4.py");
    remoteFile3.createNewFile();
    remoteFile4.createNewFile();

    Assert.assertTrue(remoteDir1.exists());
    Assert.assertTrue(remoteDir2.exists());

    String suffix1 = "_" + remoteDir1.lastModified()
        + "-" + mockClientContext.getRemoteDirectoryManager()
        .getRemoteFileSize(remoteUrl);
    String suffix2 = "_" + remoteDir2.lastModified()
        + "-" + mockClientContext.getRemoteDirectoryManager()
        .getRemoteFileSize(remoteUrl2);
    runJobCli.run(
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose",
            "--localization",
            remoteUrl + ":" + containerPath,
            "--localization",
            remoteUrl2 + ":" + containPath2 + ":rw",
            "--localization",
            remoteUrl + ":" + containerPath3,
            "--localization",
            remoteUrl2 + ":" + containerPath4});
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());

    // Ensure download remote dir 4 times
    verify(spyRdm, times(4)).copyRemoteToLocal(
        anyString(), anyString());

    // Ensure downloaded temp files are deleted
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUrl).getName()).exists());
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUrl2).getName()).exists());
    // Ensure zip file are deleted
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUrl).getName()
        + suffix1 + ".zip").exists());
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(remoteUrl2).getName()
        + suffix2 + ".zip").exists());

    // Ensure files will be localized
    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    Assert.assertEquals(4, files.size());
    ConfigFile file = files.get(0);
    // The hdfs dir should be download and compress and let YARN to uncompress
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    String expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(remoteUrl).getName() + suffix1 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());

    // Relative path in container, but not "." or "./". Use its own name
    String expectedDstFileName = new Path(containerPath).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(1);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(remoteUrl2).getName() + suffix2 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());

    expectedDstFileName = new Path(containPath2).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(2);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(remoteUrl).getName() + suffix1 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    // Relative path in container ".", use remote path name
    expectedDstFileName = new Path(remoteUrl).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(3);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(remoteUrl2).getName() + suffix2 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    // Relative path in container "./", use remote path name
    expectedDstFileName = new Path(remoteUrl2).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    // Ensure mounts env value is correct. Add one mount string
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");

    String expectedMounts =
        new Path(containPath2).getName() + ":" + containPath2 + ":rw";
    Assert.assertTrue(env.contains(expectedMounts));

    remoteFile1.delete();
    remoteFile2.delete();
    remoteFile3.delete();
    remoteFile4.delete();
    remoteDir1.delete();
    remoteDir2.delete();
  }

  /**
   * Test if file/dir to be localized whose size exceeds limit.
   * Max 10MB in configuration, mock remote will
   * always return file size 100MB.
   * This configuration will fail the job which has remoteUri
   * But don't impact local dir/file
   *
   * --localization https://a/b/1.patch:.
   * --localization s3a://a/dir:/opt/mys3dir
   * --localization /temp/script2.py:./
   */
  @Test
  public void testRunJobRemoteUriExceedLocalizationSize() throws Exception {
    String remoteUri1 = "https://a/b/1.patch";
    String containerLocal1 = ".";
    String remoteUri2 = "s3a://a/s3dir";
    String containerLocal2 = "/opt/mys3dir";
    String localUri1 = "/temp/script2";
    String containerLocal3 = "./";

    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    SubmarineConfiguration submarineConf = new SubmarineConfiguration();
    RemoteDirectoryManager spyRdm =
        spy(mockClientContext.getRemoteDirectoryManager());
    mockClientContext.setRemoteDirectoryMgr(spyRdm);
    /**
     * Max 10MB, mock remote will always return file size 100MB.
     * */
    submarineConf.set(
        SubmarineConfiguration.LOCALIZATION_MAX_ALLOWED_FILE_SIZE_MB,
        "10");
    mockClientContext.setSubmarineConfig(submarineConf);

    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    // create remote file in local staging dir to simulate
    Path stagingDir = mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea("my-job", true);
    File remoteFile1 = new File(stagingDir.toUri().getPath()
        + "/" + new Path(remoteUri1).getName());
    remoteFile1.createNewFile();
    File remoteDir1 = new File(stagingDir.toUri().getPath()
        + "/" + new Path(remoteUri2).getName());
    remoteDir1.mkdir();

    File remoteDir1File1 = new File(remoteDir1, "afile");
    remoteDir1File1.createNewFile();

    String fakeLocalDir = System.getProperty("java.io.tmpdir");
    // create local file, we need to put it under local temp dir
    File localFile1 = new File(fakeLocalDir,
        new Path(localUri1).getName());
    localFile1.createNewFile();

    Assert.assertTrue(remoteFile1.exists());
    Assert.assertTrue(remoteDir1.exists());
    Assert.assertTrue(remoteDir1File1.exists());

    String suffix1 = "_" + remoteDir1.lastModified()
        + "-" + remoteDir1.length();
    try {
      runJobCli = new RunJobCli(mockClientContext);
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--input_path", "s3://input", "--checkpoint_path", "s3://output",
              "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
              "python run-job.py", "--worker_resources",
              "memory=2048M,vcores=2",
              "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
              "ps.image", "--worker_docker_image", "worker.image",
              "--ps_launch_cmd", "python run-ps.py", "--verbose",
              "--localization",
              remoteUri1 + ":" + containerLocal1});
    } catch (IOException e) {
      // Shouldn't have exception because it's within file size limit
      Assert.assertFalse(true);
    }
    // we should download because fail fast
    verify(spyRdm, times(1)).copyRemoteToLocal(
        anyString(), anyString());
    try {
      // reset
      reset(spyRdm);
      runJobCli = new RunJobCli(mockClientContext);
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--input_path", "s3://input", "--checkpoint_path", "s3://output",
              "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
              "python run-job.py", "--worker_resources",
              "memory=2048M,vcores=2",
              "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
              "ps.image", "--worker_docker_image", "worker.image",
              "--ps_launch_cmd", "python run-ps.py", "--verbose",
              "--localization",
              remoteUri1 + ":" + containerLocal1,
              "--localization",
              remoteUri2 + ":" + containerLocal2,
              "--localization",
              localFile1.getAbsolutePath() + ":" + containerLocal3});
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage()
          .contains("104857600 exceeds configured max size:10485760"));
      // we shouldn't do any download because fail fast
      verify(spyRdm, times(0)).copyRemoteToLocal(
          anyString(), anyString());
    }

    try {
      runJobCli = new RunJobCli(mockClientContext);
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--input_path", "s3://input", "--checkpoint_path", "s3://output",
              "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
              "python run-job.py", "--worker_resources",
              "memory=2048M,vcores=2",
              "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
              "ps.image", "--worker_docker_image", "worker.image",
              "--ps_launch_cmd", "python run-ps.py", "--verbose",
              "--localization",
              localFile1.getAbsolutePath() + ":" + containerLocal3});
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage()
          .contains("104857600 exceeds configured max size:10485760"));
      // we shouldn't do any download because fail fast
      verify(spyRdm, times(0)).copyRemoteToLocal(
          anyString(), anyString());
    }

    localFile1.delete();
    remoteDir1File1.delete();
    remoteFile1.delete();
    remoteDir1.delete();
  }

  /**
   * Test remote Uri doesn't exist.
   * */
  @Test
  public void testRunJobWithNonExistRemoteUri() throws Exception {
    String remoteUri1 = "hdfs:///a/b/1.patch";
    String containerLocal1 = ".";
    String localUri1 = "/a/b/c";
    String containerLocal2 = "./";
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();

    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    try {
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--input_path", "s3://input", "--checkpoint_path", "s3://output",
              "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
              "python run-job.py", "--worker_resources",
              "memory=2048M,vcores=2",
              "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
              "ps.image", "--worker_docker_image", "worker.image",
              "--ps_launch_cmd", "python run-ps.py", "--verbose",
              "--localization",
              remoteUri1 + ":" + containerLocal1});
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage()
          .contains("doesn't exists"));
    }

    try {
      runJobCli = new RunJobCli(mockClientContext);
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--input_path", "s3://input", "--checkpoint_path", "s3://output",
              "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
              "python run-job.py", "--worker_resources",
              "memory=2048M,vcores=2",
              "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
              "ps.image", "--worker_docker_image", "worker.image",
              "--ps_launch_cmd", "python run-ps.py", "--verbose",
              "--localization",
              localUri1 + ":" + containerLocal2});
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage()
          .contains("doesn't exists"));
    }
  }

  /**
   * Test local dir
   * --localization /user/yarn/mydir:./mydir1
   * --localization /user/yarn/mydir2:/opt/dir2:rw
   * --localization /user/yarn/mydir2:.
   */
  @Test
  public void testRunJobWithLocalDirLocalization() throws Exception {
    String fakeLocalDir = System.getProperty("java.io.tmpdir");
    String localUrl = "/user/yarn/mydir";
    String containerPath = "./mydir1";
    String localUrl2 = "/user/yarn/mydir2";
    String containPath2 = "/opt/dir2";
    String containerPath3 = ".";

    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    RemoteDirectoryManager spyRdm =
        spy(mockClientContext.getRemoteDirectoryManager());
    mockClientContext.setRemoteDirectoryMgr(spyRdm);
    // create local file
    File localDir1 = new File(fakeLocalDir,
        localUrl);
    localDir1.mkdirs();
    File temp1 = new File(localDir1.getAbsolutePath() + "/1.py");
    File temp2 = new File(localDir1.getAbsolutePath() + "/2.py");
    temp1.createNewFile();
    temp2.createNewFile();

    File localDir2 = new File(fakeLocalDir,
        localUrl2);
    localDir2.mkdirs();
    File temp3 = new File(localDir1.getAbsolutePath() + "/3.py");
    File temp4 = new File(localDir1.getAbsolutePath() + "/4.py");
    temp3.createNewFile();
    temp4.createNewFile();

    Assert.assertTrue(localDir1.exists());
    Assert.assertTrue(localDir2.exists());

    String suffix1 = "_" + localDir1.lastModified()
        + "-" + localDir1.length();
    String suffix2 = "_" + localDir2.lastModified()
        + "-" + localDir2.length();

    runJobCli.run(
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path", "s3://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4096M,vcores=4", "--ps_docker_image",
            "ps.image", "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose",
            "--localization",
            fakeLocalDir + localUrl + ":" + containerPath,
            "--localization",
            fakeLocalDir + localUrl2 + ":" + containPath2 + ":rw",
            "--localization",
            fakeLocalDir + localUrl2 + ":" + containerPath3});

    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());

    // we shouldn't do any download
    verify(spyRdm, times(0)).copyRemoteToLocal(
        anyString(), anyString());

    // Ensure local original files are not deleted
    Assert.assertTrue(localDir1.exists());
    Assert.assertTrue(localDir2.exists());

    // Ensure zip file are deleted
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(localUrl).getName()
        + suffix1 + ".zip").exists());
    Assert.assertFalse(new File(System.getProperty("java.io.tmpdir")
        + "/" + new Path(localUrl2).getName()
        + suffix2 + ".zip").exists());

    // Ensure dirs will be zipped and localized
    List<ConfigFile> files = serviceSpec.getConfiguration().getFiles();
    Assert.assertEquals(3, files.size());
    ConfigFile file = files.get(0);
    Path stagingDir = mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea("my-job", true);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    String expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(localUrl).getName() + suffix1 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    String expectedDstFileName = new Path(containerPath).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(1);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(localUrl2).getName() + suffix2 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    expectedDstFileName = new Path(containPath2).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    file = files.get(2);
    Assert.assertEquals(ConfigFile.TypeEnum.ARCHIVE, file.getType());
    expectedSrcLocalization = stagingDir.toUri().getPath()
        + "/" + new Path(localUrl2).getName() + suffix2 + ".zip";
    Assert.assertEquals(expectedSrcLocalization,
        new Path(file.getSrcFile()).toUri().getPath());
    expectedDstFileName = new Path(localUrl2).getName();
    Assert.assertEquals(expectedDstFileName, file.getDestFile());

    // Ensure mounts env value is correct
    String env = serviceSpec.getConfiguration().getEnv()
        .get("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS");
    String expectedMounts = new Path(containPath2).getName()
        + ":" + containPath2 + ":rw";

    Assert.assertTrue(env.contains(expectedMounts));

    temp1.delete();
    temp2.delete();
    temp3.delete();
    temp4.delete();
    localDir2.delete();
    localDir1.delete();
  }

  /**
   * Test zip function.
   * A dir "/user/yarn/mydir" has two files and one subdir
   * */
  @Test
  public void testYarnServiceSubmitterZipFunction()
      throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    YarnServiceJobSubmitter submitter =
        (YarnServiceJobSubmitter)mockClientContext
            .getRuntimeFactory().getJobSubmitterInstance();
    String fakeLocalDir = System.getProperty("java.io.tmpdir");
    String localUrl = "/user/yarn/mydir";
    String localSubDirName = "subdir1";
    // create local file
    File localDir1 = new File(fakeLocalDir,
        localUrl);
    localDir1.mkdirs();
    File temp1 = new File(localDir1.getAbsolutePath() + "/1.py");
    File temp2 = new File(localDir1.getAbsolutePath() + "/2.py");
    temp1.createNewFile();
    temp2.createNewFile();


    File localSubDir = new File(localDir1.getAbsolutePath(), localSubDirName);
    localSubDir.mkdir();
    File temp3 = new File(localSubDir.getAbsolutePath(), "3.py");
    temp3.createNewFile();


    String zipFilePath = submitter.zipDir(localDir1.getAbsolutePath(),
        fakeLocalDir + "/user/yarn/mydir.zip");
    File zipFile = new File(zipFilePath);
    File unzipTargetDir = new File(fakeLocalDir, "unzipDir");
    FileUtil.unZip(zipFile, unzipTargetDir);
    Assert.assertTrue(
        new File(fakeLocalDir + "/unzipDir/1.py").exists());
    Assert.assertTrue(
        new File(fakeLocalDir + "/unzipDir/2.py").exists());
    Assert.assertTrue(
        new File(fakeLocalDir + "/unzipDir/subdir1").exists());
    Assert.assertTrue(
        new File(fakeLocalDir + "/unzipDir/subdir1/3.py").exists());

    zipFile.delete();
    unzipTargetDir.delete();
    temp1.delete();
    temp2.delete();
    temp3.delete();
    localSubDir.delete();
    localDir1.delete();
  }

}
