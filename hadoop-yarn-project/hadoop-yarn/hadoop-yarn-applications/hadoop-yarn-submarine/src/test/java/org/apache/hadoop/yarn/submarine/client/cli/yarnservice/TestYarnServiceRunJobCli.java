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

import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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

  @Test
  public void testBasicRunJobForDistributedTraining() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path",
            "s3://output", "--num_workers", "3", "--num_ps", "2",
            "--worker_launch_cmd", "python run-job.py", "--worker_resources",
            "memory=2048M,vcores=2", "--ps_resources", "memory=4096M,vcores=4",
            "--tensorboard", "true", "--ps_docker_image", "ps.image",
            "--worker_docker_image", "worker.image",
            "--ps_launch_cmd", "python run-ps.py", "--verbose" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(3, serviceSpec.getComponents().size());
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

    // TODO, ADD TEST TO USE SERVICE CLIENT TO VALIDATE THE JSON SPEC
  }

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path",
            "s3://output", "--num_workers", "1", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2G,vcores=2",
            "--tensorboard", "true", "--verbose" });
    Service serviceSpec = getServiceSpecFromJobSubmitter(
        runJobCli.getJobSubmitter());
    Assert.assertEquals(1, serviceSpec.getComponents().size());
    Assert.assertTrue(
        serviceSpec.getComponent(TaskType.PRIMARY_WORKER.getComponentName())
            != null);
    Component primaryWorkerComp = serviceSpec.getComponent(
        TaskType.PRIMARY_WORKER.getComponentName());
    Assert.assertEquals(2048, primaryWorkerComp.getResource().calcMemoryMB());
    Assert.assertEquals(2,
        primaryWorkerComp.getResource().getCpus().intValue());

    Assert.assertTrue(SubmarineLogs.isVerbose());

    // TODO, ADD TEST TO USE SERVICE CLIENT TO VALIDATE THE JSON SPEC
  }

  @Test
  public void testParameterStorageForTrainingJob() throws Exception {
    MockClientContext mockClientContext =
        YarnServiceCliTestUtils.getMockClientContext();
    RunJobCli runJobCli = new RunJobCli(mockClientContext);
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "s3://input", "--checkpoint_path",
            "s3://output", "--num_workers", "1", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2G,vcores=2",
            "--tensorboard", "true", "--verbose" });
    SubmarineStorage storage =
        mockClientContext.getRuntimeFactory().getSubmarineStorage();
    Map<String, String> jobInfo = storage.getJobInfoByName("my-job");
    Assert.assertTrue(jobInfo.size() > 0);
    Assert.assertEquals(jobInfo.get(StorageKeyConstants.INPUT_PATH),
        "s3://input");
  }
}
