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


package org.apache.hadoop.yarn.submarine.client.cli;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRunJobCliParsing {
  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Test
  public void testPrintHelp() {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    JobMonitor mockJobMonitor = mock(JobMonitor.class);
    RunJobCli runJobCli = new RunJobCli(mockClientContext, mockJobSubmitter,
        mockJobMonitor);
    runJobCli.printUsages();
  }

  private MockClientContext getMockClientContext()
      throws IOException, YarnException {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    when(mockJobSubmitter.submitJob(any(RunJobParameters.class))).thenReturn(
        ApplicationId.newInstance(1234L, 1));
    JobMonitor mockJobMonitor = mock(JobMonitor.class);
    SubmarineStorage storage = mock(SubmarineStorage.class);
    RuntimeFactory rtFactory = mock(RuntimeFactory.class);

    when(rtFactory.getJobSubmitterInstance()).thenReturn(mockJobSubmitter);
    when(rtFactory.getJobMonitorInstance()).thenReturn(mockJobMonitor);
    when(rtFactory.getSubmarineStorage()).thenReturn(storage);

    mockClientContext.setRuntimeFactory(rtFactory);
    return mockClientContext;
  }

  @Test
  public void testBasicRunJobForDistributedTraining() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4G,vcores=4", "--tensorboard", "true",
            "--ps_launch_cmd", "python run-ps.py", "--verbose" });

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    Assert.assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    Assert.assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    Assert.assertEquals(jobRunParameters.getNumPS(), 2);
    Assert.assertEquals(jobRunParameters.getPSLaunchCmd(), "python run-ps.py");
    Assert.assertEquals(Resources.createResource(4096, 4),
        jobRunParameters.getPsResource());
    Assert.assertEquals(jobRunParameters.getWorkerLaunchCmd(),
        "python run-job.py");
    Assert.assertEquals(Resources.createResource(2048, 2),
        jobRunParameters.getWorkerResource());
    Assert.assertEquals(jobRunParameters.getDockerImageName(),
        "tf-docker:1.1.0");
    Assert.assertTrue(SubmarineLogs.isVerbose());
  }

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
            "true", "--verbose", "--wait_job_finish" });

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    Assert.assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    Assert.assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    Assert.assertEquals(jobRunParameters.getNumWorkers(), 1);
    Assert.assertEquals(jobRunParameters.getWorkerLaunchCmd(),
        "python run-job.py");
    Assert.assertEquals(Resources.createResource(4096, 2),
        jobRunParameters.getWorkerResource());
    Assert.assertTrue(SubmarineLogs.isVerbose());
    Assert.assertTrue(jobRunParameters.isWaitJobFinish());
  }

  @Test
  public void testNoInputPathOptionSpecified() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    String expectedErrorMessage = "\"--" + CliConstants.INPUT_PATH + "\" is absent";
    String actualMessage = "";
    try {
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--checkpoint_path", "hdfs://output",
              "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
              "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
              "true", "--verbose", "--wait_job_finish"});
    } catch (ParseException e) {
      actualMessage = e.getMessage();
      e.printStackTrace();
    }
    Assert.assertEquals(expectedErrorMessage, actualMessage);
  }

  /**
   * when only run tensorboard, input_path is not needed
   * */
  @Test
  public void testNoInputPathOptionButOnlyRunTensorboard() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    boolean success = true;
    try {
      runJobCli.run(
          new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--num_workers", "0", "--tensorboard", "--verbose",
              "--tensorboard_resources", "memory=2G,vcores=2",
              "--tensorboard_docker_image", "tb_docker_image:001"});
    } catch (ParseException e) {
      success = false;
    }
    Assert.assertTrue(success);
  }

  @Test
  public void testLaunchCommandPatternReplace() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py --input=%input_path% --model_dir=%checkpoint_path% --export_dir=%saved_model_path%/savedmodel",
            "--worker_resources", "memory=2048,vcores=2", "--ps_resources",
            "memory=4096,vcores=4", "--tensorboard", "true", "--ps_launch_cmd",
            "python run-ps.py --input=%input_path% --model_dir=%checkpoint_path%/model",
            "--verbose" });

    Assert.assertEquals(
        "python run-job.py --input=hdfs://input --model_dir=hdfs://output "
            + "--export_dir=hdfs://output/savedmodel",
        runJobCli.getRunJobParameters().getWorkerLaunchCmd());
    Assert.assertEquals(
        "python run-ps.py --input=hdfs://input --model_dir=hdfs://output/model",
        runJobCli.getRunJobParameters().getPSLaunchCmd());
  }

  @Test
  public void testResourceUnitParsing() throws Exception {
    Resource res = CliUtils.createResourceFromString("memory=20g,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20 * 1024, 3), res);

    res = CliUtils.createResourceFromString("memory=20G,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20 * 1024, 3), res);

    res = CliUtils.createResourceFromString("memory=20M,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20, 3), res);

    res = CliUtils.createResourceFromString("memory=20m,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20, 3), res);

    res = CliUtils.createResourceFromString("memory-mb=20,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20, 3), res);

    res = CliUtils.createResourceFromString("memory-mb=20m,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20, 3), res);

    res = CliUtils.createResourceFromString("memory-mb=20G,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(20 * 1024, 3), res);

    // W/o unit for memory means bits, and 20 bits will be rounded to 0
    res = CliUtils.createResourceFromString("memory=20,vcores=3",
        ResourceUtils.getResourcesTypeInfo());
    Assert.assertEquals(Resources.createResource(0, 3), res);

    // Test multiple resources
    List<ResourceTypeInfo> resTypes = new ArrayList<>(
        ResourceUtils.getResourcesTypeInfo());
    resTypes.add(ResourceTypeInfo.newInstance(ResourceInformation.GPU_URI, ""));
    ResourceUtils.reinitializeResources(resTypes);
    res = CliUtils.createResourceFromString("memory=2G,vcores=3,gpu=0",
        resTypes);
    Assert.assertEquals(2 * 1024, res.getMemorySize());
    Assert.assertEquals(0, res.getResourceValue(ResourceInformation.GPU_URI));

    res = CliUtils.createResourceFromString("memory=2G,vcores=3,gpu=3",
        resTypes);
    Assert.assertEquals(2 * 1024, res.getMemorySize());
    Assert.assertEquals(3, res.getResourceValue(ResourceInformation.GPU_URI));

    res = CliUtils.createResourceFromString("memory=2G,vcores=3",
        resTypes);
    Assert.assertEquals(2 * 1024, res.getMemorySize());
    Assert.assertEquals(0, res.getResourceValue(ResourceInformation.GPU_URI));

    res = CliUtils.createResourceFromString("memory=2G,vcores=3,yarn.io/gpu=0",
        resTypes);
    Assert.assertEquals(2 * 1024, res.getMemorySize());
    Assert.assertEquals(0, res.getResourceValue(ResourceInformation.GPU_URI));

    res = CliUtils.createResourceFromString("memory=2G,vcores=3,yarn.io/gpu=3",
        resTypes);
    Assert.assertEquals(2 * 1024, res.getMemorySize());
    Assert.assertEquals(3, res.getResourceValue(ResourceInformation.GPU_URI));

    // TODO, add more negative tests.
  }
}
