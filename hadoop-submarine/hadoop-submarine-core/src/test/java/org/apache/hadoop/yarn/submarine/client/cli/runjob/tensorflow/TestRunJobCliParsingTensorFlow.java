/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.yarn.submarine.client.cli.runjob.tensorflow;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.RunJobCli;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.hadoop.yarn.submarine.client.cli.runjob.TestRunJobCliParsingCommon.getMockClientContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class that verifies the correctness of TensorFlow
 * CLI configuration parsing.
 */
public class TestRunJobCliParsingTensorFlow {

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNoInputPathOptionSpecified() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    String expectedErrorMessage = "\"--" + CliConstants.INPUT_PATH +
        "\" is absent";
    String actualMessage = "";
    try {
      runJobCli.run(
          new String[]{"--framework", "tensorflow",
              "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--checkpoint_path", "hdfs://output",
              "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
              "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
              "true", "--verbose", "--wait_job_finish"});
    } catch (ParseException e) {
      actualMessage = e.getMessage();
      e.printStackTrace();
    }
    assertEquals(expectedErrorMessage, actualMessage);
  }

  @Test
  public void testBasicRunJobForDistributedTraining() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--framework", "tensorflow",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4G,vcores=4", "--tensorboard", "true",
            "--ps_launch_cmd", "python run-ps.py", "--keytab", "/keytab/path",
            "--principal", "user/_HOST@domain.com", "--distribute_keytab",
            "--verbose" });

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    assertTrue(RunJobParameters.class +
        " must be an instance of " +
        TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    assertEquals(tensorFlowParams.getNumPS(), 2);
    assertEquals(tensorFlowParams.getPSLaunchCmd(), "python run-ps.py");
    assertEquals(Resources.createResource(4096, 4),
        tensorFlowParams.getPsResource());
    assertEquals(tensorFlowParams.getWorkerLaunchCmd(),
        "python run-job.py");
    assertEquals(Resources.createResource(2048, 2),
        tensorFlowParams.getWorkerResource());
    assertEquals(jobRunParameters.getDockerImageName(),
        "tf-docker:1.1.0");
    assertEquals(jobRunParameters.getKeytab(),
        "/keytab/path");
    assertEquals(jobRunParameters.getPrincipal(),
        "user/_HOST@domain.com");
    assertTrue(jobRunParameters.isDistributeKeytab());
    assertTrue(SubmarineLogs.isVerbose());
  }

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] { "--framework", "tensorflow",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path",
            "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
            "true", "--verbose", "--wait_job_finish" });

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    assertTrue(RunJobParameters.class +
            " must be an instance of " +
            TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    assertEquals(tensorFlowParams.getNumWorkers(), 1);
    assertEquals(tensorFlowParams.getWorkerLaunchCmd(),
        "python run-job.py");
    assertEquals(Resources.createResource(4096, 2),
        tensorFlowParams.getWorkerResource());
    assertTrue(SubmarineLogs.isVerbose());
    assertTrue(jobRunParameters.isWaitJobFinish());
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
          new String[]{"--framework", "tensorflow",
              "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--num_workers", "0", "--tensorboard", "--verbose",
              "--tensorboard_resources", "memory=2G,vcores=2",
              "--tensorboard_docker_image", "tb_docker_image:001"});
    } catch (ParseException e) {
      success = false;
    }
    assertTrue(success);
  }
}
