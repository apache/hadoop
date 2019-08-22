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


package org.apache.hadoop.yarn.submarine.client.cli.runjob.pytorch;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
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
 * Test class that verifies the correctness of PyTorch
 * CLI configuration parsing.
 */
public class TestRunJobCliParsingPyTorch {

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testBasicRunJobForSingleNodeTraining() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path",
            "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--verbose",
            "--wait_job_finish" });

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    assertTrue(RunJobParameters.class +
            " must be an instance of " +
            PyTorchRunJobParameters.class,
        jobRunParameters instanceof PyTorchRunJobParameters);
    PyTorchRunJobParameters pyTorchParams =
        (PyTorchRunJobParameters) jobRunParameters;

    assertEquals(jobRunParameters.getInputPath(), "hdfs://input");
    assertEquals(jobRunParameters.getCheckpointPath(), "hdfs://output");
    assertEquals(pyTorchParams.getNumWorkers(), 1);
    assertEquals(pyTorchParams.getWorkerLaunchCmd(),
        "python run-job.py");
    assertEquals(Resources.createResource(4096, 2),
        pyTorchParams.getWorkerResource());
    assertTrue(SubmarineLogs.isVerbose());
    assertTrue(jobRunParameters.isWaitJobFinish());
  }

  @Test
  public void testNumPSCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path","hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--num_ps", "2" });
  }

  @Test
  public void testPSResourcesCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=2048M,vcores=2" });
  }

  @Test
  public void testPSDockerImageCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_docker_image", "psDockerImage" });
  }

  @Test
  public void testPSLaunchCommandCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_launch_cmd", "psLaunchCommand" });
  }

  @Test
  public void testTensorboardCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--tensorboard" });
  }

  @Test
  public void testTensorboardResourcesCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--tensorboard_resources", "memory=2048M,vcores=2" });
  }

  @Test
  public void testTensorboardDockerImageCannotBeDefined() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("cannot be defined for PyTorch jobs");
    runJobCli.run(
        new String[] {"--framework", "pytorch",
            "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--checkpoint_path", "hdfs://output",
            "--num_workers", "3",
            "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--tensorboard_docker_image", "TBDockerImage" });
  }

}
