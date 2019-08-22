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


package org.apache.hadoop.yarn.submarine.client.cli.runjob;
import com.google.common.collect.Lists;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.PyTorchRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.yarn.submarine.client.cli.runjob.TestRunJobCliParsingCommon.getMockClientContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * This class contains some test methods to test common CLI parsing
 * functionality (including TF / PyTorch) of the run job Submarine command.
 */
@RunWith(Parameterized.class)
public class TestRunJobCliParsingParameterized {

  private final Framework framework;

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<>();
    params.add(new Object[]{Framework.TENSORFLOW });
    params.add(new Object[]{Framework.PYTORCH });
    return params;
  }

  public TestRunJobCliParsingParameterized(Framework framework) {
    this.framework = framework;
  }

  private String getFrameworkName() {
    return framework.getValue();
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

  @Test
  public void testNoInputPathOptionSpecified() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    String expectedErrorMessage = "\"--" + CliConstants.INPUT_PATH + "\"" +
        " is absent";
    String actualMessage = "";
    try {
      runJobCli.run(
          new String[]{"--framework", getFrameworkName(),
              "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
              "--checkpoint_path", "hdfs://output",
              "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
              "--worker_resources", "memory=4g,vcores=2", "--verbose",
              "--wait_job_finish"});
    } catch (ParseException e) {
      actualMessage = e.getMessage();
      e.printStackTrace();
    }
    assertEquals(expectedErrorMessage, actualMessage);
  }

  @Test
  public void testJobWithoutName() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    String expectedErrorMessage =
        "--" + CliConstants.NAME + " is absent";
    String actualMessage = "";
    try {
      runJobCli.run(
          new String[]{"--framework", getFrameworkName(),
              "--docker_image", "tf-docker:1.1.0",
              "--num_workers", "0", "--verbose"});
    } catch (ParseException e) {
      actualMessage = e.getMessage();
      e.printStackTrace();
    }
    assertEquals(expectedErrorMessage, actualMessage);
  }

  @Test
  public void testLaunchCommandPatternReplace() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    List<String> parameters = Lists.newArrayList("--framework",
        getFrameworkName(),
        "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
        "--input_path", "hdfs://input", "--checkpoint_path",
        "hdfs://output",
        "--num_workers", "3",
        "--worker_launch_cmd", "python run-job.py --input=%input_path% " +
            "--model_dir=%checkpoint_path% " +
            "--export_dir=%saved_model_path%/savedmodel",
        "--worker_resources", "memory=2048,vcores=2");

    if (framework == Framework.TENSORFLOW) {
      parameters.addAll(Lists.newArrayList(
          "--ps_resources", "memory=4096,vcores=4",
          "--ps_launch_cmd", "python run-ps.py --input=%input_path% " +
              "--model_dir=%checkpoint_path%/model",
          "--verbose"));
    }

    runJobCli.run(parameters.toArray(new String[0]));

    RunJobParameters runJobParameters = checkExpectedFrameworkParams(runJobCli);

    if (framework == Framework.TENSORFLOW) {
      TensorFlowRunJobParameters tensorFlowParams =
          (TensorFlowRunJobParameters) runJobParameters;
      assertEquals(
          "python run-job.py --input=hdfs://input --model_dir=hdfs://output "
              + "--export_dir=hdfs://output/savedmodel",
          tensorFlowParams.getWorkerLaunchCmd());
      assertEquals(
          "python run-ps.py --input=hdfs://input " +
              "--model_dir=hdfs://output/model",
          tensorFlowParams.getPSLaunchCmd());
    } else if (framework == Framework.PYTORCH) {
      PyTorchRunJobParameters pyTorchParameters =
          (PyTorchRunJobParameters) runJobParameters;
      assertEquals(
          "python run-job.py --input=hdfs://input --model_dir=hdfs://output "
              + "--export_dir=hdfs://output/savedmodel",
          pyTorchParameters.getWorkerLaunchCmd());
    }
  }

  private RunJobParameters checkExpectedFrameworkParams(RunJobCli runJobCli) {
    RunJobParameters runJobParameters = runJobCli.getRunJobParameters();

    if (framework == Framework.TENSORFLOW) {
      assertTrue(RunJobParameters.class + " must be an instance of " +
              TensorFlowRunJobParameters.class,
          runJobParameters instanceof TensorFlowRunJobParameters);
    } else if (framework == Framework.PYTORCH) {
      assertTrue(RunJobParameters.class + " must be an instance of " +
              PyTorchRunJobParameters.class,
          runJobParameters instanceof PyTorchRunJobParameters);
    }
    return runJobParameters;
  }

}
