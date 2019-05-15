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

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.io.IOException;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class contains some test methods to test common functionality
 * (including TF / PyTorch) of the run job Submarine command.
 */
public class TestRunJobCliParsingCommon {

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public static MockClientContext getMockClientContext()
      throws IOException, YarnException {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    when(mockJobSubmitter.submitJob(any(ParametersHolder.class)))
        .thenReturn(ApplicationId.newInstance(1235L, 1));

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
  public void testAbsentFrameworkFallsBackToTensorFlow() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    runJobCli.run(
        new String[]{"--name", "my-job", "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path",
            "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
            "true", "--verbose", "--wait_job_finish"});
    RunJobParameters runJobParameters = runJobCli.getRunJobParameters();
    assertTrue("Default Framework should be TensorFlow!",
        runJobParameters instanceof TensorFlowRunJobParameters);
  }

  @Test
  public void testEmptyFrameworkOption() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(MissingArgumentException.class);
    expectedException.expectMessage("Missing argument for option: framework");

    runJobCli.run(
        new String[]{"--framework", "--name", "my-job",
            "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path",
            "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
            "true", "--verbose", "--wait_job_finish"});
  }

  @Test
  public void testInvalidFrameworkOption() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    assertFalse(SubmarineLogs.isVerbose());

    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Failed to parse Framework type");

    runJobCli.run(
        new String[]{"--framework", "bla", "--name", "my-job",
            "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input", "--checkpoint_path",
            "hdfs://output",
            "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
            "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
            "true", "--verbose", "--wait_job_finish"});
  }
}
