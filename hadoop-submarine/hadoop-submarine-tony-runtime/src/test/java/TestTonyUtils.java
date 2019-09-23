/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.submarine.client.cli.param.ParametersHolder;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.runjob.RunJobCli;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.RuntimeFactory;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.submarine.runtimes.common.SubmarineStorage;
import org.apache.hadoop.yarn.submarine.runtimes.tony.TonyUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

public class TestTonyUtils {

  private MockClientContext getMockClientContext()
      throws IOException, YarnException {
    MockClientContext mockClientContext = new MockClientContext();
    JobSubmitter mockJobSubmitter = mock(JobSubmitter.class);
    when(mockJobSubmitter.submitJob(
        any(ParametersHolder.class))).thenReturn(
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

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }

  @Test
  public void testTonyConfFromClientContext() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    runJobCli.run(
        new String[] {"--framework", "tensorflow", "--name", "my-job",
            "--docker_image", "tf-docker:1.1.0",
            "--input_path", "hdfs://input",
            "--num_workers", "3", "--num_ps", "2", "--worker_launch_cmd",
            "python run-job.py", "--worker_resources", "memory=2048M,vcores=2",
            "--ps_resources", "memory=4G,vcores=4", "--ps_launch_cmd",
            "python run-ps.py"});
    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    assertTrue(RunJobParameters.class + " must be an instance of " +
            TensorFlowRunJobParameters.class,
        jobRunParameters instanceof TensorFlowRunJobParameters);
    TensorFlowRunJobParameters tensorFlowParams =
        (TensorFlowRunJobParameters) jobRunParameters;

    Configuration tonyConf = TonyUtils
        .tonyConfFromClientContext(tensorFlowParams);
    Assert.assertEquals(jobRunParameters.getDockerImageName(),
        tonyConf.get(TonyConfigurationKeys.getContainerDockerKey()));
    Assert.assertEquals("3", tonyConf.get(TonyConfigurationKeys
        .getInstancesKey("worker")));
    Assert.assertEquals(tensorFlowParams.getWorkerLaunchCmd(),
        tonyConf.get(TonyConfigurationKeys
            .getExecuteCommandKey("worker")));
    Assert.assertEquals("2048", tonyConf.get(TonyConfigurationKeys
        .getResourceKey(Constants.WORKER_JOB_NAME, Constants.MEMORY)));
    Assert.assertEquals("2", tonyConf.get(TonyConfigurationKeys
        .getResourceKey(Constants.WORKER_JOB_NAME, Constants.VCORES)));
    Assert.assertEquals("4096", tonyConf.get(TonyConfigurationKeys
        .getResourceKey(Constants.PS_JOB_NAME, Constants.MEMORY)));
    Assert.assertEquals("4", tonyConf.get(TonyConfigurationKeys
        .getResourceKey(Constants.PS_JOB_NAME,
        Constants.VCORES)));
    Assert.assertEquals(tensorFlowParams.getPSLaunchCmd(),
        tonyConf.get(TonyConfigurationKeys.getExecuteCommandKey("ps")));
  }
}
