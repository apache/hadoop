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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command.AbstractTFLaunchCommandTestHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup.DOCKER_HADOOP_HDFS_HOME;
import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup.DOCKER_JAVA_HOME;

/**
 * This class is to test the {@link TensorBoardLaunchCommand}.
 */
public class TestTensorBoardLaunchCommand extends
    AbstractTFLaunchCommandTestHelper {

  @Test
  public void testHdfsRelatedEnvironmentIsUndefined() throws IOException {
    TensorFlowRunJobParameters params = new TensorFlowRunJobParameters();
    params.setInputPath("hdfs://bla");
    params.setName("testJobname");
    params.setCheckpointPath("something");

    testHdfsRelatedEnvironmentIsUndefined(TensorFlowRole.TENSORBOARD,
        params);
  }

  @Test
  public void testHdfsRelatedEnvironmentIsDefined() throws IOException {
    TensorFlowRunJobParameters params = new TensorFlowRunJobParameters();
    params.setName("testName");
    params.setCheckpointPath("testCheckpointPath");
    params.setInputPath("hdfs://bla");
    params.setEnvars(ImmutableList.of(
        DOCKER_HADOOP_HDFS_HOME + "=" + "testHdfsHome",
        DOCKER_JAVA_HOME + "=" + "testJavaHome"));

    List<String> fileContents =
        testHdfsRelatedEnvironmentIsDefined(TensorFlowRole.TENSORBOARD,
            params);
    assertScriptContainsExportedEnvVarWithValue(fileContents, "LC_ALL",
        "C && tensorboard --logdir=testCheckpointPath");
  }

  @Test
  public void testCheckpointPathUndefined() throws IOException {
    MockClientContext mockClientContext = new MockClientContext();
    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    HadoopEnvironmentSetup hadoopEnvSetup =
        new HadoopEnvironmentSetup(mockClientContext, fsOperations);

    Component component = new Component();
    TensorFlowRunJobParameters params = new TensorFlowRunJobParameters();
    params.setCheckpointPath(null);

    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("CheckpointPath must not be null");
    new TensorBoardLaunchCommand(hadoopEnvSetup, TensorFlowRole.TENSORBOARD,
            component, params);
  }

  @Test
  public void testCheckpointPathEmptyString() throws IOException {
    MockClientContext mockClientContext = new MockClientContext();
    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    HadoopEnvironmentSetup hadoopEnvSetup =
        new HadoopEnvironmentSetup(mockClientContext, fsOperations);

    Component component = new Component();
    TensorFlowRunJobParameters params = new TensorFlowRunJobParameters();
    params.setCheckpointPath("");

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("CheckpointPath must not be empty");
    new TensorBoardLaunchCommand(hadoopEnvSetup, TensorFlowRole.TENSORBOARD,
        component, params);
  }
}