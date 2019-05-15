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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.TensorFlowRunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.api.TensorFlowRole;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.HadoopEnvironmentSetup;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorBoardLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorFlowPsLaunchCommand;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow.command.TensorFlowWorkerLaunchCommand;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class is an abstract base class for testing Tensorboard and TensorFlow
 * launch commands.
 */
public abstract class AbstractTFLaunchCommandTestHelper {
  private TensorFlowRole taskType;
  private boolean useTaskTypeOverride;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private void assertScriptContainsExportedEnvVar(List<String> fileContents,
      String varName) {
    String expected = String.format("export %s=", varName);
    assertScriptContainsLine(fileContents, expected);
  }

  protected static void assertScriptContainsExportedEnvVarWithValue(
      List<String> fileContents, String varName, String value) {
    String expected = String.format("export %s=%s", varName, value);
    assertScriptContainsLine(fileContents, expected);
  }

  protected static void assertScriptContainsLine(List<String> fileContents,
      String expected) {
    String message = String.format(
        "File does not contain expected line '%s'!" + " File contents: %s",
        expected, Arrays.toString(fileContents.toArray()));
    assertTrue(message, fileContents.contains(expected));
  }

  protected static void assertScriptContainsLineWithRegex(
      List<String> fileContents,
      String regex) {
    String message = String.format(
        "File does not contain expected line '%s'!" + " File contents: %s",
        regex, Arrays.toString(fileContents.toArray()));

    for (String line : fileContents) {
      if (line.matches(regex)) {
        return;
      }
    }
    fail(message);
  }

  protected static void assertScriptDoesNotContainLine(
      List<String> fileContents, String expected) {
    String message = String.format(
        "File contains unexpected line '%s'!" + " File contents: %s",
        expected, Arrays.toString(fileContents.toArray()));
    assertFalse(message, fileContents.contains(expected));
  }


  private AbstractLaunchCommand createLaunchCommandByTaskType(
      TensorFlowRole taskType, TensorFlowRunJobParameters params)
      throws IOException {
    MockClientContext mockClientContext = new MockClientContext();
    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    HadoopEnvironmentSetup hadoopEnvSetup =
        new HadoopEnvironmentSetup(mockClientContext, fsOperations);
    Component component = new Component();
    Configuration yarnConfig = new Configuration();

    return createLaunchCommandByTaskTypeInternal(taskType, params,
        hadoopEnvSetup, component, yarnConfig);
  }

  private AbstractLaunchCommand createLaunchCommandByTaskTypeInternal(
      TensorFlowRole taskType, TensorFlowRunJobParameters params,
      HadoopEnvironmentSetup hadoopEnvSetup, Component component,
      Configuration yarnConfig)
      throws IOException {
    if (taskType == TensorFlowRole.TENSORBOARD) {
      return new TensorBoardLaunchCommand(
          hadoopEnvSetup, getTaskType(taskType), component, params);
    } else if (taskType == TensorFlowRole.WORKER
        || taskType == TensorFlowRole.PRIMARY_WORKER) {
      return new TensorFlowWorkerLaunchCommand(
          hadoopEnvSetup, getTaskType(taskType), component, params, yarnConfig);
    } else if (taskType == TensorFlowRole.PS) {
      return new TensorFlowPsLaunchCommand(
          hadoopEnvSetup, getTaskType(taskType), component, params, yarnConfig);
    }
    throw new IllegalStateException("Unknown role!");
  }

  protected void overrideTaskType(TensorFlowRole taskType) {
    this.taskType = taskType;
    this.useTaskTypeOverride = true;
  }

  private TensorFlowRole getTaskType(TensorFlowRole taskType) {
    if (useTaskTypeOverride) {
      return this.taskType;
    }
    return taskType;
  }

  protected void testHdfsRelatedEnvironmentIsUndefined(TensorFlowRole taskType,
      TensorFlowRunJobParameters params) throws IOException {
    AbstractLaunchCommand launchCommand =
        createLaunchCommandByTaskType(taskType, params);

    expectedException.expect(IOException.class);
    expectedException
        .expectMessage("Failed to detect HDFS-related environments.");
    launchCommand.generateLaunchScript();
  }

  protected List<String> testHdfsRelatedEnvironmentIsDefined(
      TensorFlowRole taskType, TensorFlowRunJobParameters params)
      throws IOException {
    AbstractLaunchCommand launchCommand =
        createLaunchCommandByTaskType(taskType, params);

    String result = launchCommand.generateLaunchScript();
    assertNotNull(result);
    File resultFile = new File(result);
    assertTrue(resultFile.exists());

    List<String> fileContents = Files.readAllLines(
        Paths.get(resultFile.toURI()),
        Charset.forName("UTF-8"));

    assertEquals("#!/bin/bash", fileContents.get(0));
    assertScriptContainsExportedEnvVar(fileContents, "HADOOP_HOME");
    assertScriptContainsExportedEnvVar(fileContents, "HADOOP_YARN_HOME");
    assertScriptContainsExportedEnvVarWithValue(fileContents,
        "HADOOP_HDFS_HOME", "testHdfsHome");
    assertScriptContainsExportedEnvVarWithValue(fileContents,
        "HADOOP_COMMON_HOME", "testHdfsHome");
    assertScriptContainsExportedEnvVarWithValue(fileContents, "HADOOP_CONF_DIR",
        "$WORK_DIR");
    assertScriptContainsExportedEnvVarWithValue(fileContents, "JAVA_HOME",
        "testJavaHome");
    assertScriptContainsExportedEnvVarWithValue(fileContents, "LD_LIBRARY_PATH",
        "$LD_LIBRARY_PATH:$JAVA_HOME/lib/amd64/server");
    assertScriptContainsExportedEnvVarWithValue(fileContents, "CLASSPATH",
        "`$HADOOP_HDFS_HOME/bin/hadoop classpath --glob`");

    return fileContents;
  }

}
