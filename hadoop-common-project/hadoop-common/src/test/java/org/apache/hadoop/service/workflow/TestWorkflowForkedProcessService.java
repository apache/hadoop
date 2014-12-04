/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.service.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test the {@link TestWorkflowForkedProcessService}
 */
public class TestWorkflowForkedProcessService extends WorkflowServiceTestBase {
  private static final Logger
      log = LoggerFactory.getLogger(TestWorkflowForkedProcessService.class);

  private static final Logger
      processLog =
      LoggerFactory.getLogger("org.apache.hadoop.services.workflow.Process");
  public static final int RECENT_OUTPUT_SLEEP_DURATION = 4000;

  private WorkflowForkedProcessService process;
  private File testDir = new File("target");
  private ProcessCommandFactory commandFactory;
  private Map<String, String> env = new HashMap<String, String>();

  @Before
  public void setupProcesses() {
    commandFactory = ProcessCommandFactory.createProcessCommandFactory();
  }

  @After
  public void stopProcesses() {
    ServiceOperations.stop(process);
  }

  @Test
  public void testLs() throws Throwable {

    initProcess(commandFactory.ls(testDir));
    assertExecCompletes(0);
    // assert that the service did not fail
    assertNull(process.getFailureCause());
  }

  @Test
  
  public void testExitCodes() throws Throwable {
    skipOnWindows();
    initProcess(commandFactory.exitFalse());
    exec();
    assertFalse(process.isProcessRunning());
    Integer exitCode = process.getExitCode();
    assertNotNull("null exit code", exitCode);
    assertTrue(exitCode != 0);
    int corrected = process.getExitCodeSignCorrected();
    assertEquals(1, corrected);
    // assert that the exit code was uprated to a service failure
    assertNotNull(process.getFailureCause());
  }

  @Test
  public void testEcho() throws Throwable {
    skipOnWindows();

    String echoText = "hello, world";
    initProcess(commandFactory.echo(echoText));

    assertExecCompletes(0);
    assertStringInOutput(echoText, getFinalOutput());

  }

  protected void assertExecCompletes(int expected) throws InterruptedException {
    exec();
    assertFalse(process.isProcessRunning());
    Integer exitCode = process.getExitCode();
    assertNotNull("null exit code", exitCode);
    assertEquals(expected, exitCode.intValue());
  }

  @Test
  public void testSetenv() throws Throwable {

    String var = "TEST_RUN";
    String val = "TEST-RUN-ENV-VALUE";
    env.put(var, val);
    initProcess(commandFactory.env());
    exec();

    assertEquals(0, process.getExitCode().intValue());
    assertStringInOutput(val, getFinalOutput());
  }

  /**
   * Get the final output. includes a quick sleep for the tail output
   * @return the last output
   */
  private List<String> getFinalOutput() {
    return process.getRecentOutput();
  }

  private WorkflowForkedProcessService initProcess(List<String> commands) throws
      IOException {
    process = new WorkflowForkedProcessService(name.getMethodName(), env,
        commands);
    process.init(new Configuration());

    return process;
  }

  public void exec() throws InterruptedException {
    assertNotNull(process);
    EndOfServiceWaiter waiter = new EndOfServiceWaiter(process);
    process.start();
    waiter.waitForServiceToStop(5000);
  }

}
