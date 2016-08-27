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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Test the {@link LongLivedProcess} class
 */
public class TestLongLivedProcess extends WorkflowServiceTestBase implements
    LongLivedProcessLifecycleEvent {
  private static final Logger
      LOG = LoggerFactory.getLogger(TestLongLivedProcess.class);

  private LongLivedProcess process;
  private File testDir = new File("target");
  private ProcessCommandFactory commandFactory;
  private volatile boolean started, stopped;

  @Before
  public void setupProcesses() {
    commandFactory = ProcessCommandFactory.createProcessCommandFactory();
  }

  @After
  public void stopProcesses() {
    if (process != null) {
      process.stop();
    }
  }

  @Test
  public void testLs() throws Throwable {

    initProcess(commandFactory.ls(testDir));
    process.start();
    //in-thread wait
    process.run();

    //here stopped
    assertTrue("process start callback not received", started);
    assertTrue("process stop callback not received", stopped);
    assertFalse(process.isRunning());
    assertEquals(0, process.getExitCode().intValue());

    assertStringInOutput("test-classes", getFinalOutput());
  }

  @Test
  public void testExitCodes() throws Throwable {

    initProcess(commandFactory.exitFalse());
    process.start();
    //in-thread wait
    process.run();

    //here stopped

    assertFalse(process.isRunning());
    int exitCode = process.getExitCode();
    assertTrue(exitCode != 0);
    int corrected = process.getExitCodeSignCorrected();

    assertEquals(1, corrected);
  }

  @Test
  public void testEcho() throws Throwable {

    String echoText = "hello, world";
    initProcess(commandFactory.echo(echoText));
    process.start();
    //in-thread wait
    process.run();

    //here stopped
    assertTrue("process stop callback not received", stopped);
    assertEquals(0, process.getExitCode().intValue());
    assertStringInOutput(echoText, getFinalOutput());
  }

  @Test
  public void testSetenv() throws Throwable {

    String var = "TEST_RUN";
    String val = "TEST-RUN-ENV-VALUE";
    initProcess(commandFactory.env());
    process.setEnv(var, val);
    process.start();
    //in-thread wait
    process.run();

    //here stopped
    assertTrue("process stop callback not received", stopped);
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

  private LongLivedProcess initProcess(List<String> commands) {
    process = new LongLivedProcess(name.getMethodName(), LOG, commands);
    process.setLifecycleCallback(this);
    return process;
  }

  /**
   * Handler for callback events on the process
   */

  @Override
  public void onProcessStarted(LongLivedProcess process) {
    started = true;
  }

  /**
   * Handler for callback events on the process
   */
  @Override
  public void onProcessExited(LongLivedProcess process,
      int exitCode,
      int signCorrectedCode) {
    stopped = true;
  }
}
