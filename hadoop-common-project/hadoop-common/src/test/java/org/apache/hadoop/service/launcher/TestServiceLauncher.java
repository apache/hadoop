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

package org.apache.hadoop.service.launcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.BreakableService;
import org.apache.hadoop.service.launcher.testservices.FailingStopInStartService;
import org.apache.hadoop.service.launcher.testservices.InitInConstructorLaunchableService;
import org.apache.hadoop.service.launcher.testservices.LaunchableRunningService;
import org.apache.hadoop.service.launcher.testservices.NoArgsAllowedService;
import org.apache.hadoop.service.launcher.testservices.NullBindLaunchableService;
import org.apache.hadoop.service.launcher.testservices.RunningService;
import org.apache.hadoop.service.launcher.testservices.StoppingInStartLaunchableService;
import org.apache.hadoop.service.launcher.testservices.StringConstructorOnlyService;

import static org.apache.hadoop.service.launcher.LauncherArguments.*;

import static org.apache.hadoop.test.GenericTestUtils.*;
import static org.apache.hadoop.service.launcher.testservices.ExceptionInExecuteLaunchableService.*;

import org.junit.Test;

public class TestServiceLauncher extends AbstractServiceLauncherTestBase {

  @Test
  public void testRunService() throws Throwable {
    assertRuns(RunningService.NAME);
  }

  @Test
  public void testNullBindService() throws Throwable {
    assertRuns(NullBindLaunchableService.NAME);
  }

  @Test
  public void testServiceLaunchStringConstructor() throws Throwable {
    assertRuns(StringConstructorOnlyService.NAME);
  }

  /**
   * Test the behaviour of service stop logic.
   */
  @Test
  public void testStopInStartup() throws Throwable {
    FailingStopInStartService svc = new FailingStopInStartService();
    svc.init(new Configuration());
    svc.start();
    assertStopped(svc);
    Throwable cause = svc.getFailureCause();
    assertNotNull(cause);
    assertTrue(cause instanceof ServiceLaunchException);
    assertTrue(svc.waitForServiceToStop(0));
    ServiceLaunchException e = (ServiceLaunchException) cause;
    assertEquals(FailingStopInStartService.EXIT_CODE, e.getExitCode());
  }

  @Test
  public void testEx() throws Throwable {
    assertLaunchOutcome(EXIT_EXCEPTION_THROWN,
        OTHER_EXCEPTION_TEXT,
        NAME);
  }

  /**
   * This test verifies that exceptions in the
   * {@link LaunchableService#execute()} method are relayed if an instance of
   * an exit exceptions, and forwarded if not.
   */
  @Test
  public void testServiceLaunchException() throws Throwable {
    assertLaunchOutcome(EXIT_OTHER_FAILURE,
        SLE_TEXT,
        NAME,
        ARG_THROW_SLE);
  }

  @Test
  public void testIOE() throws Throwable {
    assertLaunchOutcome(IOE_EXIT_CODE,
        EXIT_IN_IOE_TEXT,
        NAME,
        ARG_THROW_IOE);
  }

  @Test
  public void testThrowable() throws Throwable {
    assertLaunchOutcome(EXIT_EXCEPTION_THROWN,
        "java.lang.OutOfMemoryError",
        NAME,
        ARG_THROWABLE);
  }

  /**
   * As the exception is doing some formatting tricks, these
   * tests verify that exception arguments are being correctly
   * used as initializers.
   */
  @Test
  public void testBasicExceptionFormatting() throws Throwable {
    ServiceLaunchException ex = new ServiceLaunchException(0, "%03x", 32);
    assertExceptionContains("020", ex);
  }

  @Test
  public void testNotEnoughArgsExceptionFormatting() throws Throwable {
    ServiceLaunchException ex = new ServiceLaunchException(0, "%03x");
    assertExceptionContains("%03x", ex);
  }

  @Test
  public void testInnerCause() throws Throwable {

    Exception cause = new Exception("cause");
    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x: %s", 32, cause);
    assertExceptionContains("020", ex);
    assertExceptionContains("cause", ex);
    assertSame(cause, ex.getCause());
  }

  @Test
  public void testInnerCauseNotInFormat() throws Throwable {

    Exception cause = new Exception("cause");
    ServiceLaunchException ex =
        new ServiceLaunchException(0, "%03x:", 32, cause);
    assertExceptionContains("020", ex);
    assertFalse(ex.getMessage().contains("cause"));
    assertSame(cause, ex.getCause());
  }

  @Test
  public void testServiceInitInConstructor() throws Throwable {
    assertRuns(InitInConstructorLaunchableService.NAME);
  }

  @Test
  public void testRunNoArgsAllowedService() throws Throwable {
    assertRuns(NoArgsAllowedService.NAME);
  }

  @Test
  public void testNoArgsOneArg() throws Throwable {
    assertLaunchOutcome(EXIT_COMMAND_ARGUMENT_ERROR, "1",
        NoArgsAllowedService.NAME, "one");
  }

  @Test
  public void testNoArgsHasConfsStripped() throws Throwable {
    assertRuns(
        NoArgsAllowedService.NAME,
        LauncherArguments.ARG_CONF_PREFIXED,
        configFile(newConf()));
  }

  @Test
  public void testRunLaunchableService() throws Throwable {
    assertRuns(LaunchableRunningService.NAME);
  }

  @Test
  public void testArgBinding() throws Throwable {
    assertLaunchOutcome(EXIT_OTHER_FAILURE,
        "",
        LaunchableRunningService.NAME,
        LaunchableRunningService.ARG_FAILING);
  }

  @Test
  public void testStoppingInStartLaunchableService() throws Throwable {
    assertRuns(StoppingInStartLaunchableService.NAME);
  }

  @Test
  public void testShutdownHookNullReference() throws Throwable {
    new ServiceShutdownHook(null).run();
  }

  @Test
  public void testShutdownHook() throws Throwable {
    BreakableService service = new BreakableService();
    setServiceToTeardown(service);
    ServiceShutdownHook hook = new ServiceShutdownHook(service);
    hook.run();
    assertStopped(service);
  }

  @Test
  public void testFailingHookCaught() throws Throwable {
    BreakableService service = new BreakableService(false, false, true);
    setServiceToTeardown(service);
    ServiceShutdownHook hook = new ServiceShutdownHook(service);
    hook.run();
    assertStopped(service);
  }

}
