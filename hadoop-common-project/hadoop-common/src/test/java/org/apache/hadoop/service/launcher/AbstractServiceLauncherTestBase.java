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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitCodeProvider;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class AbstractServiceLauncherTestBase extends Assert implements
    LauncherExitCodes {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractServiceLauncherTestBase.class);

  /**
   * All tests have a short life.
   */
  @Rule
  public Timeout testTimeout = new Timeout(15000);

  /**
   * Rule to provide the method name
   */
  @Rule
  public TestName methodName = new TestName();

  /**
   * Turn off the exit util JVM exits, downgrading them to exception throws.
   */
  @BeforeClass
  public static void disableJVMExits() {
    ExitUtil.disableSystemExit();
    ExitUtil.disableSystemHalt();
  }

  /**
   * rule to name the thread JUnit.
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Formatted fail
   * @param format format string
   * @param args argument list. If the last argument is a throwable, it
   * is used as the inner cause of the exception
   * @throws AssertionError with the formatted message
   */
  protected static void failf(String format, Object... args) {
    String message = String.format(Locale.ENGLISH, format, args);
    AssertionError error = new AssertionError(message);
    int len = args.length;
    if (len > 0 && args[len - 1] instanceof Throwable) {
      error.initCause((Throwable) args[len - 1]);
    }
    throw error;
  }

  /**
   * Assert that a service is in a state
   * @param service
   * @param expected
   */
  protected void assertInState(Service service, Service.STATE expected) {
    assertNotNull(service);
    Service.STATE actual = service.getServiceState();
    if (actual != expected) {
      
      failf("Service %s in state %s expected state: %s",
          service.getName(), actual, expected);
    }
  }

  /**
   * Assert a service has stopped
   * @param service service
   */
  protected void assertStopped(Service service) {
    assertInState(service, Service.STATE.STOPPED);
  }

  /**
   * Assert that an exception code matches the value expected
   * @param expected expected value
   * @param text text in exception -can be null
   * @param e exception providing the actual value
   */
  protected void assertExceptionDetails(int expected,
      String text,
      ExitCodeProvider e) {
    assertNotNull(e);
    String toString = e.toString();
    int exitCode = e.getExitCode();
    boolean failed = expected != exitCode;
    failed |= StringUtils.isNotEmpty(text)
              && !StringUtils.contains(toString, text);
    if (failed) {
      String error = String.format(
          "Expected exception with exit code %d and text \"%s\""
          + " but got the exit code %d"
          + " and text \"%s\"",
          expected, text,
          exitCode, toString);
      LOG.error(error, e);
      AssertionError assertionError = new AssertionError(error);
      assertionError.initCause((Throwable) e);
      throw assertionError;
    }
  }

  protected void assertServiceCreationFails(String args) {
    assertLaunchOutcome(EXIT_SERVICE_CREATION_FAILURE, "", args);
  }

  /**
   * Assert a launch outcome
   * @param expected expected value
   * @param text text in exception -can be null
   * @param args CLI args
   */
  protected void assertLaunchOutcome(int expected,
      String text,
      String... args) {
    try {
      ServiceLauncher.serviceMain(args);
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(expected, text, e);
    }
  }

  /**
   * Assert a launch runs
   * @param args CLI args
   */
  protected void assertRuns(String... args) {
    assertLaunchOutcome(0, "", args);
  }

  /**
   * Init and start a service
   * @param service the service
   * @return the service
   */
  protected <S extends Service> S run(S service) {
    assertNotNull(service);
    service.init(new Configuration());
    service.start();
    return service;
  }

  protected String configFile(Configuration conf) throws IOException {
    File file = File.createTempFile("conf", ".xml", new File("target"));
    OutputStream fos = new FileOutputStream(file);
    try {
      conf.writeXml(fos);
    } finally {
      IOUtils.closeStream(fos);
    }
    return file.toString();
  }

  protected Configuration newConf(String... kvp) {
    int len = kvp.length;
    assertEquals("unbalanced keypair len of " + len, 0, len % 2);
    Configuration conf = new Configuration(false);
    for (int i = 0; i < len; i += 2) {
      conf.set(kvp[i], kvp[i + 1]);
    }
    return conf;
  }

  /** varargs to list conversion */
  protected List<String> asList(String... args) {
    return Arrays.asList(args);
  }

  /**
   * Launch a service with the given list of arguments. Returns
   * the service launcher, from which the created service can be extracted
   * via {@link ServiceLauncher#getService()}.
   * @param serviceClass service class to create
   * @param conf configuration
   * @param args list of arguments
   * @param <S> service type
   * @return the service launcher
   * @throws ExitUtil.ExitException if the launch's exit code != 0
   */
  protected <S extends Service> ServiceLauncher<S> launchService(
      Class serviceClass,
      Configuration conf,
      List<String> args) throws ExitUtil.ExitException {
    ServiceLauncher<S> serviceLauncher =
        new ServiceLauncher<S>(serviceClass.getName());
    ExitUtil.ExitException exitException =
        serviceLauncher.launchService(conf, args, false);
    if (exitException.getExitCode() == 0) {
      return serviceLauncher;
    } else {
      throw exitException;
    }
  }

  /**
   * Launch a service with the given list of arguments. Returns
   * the service launcher, from which the created service can be extracted
   * via {@link ServiceLauncher#getService()}.
   * @param serviceClass service class to create
   * @param conf configuration
   * @param args varargs launch arguments
   * @param <S> service type
   * @return the service launcher
   * @throws ExitUtil.ExitException  if the launch's exit code != 0
   */
  protected <S extends Service> ServiceLauncher<S> launchService(
      Class serviceClass,
      Configuration conf,
      String... args) throws ExitUtil.ExitException {
    return launchService(serviceClass,conf, Arrays.asList(args));
  }

  /**
   * Launch expecting an exception
   * @param serviceClass service class to create
   * @param conf configuration
   * @param expectedText expected text; may be "" or null
   * @param errorCode error code 
   * @param args varargs launch arguments
   * @return the exception returned if there was a match
   * @throws AssertionError on a mismatch of expectation and actual
   */
  protected ExitUtil.ExitException launchExpectingException(Class serviceClass,
      Configuration conf,
      String expectedText,
      int errorCode,
      String... args) {
    try {
      ServiceLauncher<Service> launch = launchService(serviceClass, conf, args);

      failf("Expected an exception with error code %d and text \"%s\" "
            + " -but the service completed with :%s",
          errorCode, expectedText, launch.getServiceException());
      return null;
    } catch (ExitUtil.ExitException e) {
      int actualCode = e.getExitCode();
      if (errorCode != actualCode ||
          !StringUtils.contains(e.toString(), expectedText) ) {
        failf("Expected an exception with error code %d and text \"%s\" "
              + " -but the service threw an exception with exit code %d: %s",
            errorCode, expectedText, actualCode, e);
      }    
      return e;
    }
  }
}
