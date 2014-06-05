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

public class AbstractServiceLauncherTestBase extends Assert implements
    LauncherExitCodes {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractServiceLauncherTestBase.class);
  @Rule
  public Timeout testTimeout = new Timeout(10000);

  @Rule
  public TestName methodName = new TestName();

  @BeforeClass
  public static void disableJVMExits() {
    ExitUtil.disableSystemExit();
    ExitUtil.disableSystemHalt();
  }

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  protected void assertInState(Service service, Service.STATE expected) {
    Service.STATE actual = service.getServiceState();
    if (actual != expected) {
      fail("Service " + service.getName() + " in state " + actual
           + " -expected " + expected);
    }
  }

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
   * @param text text in exception -can be null
   * @param args CLI args
   */
  protected void assertRuns(String... args) {
    assertLaunchOutcome(0, "", args);
  }

  /**
   * Init and start a service
   * @param svc the service
   * @return the service
   */
  protected <S extends Service> S run(S svc) {
    svc.init(new Configuration());
    svc.start();
    return svc;
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
}
