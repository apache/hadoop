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
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceParent;
import org.apache.hadoop.util.Shell;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Test base for workflow service tests.
 */
public abstract class WorkflowServiceTestBase extends Assert {
  private static final Logger
      LOG = LoggerFactory.getLogger(WorkflowServiceTestBase.class);

  /**
   * Set the timeout for every test
   */
  @Rule
  public Timeout testTimeout = new Timeout(15000);

  @Rule
  public TestName name = new TestName();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Assert that a service is in a state
   * @param service service
   * @param expected expected state
   */
  protected void assertInState(Service service, Service.STATE expected) {
    Service.STATE actual = service.getServiceState();
    if (actual != expected) {
      fail("Service " + service.getName() + " in state " + actual
           + " -expected " + expected);
    }
  }

  /**
   * assert that a service has stopped.
   * @param service service to check
   */
  protected void assertStopped(Service service) {
    assertInState(service, Service.STATE.STOPPED);
  }

  /**
   * Log the state of a service parent, and that of all its children
   * @param parent
   */
  protected void logState(ServiceParent parent) {
    logService(parent);
    for (Service s : parent.getServices()) {
      logService(s);
    }
  }

  /**
   * Log details about a service, including any failure cause.
   * @param service service to log
   */
  protected void logService(Service service) {
    LOG.info(service.toString());
    Throwable failureCause = service.getFailureCause();
    if (failureCause != null) {
      LOG.info("Failed in state {} with {}", service.getFailureState(),
          failureCause, failureCause);
    }
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

  /**
   * Handler for callable events
   */
  public static class CallableHandler implements Callable<String> {
    public volatile boolean notified = false;
    public final String result;

    public CallableHandler(String result) {
      this.result = result;
    }

    @Override
    public String call() throws Exception {
      LOG.info("CallableHandler::call");
      notified = true;
      return result;
    }
  }

  /**
   * Assert that a string is in an output list. Fails fast if the output
   * list is empty
   * @param text text to scan for
   * @param output list of output lines.
   */
  public void assertStringInOutput(String text, List<String> output) {
    assertTrue("Empty output list", !output.isEmpty());
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (String s : output) {
      builder.append(s).append('\n');
      if (s.contains(text)) {
        found = true;
        break;
      }
    }

    if (!found) {
      String message =
          "Text \"" + text + "\" not found in " + output.size() + " lines\n";
      fail(message + builder);
    }
  }


  /**
   * skip a test on windows
   */
  public static void skipOnWindows() {
    if (Shell.WINDOWS) {
      skip("Not supported on windows");
    }
  }

  public static void skip(String message) {
    LOG.warn("Skipping test: {}", message);
    Assume.assumeTrue(message, false);
  }
}
