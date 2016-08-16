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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

/**
 * Test base for workflow service tests.
 */
public abstract class WorkflowServiceTestBase extends Assert {
  private static final Logger
      log = LoggerFactory.getLogger(WorkflowServiceTestBase.class);

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

  protected void logState(ServiceParent p) {
    logService(p);
    for (Service s : p.getServices()) {
      logService(s);
    }
  }

  protected void logService(Service s) {
    log.info(s.toString());
    Throwable failureCause = s.getFailureCause();
    if (failureCause != null) {
      log.info("Failed in state {} with {}", s.getFailureState(),
          failureCause);
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
      log.info("CallableHandler::call");
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
      builder.append(s.toLowerCase(Locale.ENGLISH)).append('\n');
      if (s.contains(text)) {
        found = true;
        break;
      }
    }

    if (!found) {
      String message =
          "Text \"" + text + "\" not found in " + output.size() + " lines\n";
      fail(message + builder.toString());
    }
  }
}
