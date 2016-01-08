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

import org.apache.hadoop.service.launcher.testservices.FailInConstructorService;
import org.apache.hadoop.service.launcher.testservices.FailInInitService;
import org.apache.hadoop.service.launcher.testservices.FailInStartService;
import org.apache.hadoop.service.launcher.testservices.FailingStopInStartService;
import org.junit.Test;

/**
 * Explore the ways in which the launcher is expected to (safely) fail.
 */
public class TestServiceLauncherCreationFailures extends
    AbstractServiceLauncherTestBase {

  public static final String SELF =
      "org.apache.hadoop.service.launcher.TestServiceLauncherCreationFailures";

  @Test
  public void testNoArgs() throws Throwable {
    try {
      ServiceLauncher.serviceMain();
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(EXIT_USAGE, "", e);
    }
  }

  @Test
  public void testUnknownClass() throws Throwable {
    assertServiceCreationFails("no.such.classname");
  }

  @Test
  public void testNotAService() throws Throwable {
    assertServiceCreationFails(SELF);
  }

  @Test
  public void testNoSimpleConstructor() throws Throwable {
    assertServiceCreationFails(
        "org.apache.hadoop.service.launcher.FailureTestService");
  }

  @Test
  public void testFailInConstructor() throws Throwable {
    assertServiceCreationFails(FailInConstructorService.NAME);
  }

  @Test
  public void testFailInInit() throws Throwable {
    assertLaunchOutcome(FailInInitService.EXIT_CODE, "",
        FailInInitService.NAME);
  }

  @Test
  public void testFailInStart() throws Throwable {
    assertLaunchOutcome(FailInStartService.EXIT_CODE, "",
        FailInStartService.NAME);
  }

  @Test
  public void testFailInStopIsIgnored() throws Throwable {
    assertRuns(FailingStopInStartService.NAME);
  }

}
