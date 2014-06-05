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
import org.apache.hadoop.service.launcher.testservices.FailingStopInStartService;
import org.junit.Test;

/**
 * Test the behaviour of service stop logic
 */
public class TestServiceStopping extends AbstractServiceLauncherTestBase {

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

}
