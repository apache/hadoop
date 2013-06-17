/**
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

package org.apache.hadoop.service;

import org.apache.hadoop.service.Service;
import org.junit.Assert;

/**
 * A set of assertions about the state of any service
 */
public class ServiceAssert extends Assert {

  public static void assertServiceStateCreated(Service service) {
    assertServiceInState(service, Service.STATE.NOTINITED);
  }

  public static void assertServiceStateInited(Service service) {
    assertServiceInState(service, Service.STATE.INITED);
  }

  public static void assertServiceStateStarted(Service service) {
    assertServiceInState(service, Service.STATE.STARTED);
  }

  public static void assertServiceStateStopped(Service service) {
    assertServiceInState(service, Service.STATE.STOPPED);
  }

  public static void assertServiceInState(Service service, Service.STATE state) {
    assertNotNull("Null service", service);
    assertEquals("Service in wrong state: " + service, state,
                 service.getServiceState());
  }

  /**
   * Assert that the breakable service has entered a state exactly the number
   * of time asserted.
   * @param service service -if null an assertion is raised.
   * @param state state to check.
   * @param expected expected count.
   */
  public static void assertStateCount(BreakableService service,
                        Service.STATE state,
                        int expected) {
    assertNotNull("Null service", service);
    int actual = service.getCount(state);
    if (expected != actual) {
      fail("Expected entry count for state [" + state +"] of " + service
               + " to be " + expected + " but was " + actual);
    }
  }

  /**
   * Assert that a service configuration contains a specific key; the value
   * is ignored.
   * @param service service to check
   * @param key key to look for
   */
  public static void assertServiceConfigurationContains(Service service,
                                                        String key) {
    assertNotNull("No option "+ key + " in service configuration",
                  service.getConfig().get(key));
  }
}
