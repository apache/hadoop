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

package org.apache.hadoop.yarn.service;

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
}
