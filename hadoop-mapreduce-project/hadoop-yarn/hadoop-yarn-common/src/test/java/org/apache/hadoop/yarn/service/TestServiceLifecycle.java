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

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestServiceLifecycle extends ServiceAssert {

  void assertStateCount(BreakableService service,
                        Service.STATE state,
                        int expected) {
    int actual = service.getCount(state);
    if (expected != actual) {
      fail("Expected entry count for state [" + state +"] of " + service
               + " to be " + expected + " but was " + actual);
    }
  }


  @Test
  public void testWalkthrough() throws Throwable {

    BreakableService svc = new BreakableService();
    assertServiceStateCreated(svc);
    assertStateCount(svc, Service.STATE.NOTINITED, 1);
    assertStateCount(svc, Service.STATE.INITED, 0);
    assertStateCount(svc, Service.STATE.STARTED, 0);
    assertStateCount(svc, Service.STATE.STOPPED, 0);
    svc.init(new Configuration());
    assertServiceStateInited(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    svc.start();
    assertServiceStateStarted(svc);
    assertStateCount(svc, Service.STATE.STARTED, 1);
    svc.stop();
    assertServiceStateStopped(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }

  /**
   * call init twice
   * @throws Throwable
   */
  @Test
  public void testInitTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    svc.init(new Configuration());
    try {
      svc.init(new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (IllegalStateException e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.INITED, 2);
  }

  /**
   * call start twice
   * @throws Throwable
   */
  @Test
  public void testStartTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    svc.init(new Configuration());
    svc.start();
    try {
      svc.start();
      fail("Expected a failure, got " + svc);
    } catch (IllegalStateException e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.STARTED, 2);
  }


  /**
   * verify that when a service is stopped more than once, no exception
   * is thrown, and the counter is incremented
   * this is because the state change operations happen after the counter in
   * the subclass is incremented, even though stop is meant to be a no-op
   * @throws Throwable
   */
  @Test
  public void testStopTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    svc.init(new Configuration());
    svc.start();
    svc.stop();
    assertStateCount(svc, Service.STATE.STOPPED, 1);
    svc.stop();
    assertStateCount(svc, Service.STATE.STOPPED, 2);
  }


  /**
   * Show that if the service failed during an init
   * operation, it stays in the created state, even after stopping it
   * @throws Throwable
   */

  @Test
  public void testStopFailedInit() throws Throwable {
    BreakableService svc = new BreakableService(true, false, false);
    assertServiceStateCreated(svc);
    try {
      svc.init(new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    //the service state wasn't passed
    assertServiceStateCreated(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    //now try to stop
    svc.stop();
    //even after the stop operation, we haven't entered the state
    assertServiceStateCreated(svc);
  }


  /**
   * Show that if the service failed during an init
   * operation, it stays in the created state, even after stopping it
   * @throws Throwable
   */

  @Test
  public void testStopFailedStart() throws Throwable {
    BreakableService svc = new BreakableService(false, true, false);
    svc.init(new Configuration());
    assertServiceStateInited(svc);
    try {
      svc.start();
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    //the service state wasn't passed
    assertServiceStateInited(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    //now try to stop
    svc.stop();
    //even after the stop operation, we haven't entered the state
    assertServiceStateInited(svc);
  }

  /**
   * verify that when a service is stopped more than once, no exception
   * is thrown, and the counter is incremented
   * this is because the state change operations happen after the counter in
   * the subclass is incremented, even though stop is meant to be a no-op
   * @throws Throwable
   */
  @Test
  public void testFailingStop() throws Throwable {
    BreakableService svc = new BreakableService(false, false, true);
    svc.init(new Configuration());
    svc.start();
    try {
      svc.stop();
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.STOPPED, 1);
    //now try again, and expect it to happen again
    try {
      svc.stop();
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.STOPPED, 2);
  }

}
