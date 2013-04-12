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

/**
 * These tests verify that the {@link ServiceOperations} methods
 * do a best-effort attempt to make  the service state change operations 
 * idempotent. That is still best effort -there is no thread safety, and
 * a failure during a state change does not prevent the operation
 * being called again.
 */
public class TestServiceOperations extends ServiceAssert {

  @Test
  public void testWalkthrough() throws Throwable {
    BreakableService svc = new BreakableService();
    assertServiceStateCreated(svc);
    Configuration conf = new Configuration();
    conf.set("test.walkthrough","t");
    ServiceOperations.init(svc, conf);
    assertServiceStateInited(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    //check the configuration made it all the way through.
    assertServiceConfigurationContains(svc, "test.walkthrough");
    ServiceOperations.start(svc);
    assertServiceStateStarted(svc);
    assertStateCount(svc, Service.STATE.STARTED, 1);
    ServiceOperations.stop(svc);
    assertServiceStateStopped(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }

  /**
   * Call init twice -expect a failure, and expect the count
   * of initialization attempts to still be 1: the state
   * check was made before the subclass method was called.
   * @throws Throwable if need be
   */
  @Test
  public void testInitTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    Configuration conf = new Configuration();
    conf.set("test.init", "t");
    ServiceOperations.init(svc, conf);
    try {
      ServiceOperations.init(svc, new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (IllegalStateException e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.INITED, 1);
    assertServiceConfigurationContains(svc, "test.init");
  }

  /**
   * call start twice; expect failures and the start invoke count to 
   * be exactly 1.
   * @throws Throwable if necessary
   */
  @Test
  public void testStartTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    ServiceOperations.init(svc, new Configuration());
    ServiceOperations.start(svc);
    try {
      ServiceOperations.start(svc);
      fail("Expected a failure, got " + svc);
    } catch (IllegalStateException e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.STARTED, 1);
  }

  /**
   * Test that the deploy operation pushes a service into its started state
   * @throws Throwable on any failure.
   */
  @Test
  public void testDeploy() throws Throwable {
    BreakableService svc = new BreakableService();
    assertServiceStateCreated(svc);
    ServiceOperations.deploy(svc, new Configuration());
    assertServiceStateStarted(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    assertStateCount(svc, Service.STATE.STARTED, 1);
    ServiceOperations.stop(svc);
    assertServiceStateStopped(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }

  /**
   * Demonstrate that the deploy operation fails when invoked twice,
   * but the service method call counts are unchanged after the second call.
   * @throws Throwable on any failure.
   */
  @Test
  public void testDeployNotIdempotent() throws Throwable {
    BreakableService svc = new BreakableService();
    assertServiceStateCreated(svc);
    ServiceOperations.deploy(svc, new Configuration());
    try {
      ServiceOperations.deploy(svc, new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (IllegalStateException e) {
      //expected
    }
    //verify state and values are unchanged
    assertServiceStateStarted(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    assertStateCount(svc, Service.STATE.STARTED, 1);
    ServiceOperations.stop(svc);
  }

  /**
   * Test that the deploy operation can fail part way through, in which
   * case the service is in the state that it was in before the failing
   * state method was called.
   * @throws Throwable on any failure.
   */
  @Test
  public void testDeployNotAtomic() throws Throwable {
    //this instance is set to fail in the start() call.
    BreakableService svc = new BreakableService(false, true, false);
    try {
      ServiceOperations.deploy(svc, new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent expected) {
      //expected
    }
    //now in the inited state
    assertServiceStateInited(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    assertStateCount(svc, Service.STATE.STARTED, 1);
    //try again -expect a failure as the service is now inited.
    try {
      ServiceOperations.deploy(svc, new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (IllegalStateException e) {
      //expected
    }
  }
  
  /**
   * verify that when a service is stopped more than once, no exception
   * is thrown, and the counter is not incremented
   * this is because the state change operations happen after the counter in
   * the subclass is incremented, even though stop is meant to be a no-op
   * @throws Throwable on a failure
   */
  @Test
  public void testStopTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    ServiceOperations.deploy(svc, new Configuration());
    ServiceOperations.stop(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
    assertServiceStateStopped(svc);
    ServiceOperations.stop(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }

  /**
   * verify that when a service that is not started is stopped, it's counter
   * is not incremented -the stop() method was not invoked.
   * @throws Throwable on a failure
   */
  @Test
  public void testStopInit() throws Throwable {
    BreakableService svc = new BreakableService();
    ServiceOperations.stop(svc);
    assertServiceStateCreated(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 0);
    ServiceOperations.stop(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 0);
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
      ServiceOperations.init(svc, new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    //the service state wasn't passed
    assertServiceStateCreated(svc);
    //the init state got invoked once
    assertStateCount(svc, Service.STATE.INITED, 1);
    //now try to stop
    ServiceOperations.stop(svc);
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
    ServiceOperations.init(svc, new Configuration());
    assertServiceStateInited(svc);
    try {
      ServiceOperations.start(svc);
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    //the service state wasn't passed
    assertServiceStateInited(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    //now try to stop
    ServiceOperations.stop(svc);
    //even after the stop operation, we haven't entered the state
    assertServiceStateInited(svc);
  }

  /**
   * verify that when a service is stopped more than once, no exception
   * is thrown, and the counter is incremented
   * this is because the state change operations happen after the counter in
   * the subclass is incremented, even though stop is meant to be a no-op.
   *
   * The {@link ServiceOperations#stop(Service)} operation does not prevent
   * this from happening
   * @throws Throwable
   */
  @Test
  public void testFailingStop() throws Throwable {
    BreakableService svc = new BreakableService(false, false, true);
    ServiceOperations.deploy(svc, new Configuration());
    try {
      ServiceOperations.stop(svc);
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      //expected
    }
    assertStateCount(svc, Service.STATE.STOPPED, 1);
    //now try to stop, this time doing it quietly
    Exception exception = ServiceOperations.stopQuietly(svc);
    assertTrue("Wrong exception type : " + exception,
        exception instanceof BreakableService.BrokenLifecycleEvent);
    assertStateCount(svc, Service.STATE.STOPPED, 2);
  }


  /**
   * verify that when a service that is not started is stopped, its counter
   * of stop calls is still incremented-and the service remains in its
   * original state..
   * @throws Throwable on a failure
   */
  @Test
  public void testStopUnstarted() throws Throwable {
    BreakableService svc = new BreakableService();

    //invocation in NOTINITED state should be no-op
    ServiceOperations.stop(svc);
    assertServiceStateCreated(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 0);

    //stop failed, now it can be initialised
    ServiceOperations.init(svc, new Configuration());

    //again, no-op
    ServiceOperations.stop(svc);
    assertServiceStateInited(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 0);

    //once started, the service can be stopped reliably
    ServiceOperations.start(svc);
    ServiceOperations.stop(svc);
    assertServiceStateStopped(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);

    //now stop one more time
    ServiceOperations.stop(svc);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }
}
