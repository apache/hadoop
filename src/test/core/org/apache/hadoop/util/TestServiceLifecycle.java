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
package org.apache.hadoop.util;

import junit.framework.TestCase;

import java.io.IOException;


/**
 * Test service transitions in a mock service
 */

public class TestServiceLifecycle extends TestCase {
  private MockService service;
  private MockService.LifecycleEventCount counter;

  public TestServiceLifecycle(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    service = new MockService();
    counter = new MockService.LifecycleEventCount();
    service.addStateChangeListener(counter);
  }

  @Override
  protected void tearDown() throws Exception {
    Service.close(service);
    super.tearDown();
  }

  private void start() throws IOException, InterruptedException {
    service.start();
  }

  private void close() throws IOException {
    service.close();
    assertInClosedState();
  }

  protected void assertInState(Service.ServiceState state)
          throws Service.ServiceStateException {
    service.verifyServiceState(state);
  }

  private void assertInLiveState() throws Service.ServiceStateException {
    assertInState(Service.ServiceState.LIVE);
  }

  private void assertInCreatedState() throws Service.ServiceStateException {
    assertInState(Service.ServiceState.CREATED);
  }

  private void assertInFailedState() throws Service.ServiceStateException {
    assertInState(Service.ServiceState.FAILED);
  }

  private void assertInClosedState() throws Service.ServiceStateException {
    assertInState(Service.ServiceState.CLOSED);
  }

  private void assertRunning() {
    assertTrue("Service is not running: " + service, service.isRunning());
  }

  private void assertNotRunning() {
    assertFalse("Service is running: " + service, service.isRunning());
  }

  private void enterState(Service.ServiceState state)
          throws Service.ServiceStateException {
    service.changeState(state);
    assertInState(state);
  }


  private void enterFailedState() throws Service.ServiceStateException {
    enterState(Service.ServiceState.FAILED);
  }

  private void enterClosedState() throws Service.ServiceStateException {
    enterState(Service.ServiceState.CLOSED);
  }

  private void assertStateChangeCount(int expected) {
    assertEquals("Wrong state change count for " + service,
            expected,
            service.getStateChangeCount());
  }

  private void assertStateListenerCount(int expected) {
      assertEquals("Wrong listener state change count for " + service,
              expected,
              counter.getCount());
    }



  private void assertNoStartFromState(Service.ServiceState serviceState)
          throws Throwable {
    enterState(serviceState);
    try {
      service.start();
      failShouldNotGetHere();
    } catch (Service.ServiceStateException expected) {
      //expected
    }
  }

  private void failShouldNotGetHere() {
    fail("expected failure, but service is in " + service.getServiceState());
  }


  /**
   * Walk through the lifecycle and check it changes visible state
   * @throws Throwable if something went wrong
   */
  public void testBasicLifecycle() throws Throwable {
    assertInCreatedState();
    assertNotRunning();
    assertNotRunning();
    start();
    assertInLiveState();
    assertRunning();
    close();
    assertStateChangeCount(3);
    assertNotRunning();
  }

  /**
   * Assert that a state changing operation is idempotent
   * @throws Throwable if something went wrong
   */
  public void testStartIdempotent() throws Throwable {
    //remove the counter, see it works. twice
    service.removeStateChangeListener(counter);
    service.removeStateChangeListener(counter);
    start();
    int count = service.getStateChangeCount();
    //declare that we want to fail in our start operation
    service.setFailOnStart(true);
    //then start. If the innerStart() method is called: failure
    start();
    //check that the state count has not changed either.
    assertStateChangeCount(count);
    assertInLiveState();
  }

  public void testTerminateIdempotent() throws Throwable {
    close();
    int count = service.getStateChangeCount();
    close();
    assertStateChangeCount(count);
  }

  public void testCloseFromCreated() throws Throwable {
    close();
  }

  public void testStaticCloseHandlesNull() throws Throwable {
    Service.close(null);
  }


  public void testStaticCloseOperation() throws Throwable {
    Service.close(service);
    assertInClosedState();
    Service.close(service);
  }

  public void testFailInStart() throws Throwable {
    service.setFailOnStart(true);
    int count = service.getStateChangeCount();
    try {
      start();
      failShouldNotGetHere();
    } catch (MockService.MockServiceException e) {
      assertInFailedState();
      //we should have entered two more states, STARED and FAILED
      assertStateChangeCount(count+2);
      assertStateListenerCount(count+2);
    }
  }


  public void testTerminateFromFailure() throws Throwable {
    enterFailedState();
    //test that we can get from failed to terminated
    close();
  }


  public void testDeploy() throws Throwable {
    Service.startService(service);
    assertInLiveState();
  }

  public void testDeployFailingStart() throws Throwable {
    service.setFailOnStart(true);
    try {
      Service.startService(service);
    } catch (MockService.MockServiceException e) {
      assertInClosedState();
    }
  }

  public void testNoStartFromTerminated() throws Throwable {
    assertNoStartFromState(Service.ServiceState.CLOSED);
  }

  public void testNoStartFromFailed() throws Throwable {
    assertNoStartFromState(Service.ServiceState.CLOSED);
  }

  public void testStartFromLiveIdempotent() throws Throwable {
    enterState(Service.ServiceState.LIVE);
    int count = service.getStateChangeCount();
    start();
    assertStateChangeCount(count);
  }

  public void testFailOnClose() throws Throwable {
    service.setFailOnClose(true);
    try {
      service.close();
      fail("Should have thrown an exception");
    } catch (IOException e) {
      assertInClosedState();
      assertTrue(service.isClosed());
    }
    //the second call should be a no-op; no exceptions get thrown
    service.close();
  }

  public void testFailIdempotent() throws Throwable {
    Exception cause = new Exception("test");
    service.enterFailedState(null);
    int count = service.getStateChangeCount();
    service.enterFailedState(cause);
    assertStateChangeCount(count);
    assertEquals(cause, service.getFailureCause());
  }

  public void testFailFromTerminatedDoesNotChangeState() throws Throwable {
    Service.startService(service);
    service.close();
    assertInClosedState();
    Exception cause = new Exception("test");
    service.enterFailedState(cause);
    assertInClosedState();
    assertEquals(cause,service.getFailureCause());
  }

  public void testFailFromFailedDoesNotChangeCause() throws Throwable {
    Exception cause = new Exception("test");
    service.enterFailedState(cause);
    assertInFailedState();
    service.enterFailedState(new Exception("test2"));
    assertInFailedState();
    assertEquals(cause, service.getFailureCause());
  }

}
