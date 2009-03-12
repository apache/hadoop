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
import java.util.List;


/**vc
 * Test service transitions in a mock service
 */

public class TestServiceLifecycle extends TestCase {
  private MockService service;

  public TestServiceLifecycle(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    service = new MockService();
  }

  @Override
  protected void tearDown() throws Exception {
    Service.close(service);
    super.tearDown();
  }

  private void ping() throws IOException {
    service.ping();
  }

  private void start() throws IOException {
    service.start();
  }

  private void close() throws IOException {
    service.close();
    assertInTerminatedState();
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

  private void assertInTerminatedState() throws Service.ServiceStateException {
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

  private void enterTerminatedState() throws Service.ServiceStateException {
    enterState(Service.ServiceState.CLOSED);
  }

  private void assertStateChangeCount(int expected) {
    assertEquals("Wrong state change count for " + service,
            expected,
            service.getStateChangeCount());
  }

  private void assertPingCount(int expected) {
    assertEquals("Wrong pingchange count for " + service,
            expected,
            service.getPingCount());
  }

  private void assertNoStartFromState(Service.ServiceState serviceState)
          throws IOException {
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
   * Test that the ping operation returns a mock exception
   * @return the service status
   * @throws IOException IO problems
   */
  private Service.ServiceStatus assertPingContainsMockException()
          throws IOException {
    Service.ServiceStatus serviceStatus = service.ping();
    List<Throwable> thrown = serviceStatus.getThrowables();
    assertFalse("No nested exceptions in service status", thrown.isEmpty());
    Throwable throwable = thrown.get(0);
    assertTrue(
            "Nested exception is not a MockServiceException : "+throwable,
            throwable instanceof MockService.MockServiceException);
    return serviceStatus;
  }

  /**
   * Walk through the lifecycle and check it changes visible state
   */
  public void testBasicLifecycle() throws Throwable {
    assertInCreatedState();
    assertNotRunning();
    assertNotRunning();
    start();
    assertInLiveState();
    assertRunning();
    ping();
    ping();
    assertPingCount(2);
    close();
    assertStateChangeCount(3);
    assertNotRunning();
  }

  /**
   * Assert that a state changing operation is idempotent
   * @throws Throwable if something went wrong
   */
  public void testStartIdempotent() throws Throwable {
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
    assertInTerminatedState();
    Service.close(service);
  }

  public void testFailInStart() throws Throwable {
    service.setFailOnStart(true);
    try {
      start();
      failShouldNotGetHere();
    } catch (MockService.MockServiceException e) {
      assertInFailedState();
    }
  }

  public void testPingInFailedReturnsException() throws Throwable {
    service.setFailOnStart(true);
    try {
      start();
      failShouldNotGetHere();
    } catch (MockService.MockServiceException e) {
      assertInFailedState();
      //and test that the ping works out
      Service.ServiceStatus serviceStatus = assertPingContainsMockException();
      assertEquals(Service.ServiceState.FAILED, serviceStatus.getState());
    }
  }

  public void testTerminateFromFailure() throws Throwable {
    enterFailedState();
    //test that we can get from failed to terminated
    close();
  }

  public void testFailInPing() throws Throwable {
    service.setFailOnPing(true);
    start();
    Service.ServiceStatus serviceStatus = service.ping();
    assertEquals(Service.ServiceState.FAILED, serviceStatus.getState());
    assertPingCount(1);
    List<Throwable> thrown = serviceStatus.getThrowables();
    assertEquals(1, thrown.size());
    Throwable throwable = thrown.get(0);
    assertTrue(throwable instanceof MockService.MockServiceException);
  }

  public void testPingInCreated() throws Throwable {
    service.setFailOnPing(true);
    ping();
    assertPingCount(0);
  }


  /**
   * Test that when in a failed state, you can't ping the service
   *
   * @throws Throwable if needed
   */
  public void testPingInFailedStateIsNoop() throws Throwable {
    enterFailedState();
    assertInFailedState();
    Service.ServiceStatus serviceStatus = service.ping();
    assertEquals(Service.ServiceState.FAILED, serviceStatus.getState());
    assertPingCount(0);
  }

  /**
   * Test that when in a terminated state, you can't ping the service
   *
   * @throws Throwable if needed
   */
  public void testPingInTerminatedStateIsNoop() throws Throwable {
    enterTerminatedState();
    assertInTerminatedState();
    Service.ServiceStatus serviceStatus = service.ping();
    assertEquals(Service.ServiceState.CLOSED, serviceStatus.getState());
    assertPingCount(0);
  }

  public void testDeploy() throws Throwable {
    Service.deploy(service);
    assertInLiveState();
  }

  public void testDeployFailingStart() throws Throwable {
    service.setFailOnStart(true);
    try {
      Service.deploy(service);
    } catch (MockService.MockServiceException e) {
      assertInTerminatedState();
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
      assertInTerminatedState();
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
    Service.deploy(service);
    service.close();
    assertInTerminatedState();
    Exception cause = new Exception("test");
    service.enterFailedState(cause);
    assertInTerminatedState();
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
