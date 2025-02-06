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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.LoggingStateChangeListener;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.service.ServiceStateException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServiceLifecycle extends ServiceAssert {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestServiceLifecycle.class);

  /**
   * Walk the {@link BreakableService} through it's lifecycle, 
   * more to verify that service's counters work than anything else
   * @throws Throwable if necessary
   */
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
   * @throws Throwable if necessary
   */
  @Test
  public void testInitTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    Configuration conf = new Configuration();
    conf.set("test.init","t");
    svc.init(conf);
    svc.init(new Configuration());
    assertStateCount(svc, Service.STATE.INITED, 1);
    assertServiceConfigurationContains(svc, "test.init");
  }

  /**
   * Call start twice
   * @throws Throwable if necessary
   */
  @Test
  public void testStartTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    svc.init(new Configuration());
    svc.start();
    svc.start();
    assertStateCount(svc, Service.STATE.STARTED, 1);
  }


  /**
   * Verify that when a service is stopped more than once, no exception
   * is thrown.
   * @throws Throwable if necessary
   */
  @Test
  public void testStopTwice() throws Throwable {
    BreakableService svc = new BreakableService();
    svc.init(new Configuration());
    svc.start();
    svc.stop();
    assertStateCount(svc, Service.STATE.STOPPED, 1);
    svc.stop();
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }


  /**
   * Show that if the service failed during an init
   * operation, it stays in the created state, even after stopping it
   * @throws Throwable if necessary
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
    assertServiceStateStopped(svc);
    assertStateCount(svc, Service.STATE.INITED, 1);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
    //now try to stop
    svc.stop();
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }


  /**
   * Show that if the service failed during an init
   * operation, it stays in the created state, even after stopping it
   * @throws Throwable if necessary
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
    assertServiceStateStopped(svc);
  }

  /**
   * verify that when a service fails during its stop operation,
   * its state does not change.
   * @throws Throwable if necessary
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
  }

  /**
   * verify that when a service that is not started is stopped, the
   * service enters the stopped state
   * @throws Throwable on a failure
   */
  @Test
  public void testStopUnstarted() throws Throwable {
    BreakableService svc = new BreakableService();
    svc.stop();
    assertServiceStateStopped(svc);
    assertStateCount(svc, Service.STATE.INITED, 0);
    assertStateCount(svc, Service.STATE.STOPPED, 1);
  }

  /**
   * Show that if the service failed during an init
   * operation, stop was called.
   */

  @Test
  public void testStopFailingInitAndStop() throws Throwable {
    BreakableService svc = new BreakableService(true, false, true);
    svc.registerServiceListener(new LoggingStateChangeListener());
    try {
      svc.init(new Configuration());
      fail("Expected a failure, got " + svc);
    } catch (BreakableService.BrokenLifecycleEvent e) {
      assertEquals(Service.STATE.INITED, e.state);
    }
    //the service state is stopped
    assertServiceStateStopped(svc);
    assertEquals(Service.STATE.INITED, svc.getFailureState());

    Throwable failureCause = svc.getFailureCause();
    assertNotNull("Null failure cause in " + svc, failureCause);
    BreakableService.BrokenLifecycleEvent cause =
      (BreakableService.BrokenLifecycleEvent) failureCause;
    assertNotNull("null state in " + cause + " raised by " + svc, cause.state);
    assertEquals(Service.STATE.INITED, cause.state);
  }

  @Test
  public void testInitNullConf() throws Throwable {
    BreakableService svc = new BreakableService(false, false, false);
    try {
      svc.init(null);
      LOG.warn("Null Configurations are permitted ");
    } catch (ServiceStateException e) {
      //expected
    }
  }

  @Test
  public void testServiceNotifications() throws Throwable {
    BreakableService svc = new BreakableService(false, false, false);
    BreakableStateChangeListener listener = new BreakableStateChangeListener();
    svc.registerServiceListener(listener);
    svc.init(new Configuration());
    assertEventCount(listener, 1);
    svc.start();
    assertEventCount(listener, 2);
    svc.stop();
    assertEventCount(listener, 3);
    svc.stop();
    assertEventCount(listener, 3);
  }

  /**
   * Test that when a service listener is unregistered, it stops being invoked
   * @throws Throwable on a failure
   */
  @Test
  public void testServiceNotificationsStopOnceUnregistered() throws Throwable {
    BreakableService svc = new BreakableService(false, false, false);
    BreakableStateChangeListener listener = new BreakableStateChangeListener();
    svc.registerServiceListener(listener);
    svc.init(new Configuration());
    assertEventCount(listener, 1);
    svc.unregisterServiceListener(listener);
    svc.start();
    assertEventCount(listener, 1);
    svc.stop();
    assertEventCount(listener, 1);
    svc.stop();
  }

  /**
   * This test uses a service listener that unregisters itself during the callbacks.
   * This a test that verifies the concurrency logic on the listener management
   * code, that it doesn't throw any immutable state change exceptions
   * if you change list membership during the notifications.
   * The standard <code>AbstractService</code> implementation copies the list
   * to an array in a <code>synchronized</code> block then iterates through
   * the copy precisely to prevent this problem.
   * @throws Throwable on a failure
   */
  @Test
  public void testServiceNotificationsUnregisterDuringCallback() throws Throwable {
    BreakableService svc = new BreakableService(false, false, false);
    BreakableStateChangeListener listener =
      new SelfUnregisteringBreakableStateChangeListener();
    BreakableStateChangeListener l2 =
      new BreakableStateChangeListener();
    svc.registerServiceListener(listener);
    svc.registerServiceListener(l2);
    svc.init(new Configuration());
    assertEventCount(listener, 1);
    assertEventCount(l2, 1);
    svc.unregisterServiceListener(listener);
    svc.start();
    assertEventCount(listener, 1);
    assertEventCount(l2, 2);
    svc.stop();
    assertEventCount(listener, 1);
    svc.stop();
  }

  private static class SelfUnregisteringBreakableStateChangeListener
    extends BreakableStateChangeListener {

    @Override
    public synchronized void stateChanged(Service service) {
      super.stateChanged(service);
      service.unregisterServiceListener(this);
    }
  }

  private void assertEventCount(BreakableStateChangeListener listener,
                                int expected) {
    assertEquals(listener.toString(), expected, listener.getEventCount());
  }

  @Test
  public void testServiceFailingNotifications() throws Throwable {
    BreakableService svc = new BreakableService(false, false, false);
    BreakableStateChangeListener listener = new BreakableStateChangeListener();
    listener.setFailingState(Service.STATE.STARTED);
    svc.registerServiceListener(listener);
    svc.init(new Configuration());
    assertEventCount(listener, 1);
    //start this; the listener failed but this won't show
    svc.start();
    //counter went up
    assertEventCount(listener, 2);
    assertEquals(1, listener.getFailureCount());
    //stop the service -this doesn't fail
    svc.stop();
    assertEventCount(listener, 3);
    assertEquals(1, listener.getFailureCount());
    svc.stop();
  }

  /**
   * This test verifies that you can block waiting for something to happen
   * and use notifications to manage it
   * @throws Throwable on a failure
   */
  @Test
  public void testListenerWithNotifications() throws Throwable {
    //this tests that a listener can get notified when a service is stopped
    AsyncSelfTerminatingService service = new AsyncSelfTerminatingService(2000);
    NotifyingListener listener = new NotifyingListener();
    service.registerServiceListener(listener);
    service.init(new Configuration());
    service.start();
    assertServiceInState(service, Service.STATE.STARTED);
    long start = System.currentTimeMillis();
    synchronized (listener) {
      listener.wait(20000);
    }
    long duration = System.currentTimeMillis() - start;
    assertEquals(Service.STATE.STOPPED, listener.notifyingState);
    assertServiceInState(service, Service.STATE.STOPPED);
    assertTrue("Duration of " + duration + " too long", duration < 10000);
  }

  @Test
  public void testSelfTerminatingService() throws Throwable {
    SelfTerminatingService service = new SelfTerminatingService();
    BreakableStateChangeListener listener = new BreakableStateChangeListener();
    service.registerServiceListener(listener);
    service.init(new Configuration());
    assertEventCount(listener, 1);
    //start the service
    service.start();
    //and expect an event count of exactly two
    assertEventCount(listener, 2);
  }

  @Test
  public void testStartInInitService() throws Throwable {
    Service service = new StartInInitService();
    BreakableStateChangeListener listener = new BreakableStateChangeListener();
    service.registerServiceListener(listener);
    service.init(new Configuration());
    assertServiceInState(service, Service.STATE.STARTED);
    assertEventCount(listener, 1);
  }

  @Test
  public void testStopInInitService() throws Throwable {
    Service service = new StopInInitService();
    BreakableStateChangeListener listener = new BreakableStateChangeListener();
    service.registerServiceListener(listener);
    service.init(new Configuration());
    assertServiceInState(service, Service.STATE.STOPPED);
    assertEventCount(listener, 1);
  }

  /**
   * Listener that wakes up all threads waiting on it
   */
  private static class NotifyingListener implements ServiceStateChangeListener {
    public Service.STATE notifyingState = Service.STATE.NOTINITED;

    public synchronized void stateChanged(Service service) {
      notifyingState = service.getServiceState();
      this.notifyAll();
    }
  }

  /**
   * Service that terminates itself after starting and sleeping for a while
   */
  private static class AsyncSelfTerminatingService extends AbstractService
                                               implements Runnable {
    final int timeout;
    private AsyncSelfTerminatingService(int timeout) {
      super("AsyncSelfTerminatingService");
      this.timeout = timeout;
    }

    @Override
    protected void serviceStart() throws Exception {
      new Thread(this).start();
      super.serviceStart();
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeout);
      } catch (InterruptedException ignored) {

      }
      this.stop();
    }
  }

  /**
   * Service that terminates itself in startup
   */
  private static class SelfTerminatingService extends AbstractService {
    private SelfTerminatingService() {
      super("SelfTerminatingService");
    }

    @Override
    protected void serviceStart() throws Exception {
      //start
      super.serviceStart();
      //then stop
      stop();
    }
  }

  /**
   * Service that starts itself in init
   */
  private static class StartInInitService extends AbstractService {
    private StartInInitService() {
      super("StartInInitService");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      super.serviceInit(conf);
      start();
    }
  }

  /**
   * Service that starts itself in init
   */
  private static class StopInInitService extends AbstractService {
    private StopInInitService() {
      super("StopInInitService");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      super.serviceInit(conf);
      stop();
    }
  }

}
