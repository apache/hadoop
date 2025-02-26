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
import org.apache.hadoop.service.Service.STATE;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestCompositeService {

  private static final int NUM_OF_SERVICES = 5;

  private static final int FAILED_SERVICE_SEQ_NUMBER = 2;

  private static final Logger LOG  =
      LoggerFactory.getLogger(TestCompositeService.class);

  /**
   * flag to state policy of CompositeService, and hence
   * what to look for after trying to stop a service from another state
   * (e.g inited)
   */
  private static final boolean STOP_ONLY_STARTED_SERVICES =
    CompositeServiceImpl.isPolicyToStopOnlyStartedServices();

  @BeforeEach
  public void setup() {
    CompositeServiceImpl.resetCounter();
  }

  @Test
  public void testCallSequence() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
        new CompositeServiceImpl[0]);

    assertEquals(NUM_OF_SERVICES, services.length,
        "Number of registered services ");

    Configuration conf = new Configuration();
    // Initialise the composite service
    serviceManager.init(conf);

    //verify they were all inited
    assertInState(STATE.INITED, services);

    // Verify the init() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals(i, services[i].getCallSequenceNumber(),
          "For " + services[i] +
          " service, init() call sequence number should have been ");
    }

    // Reset the call sequence numbers
    resetServices(services);

    serviceManager.start();
    //verify they were all started
    assertInState(STATE.STARTED, services);

    // Verify the start() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals(i,
          services[i].getCallSequenceNumber(), "For " + services[i] +
          " service, start() call sequence number should have been ");
    }
    resetServices(services);


    serviceManager.stop();
    //verify they were all stopped
    assertInState(STATE.STOPPED, services);

    // Verify the stop() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals(((NUM_OF_SERVICES - 1) - i), services[i].getCallSequenceNumber(),
          "For " + services[i] +
          " service, stop() call sequence number should have been ");
    }

    // Try to stop again. This should be a no-op.
    serviceManager.stop();
    // Verify that stop() call sequence numbers for every service don't change.
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals(((NUM_OF_SERVICES - 1) - i), services[i].getCallSequenceNumber(),
          "For " + services[i] +
          " service, stop() call sequence number should have been ");
    }
  }

  private void resetServices(CompositeServiceImpl[] services) {
    // Reset the call sequence numbers
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      services[i].reset();
    }
  }

  @Test
  public void testServiceStartup() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      if (i == FAILED_SERVICE_SEQ_NUMBER) {
        service.setThrowExceptionOnStart(true);
      }
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
        new CompositeServiceImpl[0]);

    Configuration conf = new Configuration();

    // Initialise the composite service
    serviceManager.init(conf);

    // Start the composite service
    try {
      serviceManager.start();
      fail("Exception should have been thrown due to startup failure of last service");
    } catch (ServiceTestRuntimeException e) {
      for (int i = 0; i < NUM_OF_SERVICES - 1; i++) {
        if (i >= FAILED_SERVICE_SEQ_NUMBER && STOP_ONLY_STARTED_SERVICES) {
          // Failed service state should be INITED
          assertEquals(STATE.INITED, services[NUM_OF_SERVICES - 1].getServiceState(),
              "Service state should have been ");
        } else {
          assertEquals(STATE.STOPPED, services[i].getServiceState(),
              "Service state should have been ");
        }
      }

    }
  }

  @Test
  public void testServiceStop() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      if (i == FAILED_SERVICE_SEQ_NUMBER) {
        service.setThrowExceptionOnStop(true);
      }
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
        new CompositeServiceImpl[0]);

    Configuration conf = new Configuration();

    // Initialise the composite service
    serviceManager.init(conf);

    serviceManager.start();

    // Stop the composite service
    try {
      serviceManager.stop();
    } catch (ServiceTestRuntimeException e) {
    }
    assertInState(STATE.STOPPED, services);
  }

  /**
   * Assert that all services are in the same expected state
   * @param expected expected state value
   * @param services services to examine
   */
  private void assertInState(STATE expected, CompositeServiceImpl[] services) {
    assertInState(expected, services,0, services.length);
  }

  /**
   * Assert that all services are in the same expected state
   * @param expected expected state value
   * @param services services to examine
   * @param start start offset
   * @param finish finish offset: the count stops before this number
   */
  private void assertInState(STATE expected,
                             CompositeServiceImpl[] services,
                             int start, int finish) {
    for (int i = start; i < finish; i++) {
      Service service = services[i];
      assertInState(expected, service);
    }
  }

  private void assertInState(STATE expected, Service service) {
    assertEquals(expected,  service.getServiceState(),
        "Service state should have been " + expected + " in " + service);
  }

  /**
   * Shut down from not-inited: expect nothing to have happened
   */
  @Test
  public void testServiceStopFromNotInited() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
      new CompositeServiceImpl[0]);
    serviceManager.stop();
    assertInState(STATE.NOTINITED, services);
  }

  /**
   * Shut down from inited
   */
  @Test
  public void testServiceStopFromInited() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
      new CompositeServiceImpl[0]);
    serviceManager.init(new Configuration());
    serviceManager.stop();
    if (STOP_ONLY_STARTED_SERVICES) {
      //this policy => no services were stopped
      assertInState(STATE.INITED, services);
    } else {
      assertInState(STATE.STOPPED, services);
    }
  }

  /**
   * Use a null configuration & expect a failure
   * @throws Throwable
   */
  @Test
  public void testInitNullConf() throws Throwable {
    ServiceManager serviceManager = new ServiceManager("testInitNullConf");

    CompositeServiceImpl service = new CompositeServiceImpl(0);
    serviceManager.addTestService(service);
    try {
      serviceManager.init(null);
      LOG.warn("Null Configurations are permitted " + serviceManager);
    } catch (ServiceStateException e) {
      //expected
    }
  }

  /**
   * Walk the service through their lifecycle without any children;
   * verify that it all works.
   */
  @Test
  public void testServiceLifecycleNoChildren() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");
    serviceManager.init(new Configuration());
    serviceManager.start();
    serviceManager.stop();
  }

  @Test
  public void testAddServiceInInit() throws Throwable {
    BreakableService child = new BreakableService();
    assertInState(STATE.NOTINITED, child);
    CompositeServiceAddingAChild composite =
      new CompositeServiceAddingAChild(child);
    composite.init(new Configuration());
    assertInState(STATE.INITED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddIfService() {
    CompositeService testService = new CompositeService("TestService") {
      Service service;
      @Override
      public void serviceInit(Configuration conf) {
        Integer notAService = new Integer(0);
        assertFalse(addIfService(notAService), "Added an integer as a service");

        service = new AbstractService("Service") {};
        assertTrue(addIfService(service), "Unable to add a service");
      }
    };

    testService.init(new Configuration());
    assertEquals(1, testService.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  public void testRemoveService() {
    CompositeService testService = new CompositeService("TestService") {
      @Override
      public void serviceInit(Configuration conf) {
        Integer notAService = new Integer(0);
        assertFalse(addIfService(notAService), "Added an integer as a service");

        Service service1 = new AbstractService("Service1") {};
        addIfService(service1);

        Service service2 = new AbstractService("Service2") {};
        addIfService(service2);

        Service service3 = new AbstractService("Service3") {};
        addIfService(service3);

        removeService(service1);
      }
    };

    testService.init(new Configuration());
    assertEquals(2, testService.getServices().size(),
        "Incorrect number of services");
  }

  //
  // Tests for adding child service to parent
  //

  @Test
  @Timeout(value = 10)
  public void testAddUninitedChildBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    AddSiblingService.addChildToService(parent, child);
    parent.init(new Configuration());
    assertInState(STATE.INITED, child);
    parent.start();
    assertInState(STATE.STARTED, child);
    parent.stop();
    assertInState(STATE.STOPPED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddUninitedChildInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    parent.init(new Configuration());
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.NOTINITED, child);
    try {
      parent.start();
      fail("Expected an exception, got " + parent);
    } catch (ServiceStateException e) {
      //expected
    }
    assertInState(STATE.NOTINITED, child);
    parent.stop();
    assertInState(STATE.NOTINITED, child);
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddUninitedChildInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    parent.init(new Configuration());
    parent.start();
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.NOTINITED, child);
    parent.stop();
    assertInState(STATE.NOTINITED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddUninitedChildInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    parent.init(new Configuration());
    parent.start();
    parent.stop();
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.NOTINITED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedChildBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    AddSiblingService.addChildToService(parent, child);
    parent.init(new Configuration());
    assertInState(STATE.INITED, child);
    parent.start();
    assertInState(STATE.STARTED, child);
    parent.stop();
    assertInState(STATE.STOPPED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedChildInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    parent.init(new Configuration());
    AddSiblingService.addChildToService(parent, child);
    parent.start();
    assertInState(STATE.STARTED, child);
    parent.stop();
    assertInState(STATE.STOPPED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedChildInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    parent.init(new Configuration());
    parent.start();
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.INITED, child);
    parent.stop();
    assertInState(STATE.STOPPED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedChildInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    parent.init(new Configuration());
    parent.start();
    parent.stop();
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.INITED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedChildBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    AddSiblingService.addChildToService(parent, child);
    try {
      parent.init(new Configuration());
      fail("Expected an exception, got " + parent);
    } catch (ServiceStateException e) {
      //expected
    }
    parent.stop();
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedChildInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    parent.init(new Configuration());
    AddSiblingService.addChildToService(parent, child);
    parent.start();
    assertInState(STATE.STARTED, child);
    parent.stop();
    assertInState(STATE.STOPPED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedChildInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    parent.init(new Configuration());
    parent.start();
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.STARTED, child);
    parent.stop();
    assertInState(STATE.STOPPED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedChildInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    parent.init(new Configuration());
    parent.start();
    parent.stop();
    AddSiblingService.addChildToService(parent, child);
    assertInState(STATE.STARTED, child);
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedChildBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    child.stop();
    AddSiblingService.addChildToService(parent, child);
    try {
      parent.init(new Configuration());
      fail("Expected an exception, got " + parent);
    } catch (ServiceStateException e) {
      //expected
    }
    parent.stop();
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedChildInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    child.stop();
    parent.init(new Configuration());
    AddSiblingService.addChildToService(parent, child);
    try {
      parent.start();
      fail("Expected an exception, got " + parent);
    } catch (ServiceStateException e) {
      //expected
    }
    assertInState(STATE.STOPPED, child);
    parent.stop();
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedChildInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    child.stop();
    parent.init(new Configuration());
    parent.start();
    AddSiblingService.addChildToService(parent, child);
    parent.stop();
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedChildInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService child = new BreakableService();
    child.init(new Configuration());
    child.start();
    child.stop();
    parent.init(new Configuration());
    parent.start();
    parent.stop();
    AddSiblingService.addChildToService(parent, child);
  }

  //
  // Tests for adding sibling service to parent
  //

  @Test
  @Timeout(value = 10)
  public void testAddUninitedSiblingBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.NOTINITED));
    parent.init(new Configuration());
    assertInState(STATE.NOTINITED, sibling);
    parent.start();
    assertInState(STATE.NOTINITED, sibling);
    parent.stop();
    assertInState(STATE.NOTINITED, sibling);
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddUninitedSiblingInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.INITED));
    parent.init(new Configuration());
    try {
      parent.start();
      fail("Expected an exception, got " + parent);
    } catch (ServiceStateException e) {
      //expected
    }
    parent.stop();
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddUninitedSiblingInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STARTED));
    parent.init(new Configuration());
    assertInState(STATE.NOTINITED, sibling);
    parent.start();
    assertInState(STATE.NOTINITED, sibling);
    parent.stop();
    assertInState(STATE.NOTINITED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddUninitedSiblingInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STOPPED));
    parent.init(new Configuration());
    assertInState(STATE.NOTINITED, sibling);
    parent.start();
    assertInState(STATE.NOTINITED, sibling);
    parent.stop();
    assertInState(STATE.NOTINITED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedSiblingBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.NOTINITED));
    parent.init(new Configuration());
    assertInState(STATE.INITED, sibling);
    parent.start();
    assertInState(STATE.INITED, sibling);
    parent.stop();
    assertInState(STATE.INITED, sibling);
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedSiblingInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.INITED));
    parent.init(new Configuration());
    assertInState(STATE.INITED, sibling);
    parent.start();
    assertInState(STATE.STARTED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedSiblingInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STARTED));
    parent.init(new Configuration());
    assertInState(STATE.INITED, sibling);
    parent.start();
    assertInState(STATE.INITED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddInitedSiblingInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STOPPED));
    parent.init(new Configuration());
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedSiblingBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.NOTINITED));
    parent.init(new Configuration());
    assertInState(STATE.STARTED, sibling);
    parent.start();
    assertInState(STATE.STARTED, sibling);
    parent.stop();
    assertInState(STATE.STARTED, sibling);
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedSiblingInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.INITED));
    parent.init(new Configuration());
    assertInState(STATE.STARTED, sibling);
    parent.start();
    assertInState(STATE.STARTED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }


  @Test
  @Timeout(value = 10)
  public void testAddStartedSiblingInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STARTED));
    parent.init(new Configuration());
    assertInState(STATE.STARTED, sibling);
    parent.start();
    assertInState(STATE.STARTED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStartedSiblingInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STOPPED));
    parent.init(new Configuration());
    assertInState(STATE.STARTED, sibling);
    parent.start();
    assertInState(STATE.STARTED, sibling);
    parent.stop();
    assertInState(STATE.STARTED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedSiblingBeforeInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    sibling.stop();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.NOTINITED));
    parent.init(new Configuration());
    assertInState(STATE.STOPPED, sibling);
    parent.start();
    assertInState(STATE.STOPPED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(1, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedSiblingInInit() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    sibling.stop();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.INITED));
    parent.init(new Configuration());
    assertInState(STATE.STOPPED, sibling);
    try {
      parent.start();
      fail("Expected an exception, got " + parent);
    } catch (ServiceStateException e) {
      //expected
    }
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedSiblingInStart() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    sibling.stop();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STARTED));
    parent.init(new Configuration());
    assertInState(STATE.STOPPED, sibling);
    parent.start();
    assertInState(STATE.STOPPED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  @Test
  @Timeout(value = 10)
  public void testAddStoppedSiblingInStop() throws Throwable {
    CompositeService parent = new CompositeService("parent");
    BreakableService sibling = new BreakableService();
    sibling.init(new Configuration());
    sibling.start();
    sibling.stop();
    parent.addService(new AddSiblingService(parent,
                                            sibling,
                                            STATE.STOPPED));
    parent.init(new Configuration());
    assertInState(STATE.STOPPED, sibling);
    parent.start();
    assertInState(STATE.STOPPED, sibling);
    parent.stop();
    assertInState(STATE.STOPPED, sibling);
    assertEquals(2, parent.getServices().size(),
        "Incorrect number of services");
  }

  public static class CompositeServiceAddingAChild extends CompositeService{
    Service child;

    public CompositeServiceAddingAChild(Service child) {
      super("CompositeServiceAddingAChild");
      this.child = child;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      addService(child);
      super.serviceInit(conf);
    }
  }

  public static class ServiceTestRuntimeException extends RuntimeException {
    public ServiceTestRuntimeException(String message) {
      super(message);
    }
  }

  /**
   * This is a composite service that keeps a count of the number of lifecycle
   * events called, and can be set to throw a {@link ServiceTestRuntimeException }
   * during service start or stop
   */
  public static class CompositeServiceImpl extends CompositeService {

    public static boolean isPolicyToStopOnlyStartedServices() {
      return STOP_ONLY_STARTED_SERVICES;
    }

    private static int counter = -1;

    private int callSequenceNumber = -1;

    private boolean throwExceptionOnStart;

    private boolean throwExceptionOnStop;

    public CompositeServiceImpl(int sequenceNumber) {
      super(Integer.toString(sequenceNumber));
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      counter++;
      callSequenceNumber = counter;
      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      if (throwExceptionOnStart) {
        throw new ServiceTestRuntimeException("Fake service start exception");
      }
      counter++;
      callSequenceNumber = counter;
      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      counter++;
      callSequenceNumber = counter;
      if (throwExceptionOnStop) {
        throw new ServiceTestRuntimeException("Fake service stop exception");
      }
      super.serviceStop();
    }

    public static int getCounter() {
      return counter;
    }

    public int getCallSequenceNumber() {
      return callSequenceNumber;
    }

    public void reset() {
      callSequenceNumber = -1;
      counter = -1;
    }

    public static void resetCounter() {
      counter = -1;
    }

    public void setThrowExceptionOnStart(boolean throwExceptionOnStart) {
      this.throwExceptionOnStart = throwExceptionOnStart;
    }

    public void setThrowExceptionOnStop(boolean throwExceptionOnStop) {
      this.throwExceptionOnStop = throwExceptionOnStop;
    }

    @Override
    public String toString() {
      return "Service " + getName();
    }

  }

  /**
   * Composite service that makes the addService method public to all
   */
  public static class ServiceManager extends CompositeService {

    public void addTestService(CompositeService service) {
      addService(service);
    }

    public ServiceManager(String name) {
      super(name);
    }
  }

  public static class AddSiblingService extends CompositeService {
    private final CompositeService parent;
    private final Service serviceToAdd;
    private STATE triggerState;

    public AddSiblingService(CompositeService parent,
                             Service serviceToAdd,
                             STATE triggerState) {
      super("ParentStateManipulatorService");
      this.parent = parent;
      this.serviceToAdd = serviceToAdd;
      this.triggerState = triggerState;
    }

    /**
     * Add the serviceToAdd to the parent if this service
     * is in the state requested
     */
    private void maybeAddSibling() {
      if (getServiceState() == triggerState) {
        parent.addService(serviceToAdd);
      }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
      maybeAddSibling();
      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      maybeAddSibling();
      super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
      maybeAddSibling();
      super.serviceStop();
    }

    /**
     * Expose addService method
     * @param parent parent service
     * @param child child to add
     */
    public static void addChildToService(CompositeService parent, Service child) {
      parent.addService(child);
    }
  }
}
