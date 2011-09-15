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

package org.apache.hadoop.yarn.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.junit.Test;

public class TestCompositeService {

  private static final int NUM_OF_SERVICES = 5;

  private static final int FAILED_SERVICE_SEQ_NUMBER = 2;

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

    assertEquals("Number of registered services ", NUM_OF_SERVICES,
        services.length);

    Configuration conf = new Configuration();
    // Initialise the composite service
    serviceManager.init(conf);

    // Verify the init() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, init() call sequence number should have been ", i,
          services[i].getCallSequenceNumber());
    }

    // Reset the call sequence numbers
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      services[i].reset();
    }

    serviceManager.start();

    // Verify the start() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, start() call sequence number should have been ", i,
          services[i].getCallSequenceNumber());
    }

    // Reset the call sequence numbers
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      services[i].reset();
    }

    serviceManager.stop();

    // Verify the stop() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, stop() call sequence number should have been ",
          ((NUM_OF_SERVICES - 1) - i), services[i].getCallSequenceNumber());
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
    } catch (YarnException e) {
      for (int i = 0; i < NUM_OF_SERVICES - 1; i++) {
        if (i >= FAILED_SERVICE_SEQ_NUMBER) {
          // Failed service state should be INITED
          assertEquals("Service state should have been ", STATE.INITED,
              services[NUM_OF_SERVICES - 1].getServiceState());
        } else {
          assertEquals("Service state should have been ", STATE.STOPPED,
              services[i].getServiceState());
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

    // Start the composite service
    try {
      serviceManager.stop();
    } catch (YarnException e) {
      for (int i = 0; i < NUM_OF_SERVICES - 1; i++) {
        assertEquals("Service state should have been ", STATE.STOPPED,
            services[NUM_OF_SERVICES].getServiceState());
      }
    }
  }

  public static class CompositeServiceImpl extends CompositeService {

    private static int counter = -1;

    private int callSequenceNumber = -1;

    private boolean throwExceptionOnStart;

    private boolean throwExceptionOnStop;

    public CompositeServiceImpl(int sequenceNumber) {
      super(Integer.toString(sequenceNumber));
    }

    @Override
    public synchronized void init(Configuration conf) {
      counter++;
      callSequenceNumber = counter;
      super.init(conf);
    }

    @Override
    public synchronized void start() {
      if (throwExceptionOnStart) {
        throw new YarnException("Fake service start exception");
      }
      counter++;
      callSequenceNumber = counter;
      super.start();
    }

    @Override
    public synchronized void stop() {
      counter++;
      callSequenceNumber = counter;
      if (throwExceptionOnStop) {
        throw new YarnException("Fake service stop exception");
      }
      super.stop();
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

  public static class ServiceManager extends CompositeService {

    public void addTestService(CompositeService service) {
      addService(service);
    }

    public ServiceManager(String name) {
      super(name);
    }
  }

}
