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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.junit.Test;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;


import static org.apache.hadoop.yarn.service.Service.STATE.*;

public class TestAuxServices {
  private static final Log LOG = LogFactory.getLog(TestAuxServices.class);

  static class LightService extends AbstractService
      implements AuxServices.AuxiliaryService {
    private final char idef;
    private final int expected_appId;
    private int remaining_init;
    private int remaining_stop;
    private ByteBuffer meta = null;
    private ArrayList<Integer> stoppedApps;

    LightService(String name, char idef, int expected_appId) {
      this(name, idef, expected_appId, null);
    } 
    LightService(String name, char idef, int expected_appId, ByteBuffer meta) {
      super(name);
      this.idef = idef;
      this.expected_appId = expected_appId;
      this.meta = meta;
      this.stoppedApps = new ArrayList<Integer>();
    }

    public ArrayList<Integer> getAppIdsStopped() {
      return (ArrayList)this.stoppedApps.clone();
    }

    @Override
    public void init(Configuration conf) {
      remaining_init = conf.getInt(idef + ".expected.init", 0);
      remaining_stop = conf.getInt(idef + ".expected.stop", 0);
      super.init(conf);
    }
    @Override
    public void stop() {
      assertEquals(0, remaining_init);
      assertEquals(0, remaining_stop);
      super.stop();
    }
    @Override
    public void initApp(String user, ApplicationId appId, ByteBuffer data) {
      assertEquals(idef, data.getChar());
      assertEquals(expected_appId, data.getInt());
      assertEquals(expected_appId, appId.getId());
    }
    @Override
    public void stopApp(ApplicationId appId) {
      stoppedApps.add(appId.getId());
    }
    @Override
    public ByteBuffer getMeta() {
      return meta;
    }
  }

  static class ServiceA extends LightService {
    public ServiceA() { 
      super("A", 'A', 65, ByteBuffer.wrap("A".getBytes()));
    }
  }

  static class ServiceB extends LightService {
    public ServiceB() { 
      super("B", 'B', 66, ByteBuffer.wrap("B".getBytes()));
    }
  }

  @Test
  public void testAuxEventDispatch() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    conf.setInt("A.expected.init", 1);
    conf.setInt("B.expected.stop", 1);
    final AuxServices aux = new AuxServices();
    aux.init(conf);
    aux.start();

    ApplicationId appId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(ApplicationId.class);
    appId.setId(65);
    ByteBuffer buf = ByteBuffer.allocate(6);
    buf.putChar('A');
    buf.putInt(65);
    buf.flip();
    AuxServicesEvent event = new AuxServicesEvent(
        AuxServicesEventType.APPLICATION_INIT, "user0", appId, "Asrv", buf);
    aux.handle(event);
    appId.setId(66);
    event = new AuxServicesEvent(
        AuxServicesEventType.APPLICATION_STOP, "user0", appId, "Bsrv", null);
    // verify all services got the stop event 
    aux.handle(event);
    Collection<AuxServices.AuxiliaryService> servs = aux.getServices();
    for (AuxServices.AuxiliaryService serv: servs) {
      ArrayList<Integer> appIds = ((LightService)serv).getAppIdsStopped();
      assertEquals("app not properly stopped", 1, appIds.size());
      assertTrue("wrong app stopped", appIds.contains((Integer)66));
    }
  }

  @Test
  public void testAuxServices() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    final AuxServices aux = new AuxServices();
    aux.init(conf);

    int latch = 1;
    for (Service s : aux.getServices()) {
      assertEquals(INITED, s.getServiceState());
      if (s instanceof ServiceA) { latch *= 2; }
      else if (s instanceof ServiceB) { latch *= 3; }
      else fail("Unexpected service type " + s.getClass());
    }
    assertEquals("Invalid mix of services", 6, latch);
    aux.start();
    for (Service s : aux.getServices()) {
      assertEquals(STARTED, s.getServiceState());
    }

    aux.stop();
    for (Service s : aux.getServices()) {
      assertEquals(STOPPED, s.getServiceState());
    }
  }


  @Test
  public void testAuxServicesMeta() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    final AuxServices aux = new AuxServices();
    aux.init(conf);

    int latch = 1;
    for (Service s : aux.getServices()) {
      assertEquals(INITED, s.getServiceState());
      if (s instanceof ServiceA) { latch *= 2; }
      else if (s instanceof ServiceB) { latch *= 3; }
      else fail("Unexpected service type " + s.getClass());
    }
    assertEquals("Invalid mix of services", 6, latch);
    aux.start();
    for (Service s : aux.getServices()) {
      assertEquals(STARTED, s.getServiceState());
    }

    Map<String, ByteBuffer> meta = aux.getMeta();
    assertEquals(2, meta.size());
    assertEquals("A", new String(meta.get("Asrv").array()));
    assertEquals("B", new String(meta.get("Bsrv").array()));

    aux.stop();
    for (Service s : aux.getServices()) {
      assertEquals(STOPPED, s.getServiceState());
    }
  }



  @Test
  public void testAuxUnexpectedStop() {
    Configuration conf = new Configuration();
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, new String[] { "Asrv", "Bsrv" });
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Asrv"),
        ServiceA.class, Service.class);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, "Bsrv"),
        ServiceB.class, Service.class);
    final AuxServices aux = new AuxServices();
    aux.init(conf);
    aux.start();

    Service s = aux.getServices().iterator().next();
    s.stop();
    assertEquals("Auxiliary service stopped, but AuxService unaffected.",
        STOPPED, aux.getServiceState());
    assertTrue(aux.getServices().isEmpty());
  }

}
