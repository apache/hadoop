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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.ServiceStateChangeListener;

public class AuxServices extends AbstractService
    implements ServiceStateChangeListener, EventHandler<AuxServicesEvent> {

  private static final Log LOG = LogFactory.getLog(AuxServices.class);

  public static final String AUX_SERVICES = "nodemanager.auxiluary.services";
  public static final String AUX_SERVICE_CLASS_FMT =
    "nodemanager.aux.service.%s.class";
  public final Map<String,AuxiliaryService> serviceMap;

  public AuxServices() {
    super(AuxServices.class.getName());
    serviceMap =
      Collections.synchronizedMap(new HashMap<String,AuxiliaryService>());
    // Obtain services from configuration in init()
  }

  protected final synchronized void addService(String name,
      AuxiliaryService service) {
    LOG.info("Adding auxiliary service " +
        service.getName() + ", \"" + name + "\"");
    serviceMap.put(name, service);
  }

  Collection<AuxiliaryService> getServices() {
    return Collections.unmodifiableCollection(serviceMap.values());
  }

  @Override
  public void init(Configuration conf) {
    Collection<String> auxNames = conf.getStringCollection(AUX_SERVICES);
    for (final String sName : auxNames) {
      try {
        Class<? extends AuxiliaryService> sClass = conf.getClass(
              String.format(AUX_SERVICE_CLASS_FMT, sName), null,
              AuxiliaryService.class);
        if (null == sClass) {
          throw new RuntimeException("No class defiend for " + sName);
        }
        AuxiliaryService s = ReflectionUtils.newInstance(sClass, conf);
        // TODO better use use s.getName()?
        addService(sName, s);
        s.init(conf);
      } catch (RuntimeException e) {
        LOG.fatal("Failed to initialize " + sName, e);
        throw e;
      }
    }
    super.init(conf);
  }

  @Override
  public void start() {
    // TODO fork(?) services running as configured user
    //      monitor for health, shutdown/restart(?) if any should die
    for (Service service : serviceMap.values()) {
      service.start();
      service.register(this);
    }
    super.start();
  }

  @Override
  public void stop() {
    try {
      synchronized (serviceMap) {
        for (Service service : serviceMap.values()) {
          if (service.getServiceState() == Service.STATE.STARTED) {
            service.unregister(this);
            service.stop();
          }
        }
        serviceMap.clear();
      }
    } finally {
      super.stop();
    }
  }

  @Override
  public void stateChanged(Service service) {
    LOG.fatal("Service " + service.getName() + " changed state: " +
        service.getServiceState());
    stop();
  }

  @Override
  public void handle(AuxServicesEvent event) {
    LOG.info("Got event " + event.getType() + " for service "
        + event.getServiceID());
    AuxiliaryService service = serviceMap.get(event.getServiceID());
    if (null == service) {
      // TODO kill all containers waiting on Application
      return;
    }
    switch (event.getType()) {
    case APPLICATION_INIT:
      service.initApp(event.getUser(), event.getApplicationID(),
          event.getServiceData());
      break;
    case APPLICATION_STOP:
      service.stopApp(event.getApplicationID());
      break;
    default:
      throw new RuntimeException("Unknown type: " + event.getType());
    }
  }

  public interface AuxiliaryService extends Service {
    void initApp(String user, ApplicationId appId, ByteBuffer data);
    void stopApp(ApplicationId appId);
  }

}
