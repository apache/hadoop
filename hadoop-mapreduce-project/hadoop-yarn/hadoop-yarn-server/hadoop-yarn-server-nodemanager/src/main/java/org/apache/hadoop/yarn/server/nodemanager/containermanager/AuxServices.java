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
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.ServiceStateChangeListener;

public class AuxServices extends AbstractService
    implements ServiceStateChangeListener, EventHandler<AuxServicesEvent> {

  private static final Log LOG = LogFactory.getLog(AuxServices.class);

  protected final Map<String,AuxiliaryService> serviceMap;
  protected final Map<String,ByteBuffer> serviceMeta;

  public AuxServices() {
    super(AuxServices.class.getName());
    serviceMap =
      Collections.synchronizedMap(new HashMap<String,AuxiliaryService>());
    serviceMeta =
      Collections.synchronizedMap(new HashMap<String,ByteBuffer>());
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

  /**
   * @return the meta data for all registered services, that have been started.
   * If a service has not been started no metadata will be available. The key
   * the the name of the service as defined in the configuration.
   */
  public Map<String, ByteBuffer> getMeta() {
    Map<String, ByteBuffer> metaClone = new HashMap<String, ByteBuffer>(
        serviceMeta.size());
    synchronized (serviceMeta) {
      for (Entry<String, ByteBuffer> entry : serviceMeta.entrySet()) {
        metaClone.put(entry.getKey(), entry.getValue().duplicate());
      }
    }
    return metaClone;
  }

  @Override
  public void init(Configuration conf) {
    Collection<String> auxNames = conf.getStringCollection(
        YarnConfiguration.NM_AUX_SERVICES);
    for (final String sName : auxNames) {
      try {
        Class<? extends AuxiliaryService> sClass = conf.getClass(
              String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, sName), null,
              AuxiliaryService.class);
        if (null == sClass) {
          throw new RuntimeException("No class defiend for " + sName);
        }
        AuxiliaryService s = ReflectionUtils.newInstance(sClass, conf);
        // TODO better use s.getName()?
        if(!sName.equals(s.getName())) {
          LOG.warn("The Auxilurary Service named '"+sName+"' in the "
                  +"configuration is for class "+sClass+" which has "
                  +"a name of '"+s.getName()+"'. Because these are "
                  +"not the same tools trying to send ServiceData and read "
                  +"Service Meta Data may have issues unless the refer to "
                  +"the name in the config.");
        }
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
    for (Map.Entry<String, AuxiliaryService> entry : serviceMap.entrySet()) {
      AuxiliaryService service = entry.getValue();
      String name = entry.getKey();
      service.start();
      service.register(this);
      ByteBuffer meta = service.getMeta();
      if(meta != null) {
        serviceMeta.put(name, meta);
      }
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
        serviceMeta.clear();
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
    LOG.info("Got event " + event.getType() + " for appId "
        + event.getApplicationID());
    switch (event.getType()) {
    case APPLICATION_INIT:
      LOG.info("Got APPLICATION_INIT for service " + event.getServiceID());
      AuxiliaryService service = serviceMap.get(event.getServiceID());
      if (null == service) {
        LOG.info("service is null");
        // TODO kill all containers waiting on Application
        return;
      }
      service.initApp(event.getUser(), event.getApplicationID(),
          event.getServiceData());
      break;
    case APPLICATION_STOP:
      for (AuxiliaryService serv : serviceMap.values()) {
        serv.stopApp(event.getApplicationID());
      }
      break;
    default:
      throw new RuntimeException("Unknown type: " + event.getType());
    }
  }

  public interface AuxiliaryService extends Service {
    void initApp(String user, ApplicationId appId, ByteBuffer data);
    void stopApp(ApplicationId appId);
    /**
     * Retreive metadata for this service.  This is likely going to be contact
     * information so that applications can access the service remotely.  Ideally
     * each service should provide a method to parse out the information to a usable
     * class.  This will only be called after the services start method has finished.
     * the result may be cached.
     * @return metadata for this service that should be made avaiable to applications.
     */
    ByteBuffer getMeta();
  }

}
