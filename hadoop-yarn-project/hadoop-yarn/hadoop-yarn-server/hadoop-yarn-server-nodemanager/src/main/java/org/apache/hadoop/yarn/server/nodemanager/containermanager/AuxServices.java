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
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;

import com.google.common.base.Preconditions;

public class AuxServices extends AbstractService
    implements ServiceStateChangeListener, EventHandler<AuxServicesEvent> {

  static final String STATE_STORE_ROOT_NAME = "nm-aux-services";

  private static final Log LOG = LogFactory.getLog(AuxServices.class);

  protected final Map<String,AuxiliaryService> serviceMap;
  protected final Map<String,ByteBuffer> serviceMetaData;

  private final Pattern p = Pattern.compile("^[A-Za-z_]+[A-Za-z0-9_]*$");

  public AuxServices() {
    super(AuxServices.class.getName());
    serviceMap =
      Collections.synchronizedMap(new HashMap<String,AuxiliaryService>());
    serviceMetaData =
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
   * is the name of the service as defined in the configuration.
   */
  public Map<String, ByteBuffer> getMetaData() {
    Map<String, ByteBuffer> metaClone = new HashMap<String, ByteBuffer>(
        serviceMetaData.size());
    synchronized (serviceMetaData) {
      for (Entry<String, ByteBuffer> entry : serviceMetaData.entrySet()) {
        metaClone.put(entry.getKey(), entry.getValue().duplicate());
      }
    }
    return metaClone;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    final FsPermission storeDirPerms = new FsPermission((short)0700);
    Path stateStoreRoot = null;
    FileSystem stateStoreFs = null;
    boolean recoveryEnabled = conf.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    if (recoveryEnabled) {
      stateStoreRoot = new Path(conf.get(YarnConfiguration.NM_RECOVERY_DIR),
          STATE_STORE_ROOT_NAME);
      stateStoreFs = FileSystem.getLocal(conf);
    }
    Collection<String> auxNames = conf.getStringCollection(
        YarnConfiguration.NM_AUX_SERVICES);
    for (final String sName : auxNames) {
      try {
        Preconditions
            .checkArgument(
                validateAuxServiceName(sName),
                "The ServiceName: " + sName + " set in " +
                YarnConfiguration.NM_AUX_SERVICES +" is invalid." +
                "The valid service name should only contain a-zA-Z0-9_ " +
                "and can not start with numbers");
        Class<? extends AuxiliaryService> sClass = conf.getClass(
              String.format(YarnConfiguration.NM_AUX_SERVICE_FMT, sName), null,
              AuxiliaryService.class);
        if (null == sClass) {
          throw new RuntimeException("No class defined for " + sName);
        }
        AuxiliaryService s = ReflectionUtils.newInstance(sClass, conf);
        // TODO better use s.getName()?
        if(!sName.equals(s.getName())) {
          LOG.warn("The Auxilurary Service named '"+sName+"' in the "
                  +"configuration is for "+sClass+" which has "
                  +"a name of '"+s.getName()+"'. Because these are "
                  +"not the same tools trying to send ServiceData and read "
                  +"Service Meta Data may have issues unless the refer to "
                  +"the name in the config.");
        }
        addService(sName, s);
        if (recoveryEnabled) {
          Path storePath = new Path(stateStoreRoot, sName);
          stateStoreFs.mkdirs(storePath, storeDirPerms);
          s.setRecoveryPath(storePath);
        }
        s.init(conf);
      } catch (RuntimeException e) {
        LOG.fatal("Failed to initialize " + sName, e);
        throw e;
      }
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    // TODO fork(?) services running as configured user
    //      monitor for health, shutdown/restart(?) if any should die
    for (Map.Entry<String, AuxiliaryService> entry : serviceMap.entrySet()) {
      AuxiliaryService service = entry.getValue();
      String name = entry.getKey();
      service.start();
      service.registerServiceListener(this);
      ByteBuffer meta = service.getMetaData();
      if(meta != null) {
        serviceMetaData.put(name, meta);
      }
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    try {
      synchronized (serviceMap) {
        for (Service service : serviceMap.values()) {
          if (service.getServiceState() == Service.STATE.STARTED) {
            service.unregisterServiceListener(this);
            service.stop();
          }
        }
        serviceMap.clear();
        serviceMetaData.clear();
      }
    } finally {
      super.serviceStop();
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
        AuxiliaryService service = null;
        try {
          service = serviceMap.get(event.getServiceID());
          service
              .initializeApplication(new ApplicationInitializationContext(event
                  .getUser(), event.getApplicationID(), event.getServiceData()));
        } catch (Throwable th) {
          logWarningWhenAuxServiceThrowExceptions(service,
              AuxServicesEventType.APPLICATION_INIT, th);
        }
        break;
      case APPLICATION_STOP:
        for (AuxiliaryService serv : serviceMap.values()) {
          try {
            serv.stopApplication(new ApplicationTerminationContext(event
                .getApplicationID()));
          } catch (Throwable th) {
            logWarningWhenAuxServiceThrowExceptions(serv,
                AuxServicesEventType.APPLICATION_STOP, th);
          }
        }
        break;
      case CONTAINER_INIT:
        for (AuxiliaryService serv : serviceMap.values()) {
          try {
            serv.initializeContainer(new ContainerInitializationContext(
                event.getUser(), event.getContainer().getContainerId(),
                event.getContainer().getResource()));
          } catch (Throwable th) {
            logWarningWhenAuxServiceThrowExceptions(serv,
                AuxServicesEventType.CONTAINER_INIT, th);
          }
        }
        break;
      case CONTAINER_STOP:
        for (AuxiliaryService serv : serviceMap.values()) {
          try {
            serv.stopContainer(new ContainerTerminationContext(
                event.getUser(), event.getContainer().getContainerId(),
                event.getContainer().getResource()));
          } catch (Throwable th) {
            logWarningWhenAuxServiceThrowExceptions(serv,
                AuxServicesEventType.CONTAINER_STOP, th);
          }
        }
        break;
      default:
        throw new RuntimeException("Unknown type: " + event.getType());
    }
  }

  private boolean validateAuxServiceName(String name) {
    if (name == null || name.trim().isEmpty()) {
      return false;
    }
    return p.matcher(name).matches();
  }

  private void logWarningWhenAuxServiceThrowExceptions(AuxiliaryService service,
      AuxServicesEventType eventType, Throwable th) {
    LOG.warn((null == service ? "The auxService is null"
        : "The auxService name is " + service.getName())
        + " and it got an error at event: " + eventType, th);
  }
}
