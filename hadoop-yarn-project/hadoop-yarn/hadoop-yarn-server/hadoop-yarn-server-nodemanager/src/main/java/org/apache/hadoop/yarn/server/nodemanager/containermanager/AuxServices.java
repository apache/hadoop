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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.util.FSDownload;
import com.google.common.base.Preconditions;

public class AuxServices extends AbstractService
    implements ServiceStateChangeListener, EventHandler<AuxServicesEvent> {

  public static final String NM_AUX_SERVICE_DIR = "nmAuxService";
  public static final FsPermission NM_AUX_SERVICE_DIR_PERM =
      new FsPermission((short) 0700);

  static final String STATE_STORE_ROOT_NAME = "nm-aux-services";

  private static final Logger LOG =
       LoggerFactory.getLogger(AuxServices.class);
  private static final String DEL_SUFFIX = "_DEL_";

  protected final Map<String,AuxiliaryService> serviceMap;
  protected final Map<String,ByteBuffer> serviceMetaData;
  private final AuxiliaryLocalPathHandler auxiliaryLocalPathHandler;
  private final LocalDirsHandlerService dirsHandler;
  private final DeletionService delService;
  private final UserGroupInformation userUGI;

  private final Pattern p = Pattern.compile("^[A-Za-z_]+[A-Za-z0-9_]*$");

  public AuxServices(AuxiliaryLocalPathHandler auxiliaryLocalPathHandler,
      Context nmContext, DeletionService deletionService) {
    super(AuxServices.class.getName());
    serviceMap =
      Collections.synchronizedMap(new HashMap<String,AuxiliaryService>());
    serviceMetaData =
      Collections.synchronizedMap(new HashMap<String,ByteBuffer>());
    this.auxiliaryLocalPathHandler = auxiliaryLocalPathHandler;
    this.dirsHandler = nmContext.getLocalDirsHandler();
    this.delService = deletionService;
    this.userUGI = getRemoteUgi();
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
        String classKey = String.format(
            YarnConfiguration.NM_AUX_SERVICE_FMT, sName);
        String className = conf.get(classKey);
        final String appLocalClassPath = conf.get(String.format(
            YarnConfiguration.NM_AUX_SERVICES_CLASSPATH, sName));
        final String appRemoteClassPath = conf.get(String.format(
            YarnConfiguration.NM_AUX_SERVICE_REMOTE_CLASSPATH, sName));
        AuxiliaryService s = null;
        boolean useCustomerClassLoader = ((appLocalClassPath != null
            && !appLocalClassPath.isEmpty()) ||
            (appRemoteClassPath != null && !appRemoteClassPath.isEmpty()))
            && className != null && !className.isEmpty();
        if (useCustomerClassLoader) {
          // load AuxiliaryService from local class path
          if (appRemoteClassPath == null || appRemoteClassPath.isEmpty()) {
            s = AuxiliaryServiceWithCustomClassLoader.getInstance(
                conf, className, appLocalClassPath);
          } else {
            // load AuxiliaryService from remote class path
            if (appLocalClassPath != null && !appLocalClassPath.isEmpty()) {
              throw new YarnRuntimeException("The aux serivce:" + sName
                  + " has configured local classpath:" + appLocalClassPath
                  + " and remote classpath:" + appRemoteClassPath
                  + ". Only one of them should be configured.");
            }
            FileContext localLFS = getLocalFileContext(conf);
            // create NM aux-service dir in NM localdir if it does not exist.
            Path nmAuxDir = dirsHandler.getLocalPathForWrite("."
                + Path.SEPARATOR + NM_AUX_SERVICE_DIR);
            if (!localLFS.util().exists(nmAuxDir)) {
              try {
                localLFS.mkdir(nmAuxDir, NM_AUX_SERVICE_DIR_PERM, true);
              } catch (IOException ex) {
                throw new YarnRuntimeException("Fail to create dir:"
                    + nmAuxDir.toString(), ex);
              }
            }
            Path src = new Path(appRemoteClassPath);
            FileContext remoteLFS = getRemoteFileContext(src.toUri(), conf);
            FileStatus scFileStatus = remoteLFS.getFileStatus(src);
            if (!scFileStatus.getOwner().equals(
                this.userUGI.getShortUserName())) {
              throw new YarnRuntimeException("The remote jarfile owner:"
                  + scFileStatus.getOwner() + " is not the same as the NM user:"
                  + this.userUGI.getShortUserName() + ".");
            }
            if ((scFileStatus.getPermission().toShort() & 0022) != 0) {
              throw new YarnRuntimeException("The remote jarfile should not "
                  + "be writable by group or others. "
                  + "The current Permission is "
                  + scFileStatus.getPermission().toShort());
            }
            Path dest = null;
            Path downloadDest = new Path(nmAuxDir,
                className + "_" + scFileStatus.getModificationTime());
            // check whether we need to re-download the jar
            // from remote directory
            Path targetDirPath = new Path(downloadDest,
                scFileStatus.getPath().getName());
            FileStatus[] allSubDirs = localLFS.util().listStatus(nmAuxDir);
            boolean reDownload = true;
            for (FileStatus sub : allSubDirs) {
              if (sub.getPath().getName().equals(downloadDest.getName())) {
                reDownload = false;
                dest = new Path(targetDirPath + Path.SEPARATOR + "*");
                break;
              } else {
                if (sub.getPath().getName().contains(className) &&
                    !sub.getPath().getName().endsWith(DEL_SUFFIX)) {
                  Path delPath = new Path(sub.getPath().getParent(),
                      sub.getPath().getName() + DEL_SUFFIX);
                  localLFS.rename(sub.getPath(), delPath);
                  LOG.info("delete old aux service jar dir:"
                      + delPath.toString());
                  FileDeletionTask deletionTask = new FileDeletionTask(
                      this.delService, null, delPath, null);
                  this.delService.delete(deletionTask);
                }
              }
            }
            if (reDownload) {
              LocalResourceType srcType = null;
              String lowerDst = StringUtils.toLowerCase(src.toString());
              if (lowerDst.endsWith(".jar")) {
                srcType = LocalResourceType.FILE;
              } else if (lowerDst.endsWith(".zip") ||
                  lowerDst.endsWith(".tar.gz") || lowerDst.endsWith(".tgz")
                  || lowerDst.endsWith(".tar")) {
                srcType = LocalResourceType.ARCHIVE;
              } else {
                throw new YarnRuntimeException(
                    "Can not unpack file from remote-file-path:" + src
                        + "for aux-service:" + ".\n");
              }
              LocalResource scRsrc = LocalResource.newInstance(
                  URL.fromURI(src.toUri()),
                  srcType, LocalResourceVisibility.PRIVATE,
                  scFileStatus.getLen(), scFileStatus.getModificationTime());
              FSDownload download = new FSDownload(localLFS, null, conf,
                  downloadDest, scRsrc, null);
              try {
                Path downloaded = download.call();
                // don't need to convert downloaded path into a dir
                // since its already a jar path.
                dest = downloaded;
              } catch (Exception ex) {
                throw new YarnRuntimeException(
                    "Exception happend while downloading files "
                    + "for aux-service:" + sName + " and remote-file-path:"
                    + src + ".\n" + ex.getMessage());
              }
            }
            s = AuxiliaryServiceWithCustomClassLoader.getInstance(
                new Configuration(conf), className, dest.toString());
          }
          LOG.info("The aux service:" + sName
              + " are using the custom classloader");
        } else {
          Class<? extends AuxiliaryService> sClass = conf.getClass(
              classKey, null, AuxiliaryService.class);

          if (sClass == null) {
            throw new RuntimeException("No class defined for " + sName);
          }
          s = ReflectionUtils.newInstance(sClass, new Configuration(conf));
        }
        if (s == null) {
          throw new RuntimeException("No object created for " + sName);
        }
        // TODO better use s.getName()?
        if(!sName.equals(s.getName())) {
          LOG.warn("The Auxiliary Service named '"+sName+"' in the "
              +"configuration is for "+s.getClass()+" which has "
              +"a name of '"+s.getName()+"'. Because these are "
              +"not the same tools trying to send ServiceData and read "
              +"Service Meta Data may have issues unless the refer to "
              +"the name in the config.");
        }
        s.setAuxiliaryLocalPathHandler(auxiliaryLocalPathHandler);
        addService(sName, s);
        if (recoveryEnabled) {
          Path storePath = new Path(stateStoreRoot, sName);
          stateStoreFs.mkdirs(storePath, storeDirPerms);
          s.setRecoveryPath(storePath);
        }
        s.init(new Configuration(conf));
      } catch (RuntimeException e) {
        LOG.error("Failed to initialize " + sName, e);
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
    LOG.error("Service " + service.getName() + " changed state: " +
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
                event.getContainer().getUser(),
                event.getContainer().getContainerId(),
                event.getContainer().getResource(), event.getContainer()
                .getContainerTokenIdentifier().getContainerType()));
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
                event.getContainer().getResource(), event.getContainer()
                .getContainerTokenIdentifier().getContainerType()));
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

  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  FileContext getRemoteFileContext(final URI path, Configuration conf) {
    try {
      return FileContext.getFileContext(path, conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access remote fs");
    }
  }

  private UserGroupInformation getRemoteUgi() {
    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg = "Cannot obtain the user-name. Got exception: "
          + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw new YarnRuntimeException(msg);
    }
    return remoteUgi;
  }
}
