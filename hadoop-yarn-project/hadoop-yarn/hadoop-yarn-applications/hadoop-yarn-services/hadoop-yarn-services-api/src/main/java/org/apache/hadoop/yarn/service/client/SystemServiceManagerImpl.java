/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service.client;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.service.SystemServiceManager;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.jsonSerDeser;

/**
 * SystemServiceManager implementation.
 * Scan for configure system service path.
 *
 * The service path structure is as follows:
 * SYSTEM_SERVICE_DIR_PATH
 * |---- sync
 * |     |--- user1
 * |     |    |---- service1.yarnfile
 * |     |    |---- service2.yarnfile
 * |     |--- user2
 * |     |    |---- service1.yarnfile
 * |     |    ....
 * |     |
 * |---- async
 * |     |--- user3
 * |     |    |---- service1.yarnfile
 * |     |    |---- service2.yarnfile
 * |     |--- user4
 * |     |    |---- service1.yarnfile
 * |     |    ....
 * |     |
 *
 * sync: These services are launched at the time of service start synchronously.
 *       It is a blocking service start.
 * async: These services are launched in separate thread without any delay after
 *       service start. Non-blocking service start.
 */
public class SystemServiceManagerImpl extends AbstractService
    implements SystemServiceManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SystemServiceManagerImpl.class);

  private static final String YARN_FILE_SUFFIX = ".yarnfile";
  private static final String SYNC = "sync";
  private static final String ASYNC = "async";

  private FileSystem fs;
  private Path systemServiceDir;
  private AtomicBoolean stopExecutors = new AtomicBoolean(false);
  private Map<String, Set<Service>> syncUserServices = new HashMap<>();
  private Map<String, Set<Service>> asyncUserServices = new HashMap<>();
  private UserGroupInformation loginUGI;
  private Thread serviceLaucher;

  @VisibleForTesting
  private int badFileNameExtensionSkipCounter;
  @VisibleForTesting
  private Map<String, Integer> ignoredUserServices =
      new HashMap<>();
  @VisibleForTesting
  private int badDirSkipCounter;

  public SystemServiceManagerImpl() {
    super(SystemServiceManagerImpl.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String dirPath =
        conf.get(YarnServiceConf.YARN_SERVICES_SYSTEM_SERVICE_DIRECTORY);
    if (dirPath != null) {
      systemServiceDir = new Path(dirPath);
      LOG.info("System Service Directory is configured to {}",
          systemServiceDir);
      fs = systemServiceDir.getFileSystem(conf);
      this.loginUGI = UserGroupInformation.isSecurityEnabled() ?
          UserGroupInformation.getLoginUser() :
          UserGroupInformation.getCurrentUser();
      LOG.info("UserGroupInformation initialized to {}", loginUGI);
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    scanForUserServices();
    launchUserService(syncUserServices);
    // Create a thread and submit services in background otherwise it
    // block RM switch time.
    serviceLaucher = new Thread(createRunnable());
    serviceLaucher.setName("System service launcher");
    serviceLaucher.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping {}", getName());
    stopExecutors.set(true);

    if (serviceLaucher != null) {
      serviceLaucher.interrupt();
      try {
        serviceLaucher.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }
  }

  private Runnable createRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        launchUserService(asyncUserServices);
      }
    };
  }

  void launchUserService(Map<String, Set<Service>> userServices) {
    for (Map.Entry<String, Set<Service>> entry : userServices.entrySet()) {
      String user = entry.getKey();
      Set<Service> services = entry.getValue();
      if (services.isEmpty()) {
        continue;
      }
      ServiceClient serviceClient = null;
      try {
        UserGroupInformation userUgi = getProxyUser(user);
        serviceClient = createServiceClient(userUgi);
        for (Service service : services) {
          LOG.info("POST: createService = {} user = {}", service, userUgi);
          try {
            launchServices(userUgi, serviceClient, service);
          } catch (IOException | UndeclaredThrowableException e) {
            if (e.getCause() != null) {
              LOG.warn(e.getCause().getMessage());
            } else {
              String message =
                  "Failed to create service " + service.getName() + " : ";
              LOG.error(message, e);
            }
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("System service launcher thread interrupted", e);
        break;
      } catch (Exception e) {
        LOG.error("Error while submitting services for user " + user, e);
      } finally {
        if (serviceClient != null) {
          try {
            serviceClient.close();
          } catch (IOException e) {
            LOG.warn("Error while closing serviceClient for user {}", user);
          }
        }
      }
    }
  }

  private ServiceClient createServiceClient(UserGroupInformation userUgi)
      throws IOException, InterruptedException {
    ServiceClient serviceClient =
        userUgi.doAs(new PrivilegedExceptionAction<ServiceClient>() {
          @Override public ServiceClient run()
              throws IOException, YarnException {
            ServiceClient sc = getServiceClient();
            sc.init(getConfig());
            sc.start();
            return sc;
          }
        });
    return serviceClient;
  }

  private void launchServices(UserGroupInformation userUgi,
      ServiceClient serviceClient, Service service)
      throws IOException, InterruptedException {
    if (service.getState() == ServiceState.STOPPED) {
      userUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws IOException, YarnException {
          serviceClient.actionBuild(service);
          return null;
        }
      });
      LOG.info("Service {} version {} saved.", service.getName(),
          service.getVersion());
    } else {
      ApplicationId applicationId =
          userUgi.doAs(new PrivilegedExceptionAction<ApplicationId>() {
            @Override public ApplicationId run()
                throws IOException, YarnException {
              boolean tryStart = true;
              try {
                serviceClient.actionBuild(service);
              } catch (Exception e) {
                if (e instanceof SliderException && ((SliderException) e)
                    .getExitCode() == SliderExitCodes.EXIT_INSTANCE_EXISTS) {
                  LOG.info("Service {} already exists, will attempt to start " +
                      "service", service.getName());
                } else {
                  tryStart = false;
                  LOG.info("Got exception saving {}, will not attempt to " +
                      "start service", service.getName(), e);
                }
              }
              if (tryStart) {
                return serviceClient.actionStartAndGetId(service.getName());
              } else {
                return null;
              }
            }
          });
      if (applicationId != null) {
        LOG.info("Service {} submitted with Application ID: {}",
            service.getName(), applicationId);
      }
    }
  }

  ServiceClient getServiceClient() {
    return new ServiceClient();
  }

  private UserGroupInformation getProxyUser(String user) {
    UserGroupInformation ugi;
    if (UserGroupInformation.isSecurityEnabled()) {
      ugi = UserGroupInformation.createProxyUser(user, loginUGI);
    } else {
      ugi = UserGroupInformation.createRemoteUser(user);
    }
    return ugi;
  }

  // scan for both launch service types i.e sync and async
  void scanForUserServices() throws IOException {
    if (systemServiceDir == null) {
      return;
    }
    try {
      LOG.info("Scan for launch type on {}", systemServiceDir);
      RemoteIterator<FileStatus> iterLaunchType = list(systemServiceDir);
      while (iterLaunchType.hasNext()) {
        FileStatus launchType = iterLaunchType.next();
        if (!launchType.isDirectory()) {
          LOG.debug("Scanner skips for unknown file {}", launchType.getPath());
          continue;
        }
        if (launchType.getPath().getName().equals(SYNC)) {
          scanForUserServiceDefinition(launchType.getPath(), syncUserServices);
        } else if (launchType.getPath().getName().equals(ASYNC)) {
          scanForUserServiceDefinition(launchType.getPath(), asyncUserServices);
        } else {
          badDirSkipCounter++;
          LOG.debug("Scanner skips for unknown dir {}.", launchType.getPath());
        }
      }
    } catch (FileNotFoundException e) {
      LOG.warn("System service directory {} doesn't not exist.",
          systemServiceDir);
    }
  }

  // Files are under systemServiceDir/<users>. Scan for 2 levels
  // 1st level for users
  // 2nd level for service definitions under user
  private void scanForUserServiceDefinition(Path userDirPath,
      Map<String, Set<Service>> userServices) throws IOException {
    LOG.info("Scan for users on {}", userDirPath);
    RemoteIterator<FileStatus> iterUsers = list(userDirPath);
    while (iterUsers.hasNext()) {
      FileStatus userDir = iterUsers.next();
      // if 1st level is not user directory then skip it.
      if (!userDir.isDirectory()) {
        LOG.info(
            "Service definition {} doesn't belong to any user. Ignoring.. ",
            userDir.getPath().getName());
        continue;
      }
      String userName = userDir.getPath().getName();
      LOG.info("Scanning service definitions for user {}.", userName);

      //2nd level scan
      RemoteIterator<FileStatus> iterServices = list(userDir.getPath());
      while (iterServices.hasNext()) {
        FileStatus serviceCache = iterServices.next();
        String filename = serviceCache.getPath().getName();
        if (!serviceCache.isFile()) {
          LOG.info("Scanner skips for unknown dir {}", filename);
          continue;
        }
        if (!filename.endsWith(YARN_FILE_SUFFIX)) {
          LOG.info("Scanner skips for unknown file extension, filename = {}",
              filename);
          badFileNameExtensionSkipCounter++;
          continue;
        }
        Service service = getServiceDefinition(serviceCache.getPath());
        if (service != null) {
          Set<Service> services = userServices.get(userName);
          if (services == null) {
            services = new HashSet<>();
            userServices.put(userName, services);
          }
          if (!services.add(service)) {
            int count = ignoredUserServices.containsKey(userName) ?
                ignoredUserServices.get(userName) : 0;
            ignoredUserServices.put(userName, count + 1);
            LOG.warn(
                "Ignoring service {} for the user {} as it is already present,"
                    + " filename = {}", service.getName(), userName, filename);
          } else {
            LOG.info("Added service {} for the user {}, filename = {}",
                service.getName(), userName, filename);
          }
        }
      }
    }
  }

  private Service getServiceDefinition(Path filePath) {
    Service service = null;
    try {
      LOG.debug("Loading service definition from FS: {}", filePath);
      service = jsonSerDeser.load(fs, filePath);
    } catch (IOException e) {
      LOG.info("Error while loading service definition from FS: {}", e);
    }
    return service;
  }

  private RemoteIterator<FileStatus> list(Path path) throws IOException {
    return new StoppableRemoteIterator(fs.listStatusIterator(path));
  }

  @VisibleForTesting Map<String, Integer> getIgnoredUserServices() {
    return ignoredUserServices;
  }

  private class StoppableRemoteIterator implements RemoteIterator<FileStatus> {
    private final RemoteIterator<FileStatus> remote;

    StoppableRemoteIterator(RemoteIterator<FileStatus> remote) {
      this.remote = remote;
    }

    @Override public boolean hasNext() throws IOException {
      return !stopExecutors.get() && remote.hasNext();
    }

    @Override public FileStatus next() throws IOException {
      return remote.next();
    }
  }

  @VisibleForTesting
  Map<String, Set<Service>> getSyncUserServices() {
    return syncUserServices;
  }

  @VisibleForTesting
  int getBadFileNameExtensionSkipCounter() {
    return badFileNameExtensionSkipCounter;
  }

  @VisibleForTesting
  int getBadDirSkipCounter() {
    return badDirSkipCounter;
  }
}