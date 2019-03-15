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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceFile;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecord;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.records.AuxServiceRecords;
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

  public static final String CLASS_NAME = "class.name";
  public static final String SYSTEM_CLASSES = "system.classes";

  static final String STATE_STORE_ROOT_NAME = "nm-aux-services";

  private static final Logger LOG =
       LoggerFactory.getLogger(AuxServices.class);
  private static final String DEL_SUFFIX = "_DEL_";

  private final Map<String, AuxiliaryService> serviceMap;
  private final Map<String, AuxServiceRecord> serviceRecordMap;
  private final Map<String, ByteBuffer> serviceMetaData;
  private final AuxiliaryLocalPathHandler auxiliaryLocalPathHandler;
  private final LocalDirsHandlerService dirsHandler;
  private final DeletionService delService;
  private final UserGroupInformation userUGI;

  private final FsPermission storeDirPerms = new FsPermission((short)0700);
  private Path stateStoreRoot = null;
  private FileSystem stateStoreFs = null;

  private volatile boolean manifestEnabled = false;
  private volatile Path manifest;
  private volatile FileSystem manifestFS;
  private Timer manifestReloadTimer;
  private TimerTask manifestReloadTask;
  private long manifestReloadInterval;
  private long manifestModifyTS = -1;

  private final ObjectMapper mapper;

  private final Pattern p = Pattern.compile("^[A-Za-z_]+[A-Za-z0-9_]*$");

  AuxServices(AuxiliaryLocalPathHandler auxiliaryLocalPathHandler,
      Context nmContext, DeletionService deletionService) {
    super(AuxServices.class.getName());
    serviceMap =
      Collections.synchronizedMap(new HashMap<String, AuxiliaryService>());
    serviceRecordMap =
        Collections.synchronizedMap(new HashMap<String, AuxServiceRecord>());
    serviceMetaData =
      Collections.synchronizedMap(new HashMap<String,ByteBuffer>());
    this.auxiliaryLocalPathHandler = auxiliaryLocalPathHandler;
    this.dirsHandler = nmContext.getLocalDirsHandler();
    this.delService = deletionService;
    this.userUGI = getRemoteUgi();
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // Obtain services from configuration in init()
  }

  /**
   * Returns whether aux services manifest / dynamic loading is enabled.
   */
  public boolean isManifestEnabled() {
    return manifestEnabled;
  }

  /**
   * Adds a service to the service map.
   *
   * @param name aux service name
   * @param service aux service
   * @param serviceRecord aux service record
   */
  protected final synchronized void addService(String name,
      AuxiliaryService service, AuxServiceRecord serviceRecord) {
    LOG.info("Adding auxiliary service " + serviceRecord.getName() +
        " version " + serviceRecord.getVersion());
    serviceMap.put(name, service);
    serviceRecordMap.put(name, serviceRecord);
  }

  Collection<AuxiliaryService> getServices() {
    return Collections.unmodifiableCollection(serviceMap.values());
  }

  /**
   * Gets current aux service records.
   *
   * @return a collection of service records
   */
  public Collection<AuxServiceRecord> getServiceRecords() {
    return Collections.unmodifiableCollection(serviceRecordMap.values());
  }

  /**
   * @return the meta data for all registered services, that have been started.
   * If a service has not been started no metadata will be available. The key
   * is the name of the service as defined in the configuration.
   */
  public Map<String, ByteBuffer> getMetaData() {
    Map<String, ByteBuffer> metaClone = new HashMap<>(serviceMetaData.size());
    synchronized (serviceMetaData) {
      for (Entry<String, ByteBuffer> entry : serviceMetaData.entrySet()) {
        metaClone.put(entry.getKey(), entry.getValue().duplicate());
      }
    }
    return metaClone;
  }

  /**
   * Creates an auxiliary service from a specification using the Configuration
   * classloader.
   *
   * @param service aux service record
   * @return auxiliary service
   */
  private AuxiliaryService createAuxServiceFromConfiguration(AuxServiceRecord
      service) {
    Configuration c = new Configuration(false);
    c.set(CLASS_NAME, getClassName(service));
    Class<? extends AuxiliaryService> sClass = c.getClass(CLASS_NAME,
        null, AuxiliaryService.class);

    if (sClass == null) {
      throw new YarnRuntimeException("No class defined for auxiliary " +
          "service" + service.getName());
    }
    return ReflectionUtils.newInstance(sClass, null);
  }

  /**
   * Creates an auxiliary service from a specification using a custom local
   * classpath.
   *
   * @param service aux service record
   * @param appLocalClassPath local class path
   * @param conf configuration
   * @return auxiliary service
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private AuxiliaryService createAuxServiceFromLocalClasspath(
      AuxServiceRecord service, String appLocalClassPath, Configuration conf)
      throws IOException, ClassNotFoundException {
    Preconditions.checkArgument(appLocalClassPath != null &&
        !appLocalClassPath.isEmpty(),
        "local classpath was null in createAuxServiceFromLocalClasspath");
    final String sName = service.getName();
    final String className = getClassName(service);

    if (service.getConfiguration() != null && service.getConfiguration()
        .getFiles().size() > 0) {
      throw new YarnRuntimeException("The aux service:" + sName
          + " has configured local classpath:" + appLocalClassPath
          + " and config files:" + service.getConfiguration().getFiles()
          + ". Only one of them should be configured.");
    }

    return AuxiliaryServiceWithCustomClassLoader.getInstance(conf, className,
        appLocalClassPath, getSystemClasses(service, className));
  }

  /**
   * Creates an auxiliary service from a specification.
   *
   * @param service aux service record
   * @param conf configuration
   * @param fromConfiguration true if from configuration, false if from manifest
   * @return auxiliary service
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private AuxiliaryService createAuxService(AuxServiceRecord service,
      Configuration conf, boolean fromConfiguration) throws IOException,
      ClassNotFoundException {
    final String sName = service.getName();
    final String className = getClassName(service);
    if (className == null || className.isEmpty()) {
      throw new YarnRuntimeException("Class name not provided for auxiliary " +
          "service " + sName);
    }
    if (fromConfiguration) {
      // aux services from the configuration have an additional configuration
      // option specifying a local classpath that will not be localized
      final String appLocalClassPath = conf.get(String.format(
          YarnConfiguration.NM_AUX_SERVICES_CLASSPATH, sName));
      if (appLocalClassPath != null && !appLocalClassPath.isEmpty()) {
        return createAuxServiceFromLocalClasspath(service, appLocalClassPath,
            conf);
      }
    }
    AuxServiceConfiguration serviceConf = service.getConfiguration();
    List<Path> destFiles = new ArrayList<>();
    if (serviceConf != null) {
      List<AuxServiceFile> files = serviceConf.getFiles();
      if (files != null) {
        for (AuxServiceFile file : files) {
          // localize file (if needed) and add it to the list of paths that
          // will become the classpath
          destFiles.add(maybeDownloadJars(sName, className, file.getSrcFile(),
              file.getType(), conf));
        }
      }
    }

    if (destFiles.size() > 0) {
      // create aux service using a custom localized classpath
      LOG.info("The aux service:" + sName
          + " is using the custom classloader with classpath " + destFiles);
      return AuxiliaryServiceWithCustomClassLoader.getInstance(conf,
          className, StringUtils.join(File.pathSeparatorChar, destFiles),
          getSystemClasses(service, className));
    } else {
      return createAuxServiceFromConfiguration(service);
    }
  }

  /**
   * Copies the specified remote file to local NM aux service directory. If the
   * same file already exists (as determined by modification time), the file
   * will not be copied again.
   *
   * @param sName service name
   * @param className service class name
   * @param remoteFile location of the file to download
   * @param type type of file (STATIC for a jar or ARCHIVE for a tarball)
   * @param conf configuration
   * @return path of the downloaded file
   * @throws IOException
   */
  private Path maybeDownloadJars(String sName, String className, String
      remoteFile, AuxServiceFile.TypeEnum type, Configuration conf)
      throws IOException {
    // load AuxiliaryService from remote classpath
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
    Path src = new Path(remoteFile);
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
    Path downloadDest = new Path(nmAuxDir,
        className + "_" + scFileStatus.getModificationTime());
    // check whether we need to re-download the jar
    // from remote directory
    Path targetDirPath = new Path(downloadDest,
        scFileStatus.getPath().getName());
    FileStatus[] allSubDirs = localLFS.util().listStatus(nmAuxDir);
    for (FileStatus sub : allSubDirs) {
      if (sub.getPath().getName().equals(downloadDest.getName())) {
        return new Path(targetDirPath + Path.SEPARATOR + "*");
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
    LocalResourceType srcType;
    if (type == AuxServiceFile.TypeEnum.STATIC) {
      srcType = LocalResourceType.FILE;
    } else if (type == AuxServiceFile.TypeEnum.ARCHIVE) {
      srcType = LocalResourceType.ARCHIVE;
    } else {
      throw new YarnRuntimeException(
          "Cannot unpack file of type " + type + " from remote-file-path:" +
              src + "for aux-service:" + ".\n");
    }
    LocalResource scRsrc = LocalResource.newInstance(
        URL.fromURI(src.toUri()),
        srcType, LocalResourceVisibility.PRIVATE,
        scFileStatus.getLen(), scFileStatus.getModificationTime());
    FSDownload download = new FSDownload(localLFS, null, conf,
        downloadDest, scRsrc, null);
    try {
      // don't need to convert downloaded path into a dir
      // since it's already a jar path.
      return download.call();
    } catch (Exception ex) {
      throw new YarnRuntimeException(
          "Exception happend while downloading files "
              + "for aux-service:" + sName + " and remote-file-path:"
              + src + ".\n" + ex.getMessage());
    }
  }

  /**
   * If recovery is enabled, creates a recovery directory for the named
   * service and sets it on the service.
   *
   * @param sName name of the service
   * @param s auxiliary service
   * @throws IOException
   */
  private void setStateStoreDir(String sName, AuxiliaryService s) throws
      IOException {
    if (stateStoreRoot != null) {
      Path storePath = new Path(stateStoreRoot, sName);
      stateStoreFs.mkdirs(storePath, storeDirPerms);
      s.setRecoveryPath(storePath);
    }
  }

  /**
   * Removes a service from the service map and stops it, if it exists.
   *
   * @param sName name of the service
   */
  private synchronized void maybeRemoveAuxService(String sName) {
    AuxiliaryService s;
    s = serviceMap.remove(sName);
    serviceRecordMap.remove(sName);
    serviceMetaData.remove(sName);
    if (s != null) {
      LOG.info("Removing aux service " + sName);
      stopAuxService(s);
    }
  }

  /**
   * Constructs an AuxiliaryService then configures and initializes it based
   * on a service specification.
   *
   * @param service aux service record
   * @param conf configuration
   * @param fromConfiguration true if from configuration, false if from manifest
   * @return aux service
   * @throws IOException
   */
  private AuxiliaryService initAuxService(AuxServiceRecord service,
      Configuration conf, boolean fromConfiguration) throws IOException {
    final String sName = service.getName();
    AuxiliaryService s;
    try {
      Preconditions
          .checkArgument(
              validateAuxServiceName(sName),
              "The auxiliary service name: " + sName + " is invalid. " +
                  "The valid service name should only contain a-zA-Z0-9_ " +
                  "and cannot start with numbers.");
      s = createAuxService(service, conf, fromConfiguration);
      if (s == null) {
        throw new YarnRuntimeException("No auxiliary service class loaded for" +
            " " + sName);
      }
      // TODO better use s.getName()?
      if (!sName.equals(s.getName())) {
        LOG.warn("The Auxiliary Service named '" + sName + "' in the "
            + "configuration is for " + s.getClass() + " which has "
            + "a name of '" + s.getName() + "'. Because these are "
            + "not the same tools trying to send ServiceData and read "
            + "Service Meta Data may have issues unless the refer to "
            + "the name in the config.");
      }
      s.setAuxiliaryLocalPathHandler(auxiliaryLocalPathHandler);
      setStateStoreDir(sName, s);
      Configuration customConf = new Configuration(conf);
      if (service.getConfiguration() != null) {
        for (Entry<String, String> entry : service.getConfiguration()
            .getProperties().entrySet()) {
          customConf.set(entry.getKey(), entry.getValue());
        }
      }
      s.init(customConf);

      LOG.info("Initialized auxiliary service " + sName);
    } catch (RuntimeException e) {
      LOG.error("Failed to initialize " + sName, e);
      throw e;
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(e);
    }
    return s;
  }

  /**
   * Reloads auxiliary services manifest. Must be called after service init.
   *
   * @throws IOException if manifest can't be loaded
   */
  @VisibleForTesting
  protected void reloadManifest() throws IOException {
    loadManifest(getConfig(), true);
  }

  /**
   * Reloads auxiliary services. Must be called after service init.
   *
   * @param services a list of auxiliary services
   * @throws IOException if aux services have not been started yet or dynamic
   * reloading is not enabled
   */
  public synchronized void reload(AuxServiceRecords services) throws
      IOException {
    if (!manifestEnabled) {
      throw new IOException("Dynamic reloading is not enabled via " +
          YarnConfiguration.NM_AUX_SERVICES_MANIFEST_ENABLED);
    }
    if (getServiceState() != Service.STATE.STARTED) {
      throw new IOException("Auxiliary services have not been started yet, " +
          "please retry later");
    }
    LOG.info("Received list of auxiliary services: " + mapper
        .writeValueAsString(services));
    loadServices(services, getConfig(), true);
  }

  private boolean checkManifestPermissions(FileStatus status) throws
      IOException {
    if ((status.getPermission().toShort() & 0022) != 0) {
      LOG.error("Manifest file and parents must not be writable by group or " +
          "others. The current Permission of " + status.getPath() + " is " +
          status.getPermission());
      return false;
    }
    Path parent = status.getPath().getParent();
    if (parent == null) {
      return true;
    }
    return checkManifestPermissions(manifestFS.getFileStatus(parent));
  }

  private boolean checkManifestOwnerAndPermissions(FileStatus status) throws
      IOException {
    AccessControlList yarnAdminAcl = new AccessControlList(getConfig().get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    if (!yarnAdminAcl.isUserAllowed(
        UserGroupInformation.createRemoteUser(status.getOwner()))) {
      LOG.error("Manifest must be owned by YARN admin: " + manifest);
      return false;
    }

    return checkManifestPermissions(status);
  }

  /**
   * Reads the manifest file if it is configured, exists, and has not been
   * modified since the last read.
   *
   * @return aux service records
   * @throws IOException
   */
  private synchronized AuxServiceRecords maybeReadManifestFile() throws
      IOException {
    if (manifest == null) {
      return null;
    }
    if (!manifestFS.exists(manifest)) {
      LOG.warn("Manifest file " + manifest + " doesn't exist");
      return null;
    }
    FileStatus status;
    try {
      status = manifestFS.getFileStatus(manifest);
    } catch (FileNotFoundException e) {
      LOG.warn("Manifest file " + manifest + " doesn't exist");
      return null;
    }
    if (!status.isFile()) {
      LOG.warn("Manifest file " + manifest + " is not a file");
    }
    if (!checkManifestOwnerAndPermissions(status)) {
      return null;
    }
    if (status.getModificationTime() == manifestModifyTS) {
      return null;
    }
    manifestModifyTS = status.getModificationTime();
    LOG.info("Reading auxiliary services manifest " + manifest);
    try (FSDataInputStream in = manifestFS.open(manifest)) {
      return mapper.readValue((InputStream) in, AuxServiceRecords.class);
    }
  }

  /**
   * Updates current aux services based on changes found in the manifest.
   *
   * @param conf configuration
   * @param startServices if true starts services, otherwise only inits services
   * @throws IOException
   */
  @VisibleForTesting
  protected synchronized void loadManifest(Configuration conf, boolean
      startServices) throws IOException {
    if (!manifestEnabled) {
      throw new IOException("Dynamic reloading is not enabled via " +
          YarnConfiguration.NM_AUX_SERVICES_MANIFEST_ENABLED);
    }
    if (manifest == null) {
      return;
    }
    if (!manifestFS.exists(manifest)) {
      if (serviceMap.isEmpty()) {
        return;
      }
      LOG.info("Manifest file " + manifest + " doesn't exist, stopping " +
          "auxiliary services");
      Set<String> servicesToRemove = new HashSet<>(serviceMap.keySet());
      for (String sName : servicesToRemove) {
        maybeRemoveAuxService(sName);
      }
      return;
    }
    AuxServiceRecords services = maybeReadManifestFile();
    loadServices(services, conf, startServices);
  }

  /**
   * Updates current aux services based on changes found in the service list.
   *
   * @param services list of auxiliary services
   * @param conf configuration
   * @param startServices if true starts services, otherwise only inits services
   * @throws IOException
   */
  private synchronized void loadServices(AuxServiceRecords services,
      Configuration conf, boolean startServices) throws IOException {
    if (services == null) {
      // read did not occur or no changes detected
      return;
    }
    Set<String> loadedAuxServices = new HashSet<>();
    boolean foundChanges = false;
    if (services.getServices() != null) {
      for (AuxServiceRecord service : services.getServices()) {
        AuxServiceRecord existingService = serviceRecordMap.get(service
            .getName());
        loadedAuxServices.add(service.getName());
        if (existingService != null && existingService.equals(service)) {
          LOG.debug("Auxiliary service already loaded: {}", service.getName());
          continue;
        }
        foundChanges = true;
        try {
          // stop aux service
          maybeRemoveAuxService(service.getName());
          // init aux service
          AuxiliaryService s = initAuxService(service, conf, false);
          if (startServices) {
            // start aux service
            startAuxService(service.getName(), s, service);
          }
          // add aux service to serviceMap
          addService(service.getName(), s, service);
        } catch (IOException e) {
          LOG.error("Failed to load auxiliary service " + service.getName());
        }
      }
    }

    // remove aux services that do not appear in the new list
    Set<String> servicesToRemove = new HashSet<>(serviceMap.keySet());
    servicesToRemove.removeAll(loadedAuxServices);
    for (String sName : servicesToRemove) {
      foundChanges = true;
      maybeRemoveAuxService(sName);
    }

    if (!foundChanges) {
      LOG.info("No auxiliary services changes detected");
    }
  }

  private static String getClassName(AuxServiceRecord service) {
    AuxServiceConfiguration serviceConf = service.getConfiguration();
    if (serviceConf == null) {
      return null;
    }
    return serviceConf.getProperty(CLASS_NAME);
  }

  private static String[] getSystemClasses(AuxServiceRecord service, String
      className) {
    AuxServiceConfiguration serviceConf =
        service.getConfiguration();
    if (serviceConf == null) {
      return new String[]{className};
    }
    return StringUtils.split(serviceConf.getProperty(SYSTEM_CLASSES,
        className));
  }

  /**
   * Translates an aux service specified in the Configuration to an aux
   * service record.
   *
   * @param sName aux service name
   * @param conf configuration
   * @return
   */
  private static AuxServiceRecord createServiceRecordFromConfiguration(String
      sName, Configuration conf) {
    String className = conf.get(String.format(
        YarnConfiguration.NM_AUX_SERVICE_FMT, sName));
    String remoteClassPath = conf.get(String.format(
        YarnConfiguration.NM_AUX_SERVICE_REMOTE_CLASSPATH, sName));
    String[] systemClasses = conf.getTrimmedStrings(String.format(
        YarnConfiguration.NM_AUX_SERVICES_SYSTEM_CLASSES, sName));

    AuxServiceConfiguration serviceConf = new AuxServiceConfiguration();
    if (className != null) {
      serviceConf.setProperty(CLASS_NAME, className);
    }
    if (systemClasses != null) {
      serviceConf.setProperty(SYSTEM_CLASSES, StringUtils.join(",",
          systemClasses));
    }
    if (remoteClassPath != null) {
      AuxServiceFile.TypeEnum type;
      String lcClassPath = StringUtils.toLowerCase(remoteClassPath);
      if (lcClassPath.endsWith(".jar")) {
        type = AuxServiceFile.TypeEnum.STATIC;
      } else if (lcClassPath.endsWith(".zip") ||
          lcClassPath.endsWith(".tar.gz") || lcClassPath.endsWith(".tgz") ||
          lcClassPath.endsWith(".tar")) {
        type = AuxServiceFile.TypeEnum.ARCHIVE;
      } else {
        throw new YarnRuntimeException("Cannot unpack file from " +
            "remote-file-path:" + remoteClassPath + "for aux-service:" +
            sName + ".\n");
      }
      AuxServiceFile file = new AuxServiceFile().srcFile(remoteClassPath)
          .type(type);
      serviceConf.getFiles().add(file);
    }
    return new AuxServiceRecord().name(sName).configuration(serviceConf);
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    boolean recoveryEnabled = conf.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    if (recoveryEnabled) {
      stateStoreRoot = new Path(conf.get(YarnConfiguration.NM_RECOVERY_DIR),
          STATE_STORE_ROOT_NAME);
      stateStoreFs = FileSystem.getLocal(conf);
    }
    manifestEnabled = conf.getBoolean(
        YarnConfiguration.NM_AUX_SERVICES_MANIFEST_ENABLED,
        YarnConfiguration.DEFAULT_NM_AUX_SERVICES_MANIFEST_ENABLED);
    if (!manifestEnabled) {
      Collection<String> auxNames = conf.getStringCollection(
          YarnConfiguration.NM_AUX_SERVICES);
      for (final String sName : auxNames) {
        AuxServiceRecord service = createServiceRecordFromConfiguration(sName,
            conf);
        maybeRemoveAuxService(sName);
        AuxiliaryService s = initAuxService(service, conf, true);
        addService(sName, s, service);
      }
    } else {
      String manifestStr = conf.get(YarnConfiguration.NM_AUX_SERVICES_MANIFEST);
      if (manifestStr != null) {
        manifest = new Path(manifestStr);
        manifestFS = FileSystem.get(new URI(manifestStr), conf);
        loadManifest(conf, false);
        manifestReloadInterval = conf.getLong(
            YarnConfiguration.NM_AUX_SERVICES_MANIFEST_RELOAD_MS,
            YarnConfiguration.DEFAULT_NM_AUX_SERVICES_MANIFEST_RELOAD_MS);
        manifestReloadTask = new ManifestReloadTask();
      } else {
        LOG.info("Auxiliary services manifest is enabled, but no manifest " +
            "file is specified in the configuration.");
      }
    }

    super.serviceInit(conf);
  }

  private void startAuxService(String name, AuxiliaryService service,
      AuxServiceRecord serviceRecord) {
    service.start();
    service.registerServiceListener(this);
    ByteBuffer meta = service.getMetaData();
    if (meta != null) {
      serviceMetaData.put(name, meta);
    }
    serviceRecord.setLaunchTime(new Date());
  }

  private void stopAuxService(Service service) {
    if (service.getServiceState() == Service.STATE.STARTED) {
      service.unregisterServiceListener(this);
      service.stop();
    }
  }

  @Override
  public synchronized void serviceStart() throws Exception {
    // TODO fork(?) services running as configured user
    //      monitor for health, shutdown/restart(?) if any should die
    for (Map.Entry<String, AuxiliaryService> entry : serviceMap.entrySet()) {
      AuxiliaryService service = entry.getValue();
      String name = entry.getKey();
      startAuxService(name, service, serviceRecordMap.get(name));
    }
    if (manifestEnabled && manifest != null && manifestReloadInterval > 0) {
      LOG.info("Scheduling reloading auxiliary services manifest file at " +
          "interval " + manifestReloadInterval + " ms");
      manifestReloadTimer = new Timer("AuxServicesManifestReload-Timer",
          true);
      manifestReloadTimer.schedule(manifestReloadTask,
          manifestReloadInterval, manifestReloadInterval);
    }
    super.serviceStart();
  }

  @Override
  public synchronized void serviceStop() throws Exception {
    try {
      for (Service service : serviceMap.values()) {
        stopAuxService(service);
      }
      serviceMap.clear();
      serviceRecordMap.clear();
      serviceMetaData.clear();
      if (manifestFS != null) {
        manifestFS.close();
      }
      if (manifestReloadTimer != null) {
        manifestReloadTimer.cancel();
      }
    } finally {
      super.serviceStop();
    }
  }

  @Override
  public void stateChanged(Service service) {
    // services changing state is expected on reload
    LOG.info("Service " + service.getName() + " changed state: " +
        service.getServiceState());
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

  protected static AuxServiceRecord newAuxService(String name, String
      className) {
    AuxServiceConfiguration serviceConf = new AuxServiceConfiguration();
    serviceConf.setProperty(CLASS_NAME, className);
    return new AuxServiceRecord().name(name).configuration(serviceConf);
  }

  protected static void setClasspath(AuxServiceRecord service, String
      classpath) {
    service.getConfiguration().getFiles().add(new AuxServiceFile()
        .srcFile(classpath).type(AuxServiceFile.TypeEnum.STATIC));
  }

  protected static void setSystemClasses(AuxServiceRecord service, String
      systemClasses) {
    service.getConfiguration().setProperty(SYSTEM_CLASSES, systemClasses);
  }

  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * manifest reload.
   */
  private final class ManifestReloadTask extends TimerTask {
    @Override
    public void run() {
      try {
        reloadManifest();
      } catch (Throwable t) {
        // Prevent uncaught exceptions from killing this thread
        LOG.warn("Error while reloading manifest: ", t);
      }
    }
  }
}
