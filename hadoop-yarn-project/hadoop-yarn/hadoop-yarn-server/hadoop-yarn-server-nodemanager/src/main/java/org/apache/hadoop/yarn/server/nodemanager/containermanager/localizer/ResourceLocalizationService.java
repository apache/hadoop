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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerBuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.FSDownload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ResourceLocalizationService extends CompositeService
    implements EventHandler<LocalizationEvent>, LocalizationProtocol {

  private static final Log LOG = LogFactory.getLog(ResourceLocalizationService.class);
  public static final String NM_PRIVATE_DIR = "nmPrivate";
  public static final FsPermission NM_PRIVATE_PERM = new FsPermission((short) 0700);

  private Server server;
  private InetSocketAddress localizationServerAddress;
  private long cacheTargetSize;
  private long cacheCleanupPeriod;

  private final ContainerExecutor exec;
  protected final Dispatcher dispatcher;
  private final DeletionService delService;
  private LocalizerTracker localizerTracker;
  private RecordFactory recordFactory;
  private final ScheduledExecutorService cacheCleanup;
  private LocalizerTokenSecretManager secretManager;

  private LocalResourcesTracker publicRsrc;

  private LocalDirsHandlerService dirsHandler;

  /**
   * Map of LocalResourceTrackers keyed by username, for private
   * resources.
   */
  private final ConcurrentMap<String,LocalResourcesTracker> privateRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();

  /**
   * Map of LocalResourceTrackers keyed by appid, for application
   * resources.
   */
  private final ConcurrentMap<String,LocalResourcesTracker> appRsrc =
    new ConcurrentHashMap<String,LocalResourcesTracker>();

  public ResourceLocalizationService(Dispatcher dispatcher,
      ContainerExecutor exec, DeletionService delService,
      LocalDirsHandlerService dirsHandler) {

    super(ResourceLocalizationService.class.getName());
    this.exec = exec;
    this.dispatcher = dispatcher;
    this.delService = delService;
    this.dirsHandler = dirsHandler;

    this.cacheCleanup = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
          .setNameFormat("ResourceLocalizationService Cache Cleanup")
          .build());
  }

  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  private void validateConf(Configuration conf) {
    int perDirFileLimit =
        conf.getInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
          YarnConfiguration.DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY);
    if (perDirFileLimit <= 36) {
      LOG.error(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
          + " parameter is configured with very low value.");
      throw new YarnRuntimeException(
        YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
            + " parameter is configured with a value less than 37.");
    } else {
      LOG.info("per directory file limit = " + perDirFileLimit);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.validateConf(conf);
    this.publicRsrc =
        new LocalResourcesTrackerImpl(null, dispatcher, true, conf);
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);

    try {
      FileContext lfs = getLocalFileContext(conf);
      lfs.setUMask(new FsPermission((short)FsPermission.DEFAULT_UMASK));

      cleanUpLocalDir(lfs,delService);

      List<String> localDirs = dirsHandler.getLocalDirs();
      for (String localDir : localDirs) {
        // $local/usercache
        Path userDir = new Path(localDir, ContainerLocalizer.USERCACHE);
        lfs.mkdir(userDir, null, true);
        // $local/filecache
        Path fileDir = new Path(localDir, ContainerLocalizer.FILECACHE);
        lfs.mkdir(fileDir, null, true);
        // $local/nmPrivate
        Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
        lfs.mkdir(sysDir, NM_PRIVATE_PERM, true);
      }

      List<String> logDirs = dirsHandler.getLogDirs();
      for (String logDir : logDirs) {
        lfs.mkdir(new Path(logDir), null, true);
      }
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to initialize LocalizationService", e);
    }

    cacheTargetSize =
      conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_TARGET_SIZE_MB, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_TARGET_SIZE_MB) << 20;
    cacheCleanupPeriod =
      conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS);
    localizationServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_PORT);

    localizerTracker = createLocalizerTracker(conf);
    addService(localizerTracker);
    dispatcher.register(LocalizerEventType.class, localizerTracker);
    super.serviceInit(conf);
  }

  @Override
  public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status) {
    return localizerTracker.processHeartbeat(status);
  }

  @Override
  public void serviceStart() throws Exception {
    cacheCleanup.scheduleWithFixedDelay(new CacheCleanup(dispatcher),
        cacheCleanupPeriod, cacheCleanupPeriod, TimeUnit.MILLISECONDS);
    server = createServer();
    server.start();
    localizationServerAddress =
        getConfig().updateConnectAddr(YarnConfiguration.NM_LOCALIZER_ADDRESS,
                                      server.getListenerAddress());
    LOG.info("Localizer started on port " + server.getPort());
    super.serviceStart();
  }

  LocalizerTracker createLocalizerTracker(Configuration conf) {
    return new LocalizerTracker(conf);
  }

  Server createServer() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      secretManager = new LocalizerTokenSecretManager();      
    }
    
    Server server = rpc.getServer(LocalizationProtocol.class, this,
        localizationServerAddress, conf, secretManager, 
        conf.getInt(YarnConfiguration.NM_LOCALIZER_CLIENT_THREAD_COUNT, 
            YarnConfiguration.DEFAULT_NM_LOCALIZER_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      server.refreshServiceAcl(conf, new NMPolicyProvider());
    }
    
    return server;
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    cacheCleanup.shutdown();
    super.serviceStop();
  }

  @Override
  public void handle(LocalizationEvent event) {
    // TODO: create log dir as $logdir/$user/$appId
    switch (event.getType()) {
    case INIT_APPLICATION_RESOURCES:
      handleInitApplicationResources(
          ((ApplicationLocalizationEvent)event).getApplication());
      break;
    case INIT_CONTAINER_RESOURCES:
      handleInitContainerResources((ContainerLocalizationRequestEvent) event);
      break;
    case CACHE_CLEANUP:
      handleCacheCleanup(event);
      break;
    case CLEANUP_CONTAINER_RESOURCES:
      handleCleanupContainerResources((ContainerLocalizationCleanupEvent)event);
      break;
    case DESTROY_APPLICATION_RESOURCES:
      handleDestroyApplicationResources(
          ((ApplicationLocalizationEvent)event).getApplication());
      break;
    default:
      throw new YarnRuntimeException("Unknown localization event: " + event);
    }
  }
  
  /**
   * Handle event received the first time any container is scheduled
   * by a given application.
   */
  @SuppressWarnings("unchecked")
  private void handleInitApplicationResources(Application app) {
    // 0) Create application tracking structs
    String userName = app.getUser();
    privateRsrc.putIfAbsent(userName, new LocalResourcesTrackerImpl(userName,
      dispatcher, true, super.getConfig()));
    if (null != appRsrc.putIfAbsent(
      ConverterUtils.toString(app.getAppId()),
      new LocalResourcesTrackerImpl(app.getUser(), dispatcher, false, super
        .getConfig()))) {
      LOG.warn("Initializing application " + app + " already present");
      assert false; // TODO: FIXME assert doesn't help
                    // ^ The condition is benign. Tests should fail and it
                    // should appear in logs, but it's an internal error
                    // that should have no effect on applications
    }
    // 1) Signal container init
    //
    // This is handled by the ApplicationImpl state machine and allows
    // containers to proceed with launching.
    dispatcher.getEventHandler().handle(new ApplicationInitedEvent(
          app.getAppId()));
  }
  
  /**
   * For each of the requested resources for a container, determines the
   * appropriate {@link LocalResourcesTracker} and forwards a 
   * {@link LocalResourceRequest} to that tracker.
   */
  private void handleInitContainerResources(
      ContainerLocalizationRequestEvent rsrcReqs) {
    Container c = rsrcReqs.getContainer();
    // create a loading cache for the file statuses
    LoadingCache<Path,Future<FileStatus>> statCache =
        CacheBuilder.newBuilder().build(FSDownload.createStatusCacheLoader(getConfig()));
    LocalizerContext ctxt = new LocalizerContext(
        c.getUser(), c.getContainerId(), c.getCredentials(), statCache);
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
      rsrcReqs.getRequestedResources();
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e :
         rsrcs.entrySet()) {
      LocalResourcesTracker tracker =
          getLocalResourcesTracker(e.getKey(), c.getUser(),
              c.getContainerId().getApplicationAttemptId()
                  .getApplicationId());
      for (LocalResourceRequest req : e.getValue()) {
        tracker.handle(new ResourceRequestEvent(req, e.getKey(), ctxt));
      }
    }
  }
  
  private void handleCacheCleanup(LocalizationEvent event) {
    ResourceRetentionSet retain =
      new ResourceRetentionSet(delService, cacheTargetSize);
    retain.addResources(publicRsrc);
    LOG.debug("Resource cleanup (public) " + retain);
    for (LocalResourcesTracker t : privateRsrc.values()) {
      retain.addResources(t);
      LOG.debug("Resource cleanup " + t.getUser() + ":" + retain);
    }
    //TODO Check if appRsrcs should also be added to the retention set.
  }


  @SuppressWarnings("unchecked")
  private void handleCleanupContainerResources(
      ContainerLocalizationCleanupEvent rsrcCleanup) {
    Container c = rsrcCleanup.getContainer();
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
      rsrcCleanup.getResources();
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e :
         rsrcs.entrySet()) {
      LocalResourcesTracker tracker = getLocalResourcesTracker(e.getKey(), c.getUser(), 
          c.getContainerId().getApplicationAttemptId()
          .getApplicationId());
      for (LocalResourceRequest req : e.getValue()) {
        tracker.handle(new ResourceReleaseEvent(req,
            c.getContainerId()));
      }
    }
    String locId = ConverterUtils.toString(c.getContainerId());
    localizerTracker.cleanupPrivLocalizers(locId);
    
    // Delete the container directories
    String userName = c.getUser();
    String containerIDStr = c.toString();
    String appIDStr = ConverterUtils.toString(
        c.getContainerId().getApplicationAttemptId().getApplicationId());
    for (String localDir : dirsHandler.getLocalDirs()) {

      // Delete the user-owned container-dir
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(allAppsdir, appIDStr);
      Path containerDir = new Path(appDir, containerIDStr);
      delService.delete(userName, containerDir, new Path[] {});

      // Delete the nmPrivate container-dir
      
      Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
      Path appSysDir = new Path(sysDir, appIDStr);
      Path containerSysDir = new Path(appSysDir, containerIDStr);
      delService.delete(null, containerSysDir,  new Path[] {});
    }

    dispatcher.getEventHandler().handle(
        new ContainerEvent(c.getContainerId(),
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
  }


  @SuppressWarnings({"unchecked"})
  private void handleDestroyApplicationResources(Application application) {
    String userName;
    String appIDStr;
    LocalResourcesTracker appLocalRsrcsTracker =
      appRsrc.remove(ConverterUtils.toString(application.getAppId()));
    if (null == appLocalRsrcsTracker) {
      LOG.warn("Removing uninitialized application " + application);
    }
    // TODO: What to do with appLocalRsrcsTracker?

    // Delete the application directories
    userName = application.getUser();
    appIDStr = application.toString();
    for (String localDir : dirsHandler.getLocalDirs()) {

      // Delete the user-owned app-dir
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(allAppsdir, appIDStr);
      delService.delete(userName, appDir, new Path[] {});

      // Delete the nmPrivate app-dir
      Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
      Path appSysDir = new Path(sysDir, appIDStr);
      delService.delete(null, appSysDir, new Path[] {});
    }

    // TODO: decrement reference counts of all resources associated with this
    // app

    dispatcher.getEventHandler().handle(new ApplicationEvent(
          application.getAppId(),
          ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
  }


  LocalResourcesTracker getLocalResourcesTracker(
      LocalResourceVisibility visibility, String user, ApplicationId appId) {
    switch (visibility) {
      default:
      case PUBLIC:
        return publicRsrc;
      case PRIVATE:
        return privateRsrc.get(user);
      case APPLICATION:
        return appRsrc.get(ConverterUtils.toString(appId));
    }
  }

  private String getUserFileCachePath(String user) {
    return StringUtils.join(Path.SEPARATOR, Arrays.asList(".",
      ContainerLocalizer.USERCACHE, user, ContainerLocalizer.FILECACHE));

  }

  private String getAppFileCachePath(String user, String appId) {
    return StringUtils.join(Path.SEPARATOR, Arrays.asList(".",
      ContainerLocalizer.USERCACHE, user, ContainerLocalizer.APPCACHE, appId,
      ContainerLocalizer.FILECACHE));
  }
  
  @VisibleForTesting
  @Private
  public PublicLocalizer getPublicLocalizer() {
    return localizerTracker.publicLocalizer;
  }

  @VisibleForTesting
  @Private
  public LocalizerRunner getLocalizerRunner(String locId) {
    return localizerTracker.privLocalizers.get(locId);
  }
  
  @VisibleForTesting
  @Private
  public Map<String, LocalizerRunner> getPrivateLocalizers() {
    return localizerTracker.privLocalizers;
  }
  
  /**
   * Sub-component handling the spawning of {@link ContainerLocalizer}s
   */
  class LocalizerTracker extends AbstractService implements EventHandler<LocalizerEvent>  {

    private final PublicLocalizer publicLocalizer;
    private final Map<String,LocalizerRunner> privLocalizers;

    LocalizerTracker(Configuration conf) {
      this(conf, new HashMap<String,LocalizerRunner>());
    }

    LocalizerTracker(Configuration conf,
        Map<String,LocalizerRunner> privLocalizers) {
      super(LocalizerTracker.class.getName());
      this.publicLocalizer = new PublicLocalizer(conf);
      this.privLocalizers = privLocalizers;
    }
    
    @Override
    public synchronized void serviceStart() throws Exception {
      publicLocalizer.start();
      super.serviceStart();
    }

    public LocalizerHeartbeatResponse processHeartbeat(LocalizerStatus status) {
      String locId = status.getLocalizerId();
      synchronized (privLocalizers) {
        LocalizerRunner localizer = privLocalizers.get(locId);
        if (null == localizer) {
          // TODO process resources anyway
          LOG.info("Unknown localizer with localizerId " + locId
              + " is sending heartbeat. Ordering it to DIE");
          LocalizerHeartbeatResponse response =
            recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
          response.setLocalizerAction(LocalizerAction.DIE);
          return response;
        }
        return localizer.update(status.getResources());
      }
    }
    
    @Override
    public void serviceStop() throws Exception {
      for (LocalizerRunner localizer : privLocalizers.values()) {
        localizer.interrupt();
      }
      publicLocalizer.interrupt();
      super.serviceStop();
    }

    @Override
    public void handle(LocalizerEvent event) {
      String locId = event.getLocalizerId();
      switch (event.getType()) {
      case REQUEST_RESOURCE_LOCALIZATION:
        // 0) find running localizer or start new thread
        LocalizerResourceRequestEvent req =
          (LocalizerResourceRequestEvent)event;
        switch (req.getVisibility()) {
        case PUBLIC:
          publicLocalizer.addResource(req);
          break;
        case PRIVATE:
        case APPLICATION:
          synchronized (privLocalizers) {
            LocalizerRunner localizer = privLocalizers.get(locId);
            if (null == localizer) {
              LOG.info("Created localizer for " + locId);
              localizer = new LocalizerRunner(req.getContext(), locId);
              privLocalizers.put(locId, localizer);
              localizer.start();
            }
            // 1) propagate event
            localizer.addResource(req);
          }
          break;
        }
        break;
      }
    }

    public void cleanupPrivLocalizers(String locId) {
      synchronized (privLocalizers) {
        LocalizerRunner localizer = privLocalizers.get(locId);
        if (null == localizer) {
          return; // ignore; already gone
        }
        privLocalizers.remove(locId);
        localizer.interrupt();
      }
    }
  }
  

  private static ExecutorService createLocalizerExecutor(Configuration conf) {
    int nThreads = conf.getInt(
        YarnConfiguration.NM_LOCALIZER_FETCH_THREAD_COUNT,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_FETCH_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("PublicLocalizer #%d")
      .build();
    return Executors.newFixedThreadPool(nThreads, tf);
  }


  class PublicLocalizer extends Thread {

    final FileContext lfs;
    final Configuration conf;
    final ExecutorService threadPool;
    final CompletionService<Path> queue;
    // Its shared between public localizer and dispatcher thread.
    final Map<Future<Path>,LocalizerResourceRequestEvent> pending;

    PublicLocalizer(Configuration conf) {
      super("Public Localizer");
      this.lfs = getLocalFileContext(conf);
      this.conf = conf;
      this.pending = Collections.synchronizedMap(
          new HashMap<Future<Path>, LocalizerResourceRequestEvent>());
      this.threadPool = createLocalizerExecutor(conf);
      this.queue = new ExecutorCompletionService<Path>(threadPool);
    }

    public void addResource(LocalizerResourceRequestEvent request) {
      // TODO handle failures, cancellation, requests by other containers
      LocalizedResource rsrc = request.getResource();
      LocalResourceRequest key = rsrc.getRequest();
      LOG.info("Downloading public rsrc:" + key);
      /*
       * Here multiple containers may request the same resource. So we need
       * to start downloading only when
       * 1) ResourceState == DOWNLOADING
       * 2) We are able to acquire non blocking semaphore lock.
       * If not we will skip this resource as either it is getting downloaded
       * or it FAILED / LOCALIZED.
       */

      if (rsrc.tryAcquire()) {
        if (rsrc.getState().equals(ResourceState.DOWNLOADING)) {
          LocalResource resource = request.getResource().getRequest();
          try {
            Path publicDirDestPath =
                dirsHandler.getLocalPathForWrite("." + Path.SEPARATOR
                    + ContainerLocalizer.FILECACHE,
                  ContainerLocalizer.getEstimatedSize(resource), true);
            Path hierarchicalPath =
                publicRsrc.getPathForLocalization(key, publicDirDestPath);
            if (!hierarchicalPath.equals(publicDirDestPath)) {
              publicDirDestPath = hierarchicalPath;
              DiskChecker.checkDir(new File(publicDirDestPath.toUri().getPath()));
            }
            publicDirDestPath =
                new Path(publicDirDestPath, Long.toString(publicRsrc
                  .nextUniqueNumber()));
            // explicitly synchronize pending here to avoid future task
            // completing and being dequeued before pending updated
            synchronized (pending) {
              pending.put(queue.submit(new FSDownload(lfs, null, conf,
                  publicDirDestPath, resource, request.getContext().getStatCache())),
                  request);
            }
          } catch (IOException e) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
              .getResource().getRequest(), e.getMessage()));
            LOG.error("Local path for public localization is not found. "
                + " May be disks failed.", e);
          } catch (RejectedExecutionException re) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
              .getResource().getRequest(), re.getMessage()));
            LOG.error("Failed to submit rsrc " + rsrc + " for download."
                + " Either queue is full or threadpool is shutdown.", re);
          }
        } else {
          rsrc.unlock();
        }
      }
    }

    @Override
    public void run() {
      try {
        // TODO shutdown, better error handling esp. DU
        while (!Thread.currentThread().isInterrupted()) {
          try {
            Future<Path> completed = queue.take();
            LocalizerResourceRequestEvent assoc = pending.remove(completed);
            try {
              Path local = completed.get();
              if (null == assoc) {
                LOG.error("Localized unkonwn resource to " + completed);
                // TODO delete
                return;
              }
              LocalResourceRequest key = assoc.getResource().getRequest();
              publicRsrc.handle(new ResourceLocalizedEvent(key, local, FileUtil
                .getDU(new File(local.toUri()))));
              assoc.getResource().unlock();
            } catch (ExecutionException e) {
              LOG.info("Failed to download rsrc " + assoc.getResource(),
                  e.getCause());
              LocalResourceRequest req = assoc.getResource().getRequest();
              publicRsrc.handle(new ResourceFailedLocalizationEvent(req,
                  e.getMessage()));
              assoc.getResource().unlock();
            } catch (CancellationException e) {
              // ignore; shutting down
            }
          } catch (InterruptedException e) {
            return;
          }
        }
      } catch(Throwable t) {
        LOG.fatal("Error: Shutting down", t);
      } finally {
        LOG.info("Public cache exiting");
        threadPool.shutdownNow();
      }
    }

  }

  /**
   * Runs the {@link ContainerLocalizer} itself in a separate process with
   * access to user's credentials. One {@link LocalizerRunner} per localizerId.
   * 
   */
  class LocalizerRunner extends Thread {

    final LocalizerContext context;
    final String localizerId;
    final Map<LocalResourceRequest,LocalizerResourceRequestEvent> scheduled;
    // Its a shared list between Private Localizer and dispatcher thread.
    final List<LocalizerResourceRequestEvent> pending;

    // TODO: threadsafe, use outer?
    private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(getConfig());

    LocalizerRunner(LocalizerContext context, String localizerId) {
      super("LocalizerRunner for " + localizerId);
      this.context = context;
      this.localizerId = localizerId;
      this.pending =
          Collections
            .synchronizedList(new ArrayList<LocalizerResourceRequestEvent>());
      this.scheduled =
          new HashMap<LocalResourceRequest, LocalizerResourceRequestEvent>();
    }

    public void addResource(LocalizerResourceRequestEvent request) {
      pending.add(request);
    }

    /**
     * Find next resource to be given to a spawned localizer.
     * 
     * @return
     */
    private LocalResource findNextResource() {
      synchronized (pending) {
        for (Iterator<LocalizerResourceRequestEvent> i = pending.iterator();
            i.hasNext();) {
         LocalizerResourceRequestEvent evt = i.next();
         LocalizedResource nRsrc = evt.getResource();
         // Resource download should take place ONLY if resource is in
         // Downloading state
         if (!ResourceState.DOWNLOADING.equals(nRsrc.getState())) {
           i.remove();
           continue;
         }
         /*
          * Multiple containers will try to download the same resource. So the
          * resource download should start only if
          * 1) We can acquire a non blocking semaphore lock on resource
          * 2) Resource is still in DOWNLOADING state
          */
         if (nRsrc.tryAcquire()) {
           if (nRsrc.getState().equals(ResourceState.DOWNLOADING)) {
             LocalResourceRequest nextRsrc = nRsrc.getRequest();
             LocalResource next =
                 recordFactory.newRecordInstance(LocalResource.class);
             next.setResource(ConverterUtils.getYarnUrlFromPath(nextRsrc
               .getPath()));
             next.setTimestamp(nextRsrc.getTimestamp());
             next.setType(nextRsrc.getType());
             next.setVisibility(evt.getVisibility());
             next.setPattern(evt.getPattern());
             scheduled.put(nextRsrc, evt);
             return next;
           } else {
             // Need to release acquired lock
             nRsrc.unlock();
           }
         }
       }
       return null;
      }
    }

    LocalizerHeartbeatResponse update(
        List<LocalResourceStatus> remoteResourceStatuses) {
      LocalizerHeartbeatResponse response =
        recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);

      String user = context.getUser();
      ApplicationId applicationId =
          context.getContainerId().getApplicationAttemptId().getApplicationId();
      // The localizer has just spawned. Start giving it resources for
      // remote-fetching.
      if (remoteResourceStatuses.isEmpty()) {
        LocalResource next = findNextResource();
        if (next != null) {
          response.setLocalizerAction(LocalizerAction.LIVE);
          try {
            ArrayList<ResourceLocalizationSpec> rsrcs =
                new ArrayList<ResourceLocalizationSpec>();
            ResourceLocalizationSpec rsrc =
                NodeManagerBuilderUtils.newResourceLocalizationSpec(next,
                  getPathForLocalization(next));
            rsrcs.add(rsrc);
            response.setResourceSpecs(rsrcs);
          } catch (IOException e) {
            LOG.error("local path for PRIVATE localization could not be found."
                + "Disks might have failed.", e);
          } catch (URISyntaxException e) {
            // TODO fail? Already translated several times...
          }
        } else if (pending.isEmpty()) {
          // TODO: Synchronization
          response.setLocalizerAction(LocalizerAction.DIE);
        } else {
          response.setLocalizerAction(LocalizerAction.LIVE);
        }
        return response;
      }
      ArrayList<ResourceLocalizationSpec> rsrcs =
          new ArrayList<ResourceLocalizationSpec>();
       /*
        * TODO : It doesn't support multiple downloads per ContainerLocalizer
        * at the same time. We need to think whether we should support this.
        */

      for (LocalResourceStatus stat : remoteResourceStatuses) {
        LocalResource rsrc = stat.getResource();
        LocalResourceRequest req = null;
        try {
          req = new LocalResourceRequest(rsrc);
        } catch (URISyntaxException e) {
          // TODO fail? Already translated several times...
        }
        LocalizerResourceRequestEvent assoc = scheduled.get(req);
        if (assoc == null) {
          // internal error
          LOG.error("Unknown resource reported: " + req);
          continue;
        }
        switch (stat.getStatus()) {
          case FETCH_SUCCESS:
            // notify resource
            try {
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(
                new ResourceLocalizedEvent(req, ConverterUtils
                  .getPathFromYarnURL(stat.getLocalPath()), stat.getLocalSize()));
            } catch (URISyntaxException e) { }

            // unlocking the resource and removing it from scheduled resource
            // list
            assoc.getResource().unlock();
            scheduled.remove(req);
            
            if (pending.isEmpty()) {
              // TODO: Synchronization
              response.setLocalizerAction(LocalizerAction.DIE);
              break;
            }
            response.setLocalizerAction(LocalizerAction.LIVE);
            LocalResource next = findNextResource();
            if (next != null) {
              try {
                ResourceLocalizationSpec resource =
                    NodeManagerBuilderUtils.newResourceLocalizationSpec(next,
                      getPathForLocalization(next));
                rsrcs.add(resource);
              } catch (IOException e) {
                LOG.error("local path for PRIVATE localization could not be " +
                  "found. Disks might have failed.", e);
              } catch (URISyntaxException e) {
                  //TODO fail? Already translated several times...
              }
            }
            break;
          case FETCH_PENDING:
            response.setLocalizerAction(LocalizerAction.LIVE);
            break;
          case FETCH_FAILURE:
            LOG.info("DEBUG: FAILED " + req 
                + ", " + stat.getException().getMessage());
            response.setLocalizerAction(LocalizerAction.DIE);
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, stat.getException().getMessage()));

            // unlocking the resource and removing it from scheduled resource
            // list
            assoc.getResource().unlock();
            scheduled.remove(req);
            
            break;
          default:
            LOG.info("Unknown status: " + stat.getStatus());
            response.setLocalizerAction(LocalizerAction.DIE);
            getLocalResourcesTracker(req.getVisibility(), user, applicationId)
              .handle(new ResourceFailedLocalizationEvent(
                  req, stat.getException().getMessage()));
            break;
        }
      }
      response.setResourceSpecs(rsrcs);
      return response;
    }

    private Path getPathForLocalization(LocalResource rsrc) throws IOException,
        URISyntaxException {
      String user = context.getUser();
      ApplicationId appId =
          context.getContainerId().getApplicationAttemptId().getApplicationId();
      LocalResourceVisibility vis = rsrc.getVisibility();
      LocalResourcesTracker tracker =
          getLocalResourcesTracker(vis, user, appId);
      String cacheDirectory = null;
      if (vis == LocalResourceVisibility.PRIVATE) {// PRIVATE Only
        cacheDirectory = getUserFileCachePath(user);
      } else {// APPLICATION ONLY
        cacheDirectory = getAppFileCachePath(user, appId.toString());
      }
      Path dirPath =
          dirsHandler.getLocalPathForWrite(cacheDirectory,
            ContainerLocalizer.getEstimatedSize(rsrc), false);
      dirPath = tracker.getPathForLocalization(new LocalResourceRequest(rsrc),
        dirPath);
      return new Path (dirPath, Long.toString(tracker.nextUniqueNumber()));
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      Path nmPrivateCTokensPath = null;
      try {
        // Get nmPrivateDir
        nmPrivateCTokensPath =
          dirsHandler.getLocalPathForWrite(
                NM_PRIVATE_DIR + Path.SEPARATOR
                    + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
                        localizerId));

        // 0) init queue, etc.
        // 1) write credentials to private dir
        writeCredentials(nmPrivateCTokensPath);
        // 2) exec initApplication and wait
        List<String> localDirs = dirsHandler.getLocalDirs();
        List<String> logDirs = dirsHandler.getLogDirs();
        if (dirsHandler.areDisksHealthy()) {
          exec.startLocalizer(nmPrivateCTokensPath, localizationServerAddress,
              context.getUser(),
              ConverterUtils.toString(
                  context.getContainerId().
                  getApplicationAttemptId().getApplicationId()),
              localizerId, localDirs, logDirs);
        } else {
          throw new IOException("All disks failed. "
              + dirsHandler.getDisksHealthReport());
        }
      // TODO handle ExitCodeException separately?
      } catch (Exception e) {
        LOG.info("Localizer failed", e);
        // 3) on error, report failure to Container and signal ABORT
        // 3.1) notify resource of failed localization
        ContainerId cId = context.getContainerId();
        dispatcher.getEventHandler().handle(
            new ContainerResourceFailedEvent(cId, null, e.getMessage()));
      } finally {
        for (LocalizerResourceRequestEvent event : scheduled.values()) {
          event.getResource().unlock();
        }
        delService.delete(null, nmPrivateCTokensPath, new Path[] {});
      }
    }

    private void writeCredentials(Path nmPrivateCTokensPath)
        throws IOException {
      DataOutputStream tokenOut = null;
      try {
        Credentials credentials = context.getCredentials();
        FileContext lfs = getLocalFileContext(getConfig());
        tokenOut =
            lfs.create(nmPrivateCTokensPath, EnumSet.of(CREATE, OVERWRITE));
        LOG.info("Writing credentials to the nmPrivate file "
            + nmPrivateCTokensPath.toString() + ". Credentials list: ");
        if (LOG.isDebugEnabled()) {
          for (Token<? extends TokenIdentifier> tk : credentials
              .getAllTokens()) {
            LOG.debug(tk.getService() + " : " + tk.encodeToUrlString());
          }
        }
        if (UserGroupInformation.isSecurityEnabled()) {
          credentials = new Credentials(credentials);
          LocalizerTokenIdentifier id = secretManager.createIdentifier();
          Token<LocalizerTokenIdentifier> localizerToken =
              new Token<LocalizerTokenIdentifier>(id, secretManager);
          credentials.addToken(id.getKind(), localizerToken);
        }
        credentials.writeTokenStorageToStream(tokenOut);
      } finally {
        if (tokenOut != null) {
          tokenOut.close();
        }
      }
    }

  }

  static class CacheCleanup extends Thread {

    private final Dispatcher dispatcher;

    public CacheCleanup(Dispatcher dispatcher) {
      super("CacheCleanup");
      this.dispatcher = dispatcher;
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      dispatcher.getEventHandler().handle(
          new LocalizationEvent(LocalizationEventType.CACHE_CLEANUP));
    }

  }

  private void cleanUpLocalDir(FileContext lfs, DeletionService del) {
    long currentTimeStamp = System.currentTimeMillis();
    for (String localDir : dirsHandler.getLocalDirs()) {
      renameLocalDir(lfs, localDir, ContainerLocalizer.USERCACHE,
          currentTimeStamp);
      renameLocalDir(lfs, localDir, ContainerLocalizer.FILECACHE,
          currentTimeStamp);
      renameLocalDir(lfs, localDir, ResourceLocalizationService.NM_PRIVATE_DIR,
          currentTimeStamp);
      try {
        deleteLocalDir(lfs, del, localDir);
      } catch (IOException e) {
        // Do nothing, just give the warning
        LOG.warn("Failed to delete localDir: " + localDir);
      }
    }
  }

  private void renameLocalDir(FileContext lfs, String localDir,
      String localSubDir, long currentTimeStamp) {
    try {
      lfs.rename(new Path(localDir, localSubDir), new Path(
          localDir, localSubDir + "_DEL_" + currentTimeStamp));
    } catch (FileNotFoundException ex) {
      // No need to handle this exception
      // localSubDir may not be exist
    } catch (Exception ex) {
      // Do nothing, just give the warning
      LOG.warn("Failed to rename the local file under " +
          localDir + "/" + localSubDir);
    }
  }

  private void deleteLocalDir(FileContext lfs, DeletionService del,
      String localDir) throws IOException {
    RemoteIterator<FileStatus> fileStatus = lfs.listStatus(new Path(localDir));
    if (fileStatus != null) {
      while (fileStatus.hasNext()) {
        FileStatus status = fileStatus.next();
        try {
          if (status.getPath().getName().matches(".*" +
              ContainerLocalizer.USERCACHE + "_DEL_.*")) {
            LOG.info("usercache path : " + status.getPath().toString());
            cleanUpFilesPerUserDir(lfs, del, status.getPath());
          } else if (status.getPath().getName()
              .matches(".*" + NM_PRIVATE_DIR + "_DEL_.*")
              ||
              status.getPath().getName()
                  .matches(".*" + ContainerLocalizer.FILECACHE + "_DEL_.*")) {
            del.delete(null, status.getPath(), new Path[] {});
          }
        } catch (IOException ex) {
          // Do nothing, just give the warning
          LOG.warn("Failed to delete this local Directory: " +
              status.getPath().getName());
        }
      }
    }
  }

  private void cleanUpFilesPerUserDir(FileContext lfs, DeletionService del,
      Path userDirPath) throws IOException {
    RemoteIterator<FileStatus> userDirStatus = lfs.listStatus(userDirPath);
    FileDeletionTask dependentDeletionTask =
        del.createFileDeletionTask(null, userDirPath, new Path[] {});
    if (userDirStatus != null) {
      List<FileDeletionTask> deletionTasks = new ArrayList<FileDeletionTask>();
      while (userDirStatus.hasNext()) {
        FileStatus status = userDirStatus.next();
        String owner = status.getOwner();
        FileDeletionTask deletionTask =
            del.createFileDeletionTask(owner, null,
              new Path[] { status.getPath() });
        deletionTask.addFileDeletionTaskDependency(dependentDeletionTask);
        deletionTasks.add(deletionTask);
      }
      for (FileDeletionTask task : deletionTasks) {
        del.scheduleFileDeletionTask(task);
      }
    } else {
      del.scheduleFileDeletionTask(dependentDeletionTask);
    }
  }

}
