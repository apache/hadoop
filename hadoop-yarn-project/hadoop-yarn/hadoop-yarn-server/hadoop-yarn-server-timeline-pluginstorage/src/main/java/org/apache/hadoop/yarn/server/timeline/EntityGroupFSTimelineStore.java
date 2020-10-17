/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timeline;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.util.Apps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.MalformedURLException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin timeline storage to support timeline server v1.5 API. This storage
 * uses a file system to store timeline entities in their groups.
 */
public class EntityGroupFSTimelineStore extends CompositeService
    implements TimelineStore {

  static final String DOMAIN_LOG_PREFIX = "domainlog-";
  static final String SUMMARY_LOG_PREFIX = "summarylog-";
  static final String ENTITY_LOG_PREFIX = "entitylog-";

  static final String ATS_V15_SERVER_DFS_CALLER_CTXT = "yarn_ats_server_v1_5";

  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);
  private static final FsPermission ACTIVE_DIR_PERMISSION =
      new FsPermission((short) 01777);
  private static final FsPermission DONE_DIR_PERMISSION =
      new FsPermission((short) 0700);

  // Active dir: <activeRoot>/appId/attemptId/cacheId.log
  // Done dir: <doneRoot>/cluster_ts/hash1/hash2/appId/attemptId/cacheId.log
  private static final String APP_DONE_DIR_PREFIX_FORMAT =
      "%d" + Path.SEPARATOR     // cluster timestamp
          + "%04d" + Path.SEPARATOR // app num / 1,000,000
          + "%03d" + Path.SEPARATOR // (app num / 1000) % 1000
          + "%s" + Path.SEPARATOR; // full app id
  // Indicates when to force release a cache item even if there are active
  // readers. Enlarge this factor may increase memory usage for the reader since
  // there may be more cache items "hanging" in memory but not in cache.
  private static final int CACHE_ITEM_OVERFLOW_FACTOR = 2;

  private YarnClient yarnClient;
  private TimelineStore summaryStore;
  private TimelineACLsManager aclManager;
  private TimelineDataManager summaryTdm;
  private ConcurrentMap<ApplicationId, AppLogs> appIdLogMap =
      new ConcurrentHashMap<ApplicationId, AppLogs>();
  private ScheduledThreadPoolExecutor executor;
  private AtomicBoolean stopExecutors = new AtomicBoolean(false);
  private FileSystem fs;
  private ObjectMapper objMapper;
  private JsonFactory jsonFactory;
  private Path activeRootPath;
  private Path doneRootPath;
  private long logRetainMillis;
  private long unknownActiveMillis;
  private int appCacheMaxSize = 0;
  private List<TimelineEntityGroupPlugin> cacheIdPlugins;
  private Map<TimelineEntityGroupId, EntityCacheItem> cachedLogs;

  @VisibleForTesting
  @InterfaceAudience.Private
  EntityGroupFSTimelineStoreMetrics metrics;

  public EntityGroupFSTimelineStore() {
    super(EntityGroupFSTimelineStore.class.getSimpleName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    metrics = EntityGroupFSTimelineStoreMetrics.create();
    summaryStore = createSummaryStore();
    addService(summaryStore);

    long logRetainSecs = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETAIN_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_RETAIN_SECONDS_DEFAULT);
    logRetainMillis = logRetainSecs * 1000;
    LOG.info("Cleaner set to delete logs older than {} seconds", logRetainSecs);
    long unknownActiveSecs = conf.getLong(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS,
        YarnConfiguration.
            TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_UNKNOWN_ACTIVE_SECONDS_DEFAULT
    );
    unknownActiveMillis = unknownActiveSecs * 1000;
    LOG.info("Unknown apps will be treated as complete after {} seconds",
        unknownActiveSecs);
    appCacheMaxSize = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_APP_CACHE_SIZE_DEFAULT);
    LOG.info("Application cache size is {}", appCacheMaxSize);
    cachedLogs = Collections.synchronizedMap(
      new LinkedHashMap<TimelineEntityGroupId, EntityCacheItem>(
          appCacheMaxSize + 1, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(
              Map.Entry<TimelineEntityGroupId, EntityCacheItem> eldest) {
            if (super.size() > appCacheMaxSize) {
              TimelineEntityGroupId groupId = eldest.getKey();
              LOG.debug("Evicting {} due to space limitations", groupId);
              EntityCacheItem cacheItem = eldest.getValue();
              LOG.debug("Force release cache {}.", groupId);
              cacheItem.forceRelease();
              if (cacheItem.getAppLogs().isDone()) {
                appIdLogMap.remove(groupId.getApplicationId());
              }
              metrics.incrCacheEvicts();
              return true;
            }
            return false;
          }
      });
    cacheIdPlugins = loadPlugIns(conf);
    // Initialize yarn client for application status
    yarnClient = createAndInitYarnClient(conf);
    // if non-null, hook its lifecycle up
    addIfService(yarnClient);
    activeRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR_DEFAULT));
    doneRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT));
    fs = activeRootPath.getFileSystem(conf);
    CallerContext.setCurrent(
        new CallerContext.Builder(ATS_V15_SERVER_DFS_CALLER_CTXT).build());
    super.serviceInit(conf);
  }

  private List<TimelineEntityGroupPlugin> loadPlugIns(Configuration conf)
      throws RuntimeException {
    Collection<String> pluginNames = conf.getTrimmedStringCollection(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES);

    String pluginClasspath = conf.getTrimmed(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSPATH);
    String[] systemClasses = conf.getTrimmedStrings(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_SYSTEM_CLASSES);

    List<TimelineEntityGroupPlugin> pluginList
        = new LinkedList<TimelineEntityGroupPlugin>();
    ClassLoader customClassLoader = null;
    if (pluginClasspath != null && pluginClasspath.length() > 0) {
      try {
        customClassLoader = createPluginClassLoader(pluginClasspath,
            systemClasses);
      } catch (IOException ioe) {
        LOG.warn("Error loading classloader", ioe);
      }
    }
    for (final String name : pluginNames) {
      LOG.debug("Trying to load plugin class {}", name);
      TimelineEntityGroupPlugin cacheIdPlugin = null;

      try {
        if (customClassLoader != null) {
          LOG.debug("Load plugin {} with classpath: {}", name, pluginClasspath);
          Class<?> clazz = Class.forName(name, true, customClassLoader);
          Class<? extends TimelineEntityGroupPlugin> sClass = clazz.asSubclass(
              TimelineEntityGroupPlugin.class);
          cacheIdPlugin = ReflectionUtils.newInstance(sClass, conf);
        } else {
          LOG.debug("Load plugin class with system classpath");
          Class<?> clazz = conf.getClassByName(name);
          cacheIdPlugin =
              (TimelineEntityGroupPlugin) ReflectionUtils.newInstance(
                  clazz, conf);
        }
      } catch (Exception e) {
        LOG.warn("Error loading plugin " + name, e);
        throw new RuntimeException("No class defined for " + name, e);
      }

      LOG.info("Load plugin class {}", cacheIdPlugin.getClass().getName());
      pluginList.add(cacheIdPlugin);
    }
    return pluginList;
  }

  private TimelineStore createSummaryStore() {
    return ReflectionUtils.newInstance(getConfig().getClass(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_STORE,
        LeveldbTimelineStore.class, TimelineStore.class), getConfig());
  }

  @Override
  protected void serviceStart() throws Exception {

    super.serviceStart();
    LOG.info("Starting {}", getName());
    summaryStore.start();

    Configuration conf = getConfig();
    aclManager = new TimelineACLsManager(conf);
    aclManager.setTimelineStore(summaryStore);
    summaryTdm = new TimelineDataManager(summaryStore, aclManager);
    summaryTdm.init(conf);
    addService(summaryTdm);
    // start child services that aren't already started
    super.serviceStart();

    if (!fs.exists(activeRootPath)) {
      fs.mkdirs(activeRootPath);
      fs.setPermission(activeRootPath, ACTIVE_DIR_PERMISSION);
    }
    if (!fs.exists(doneRootPath)) {
      fs.mkdirs(doneRootPath);
      fs.setPermission(doneRootPath, DONE_DIR_PERMISSION);
    }

    objMapper = new ObjectMapper();
    objMapper.setAnnotationIntrospector(
        new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
    jsonFactory = new MappingJsonFactory(objMapper);
    final long scanIntervalSecs = conf.getLong(
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SCAN_INTERVAL_SECONDS_DEFAULT
    );
    final long cleanerIntervalSecs = conf.getLong(
        YarnConfiguration
          .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CLEANER_INTERVAL_SECONDS,
        YarnConfiguration
          .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_CLEANER_INTERVAL_SECONDS_DEFAULT
    );
    final int numThreads = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_THREADS,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_THREADS_DEFAULT);
    LOG.info("Scanning active directory {} every {} seconds", activeRootPath,
        scanIntervalSecs);
    LOG.info("Cleaning logs every {} seconds", cleanerIntervalSecs);

    executor = new ScheduledThreadPoolExecutor(numThreads,
        new ThreadFactoryBuilder().setNameFormat("EntityLogPluginWorker #%d")
            .build());
    executor.scheduleAtFixedRate(new EntityLogScanner(), 0, scanIntervalSecs,
        TimeUnit.SECONDS);
    executor.scheduleAtFixedRate(new EntityLogCleaner(), cleanerIntervalSecs,
        cleanerIntervalSecs, TimeUnit.SECONDS);
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping {}", getName());
    stopExecutors.set(true);
    if (executor != null) {
      executor.shutdown();
      if (executor.isTerminating()) {
        LOG.info("Waiting for executor to terminate");
        boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        if (terminated) {
          LOG.info("Executor terminated");
        } else {
          LOG.warn("Executor did not terminate");
          executor.shutdownNow();
        }
      }
    }
    synchronized (cachedLogs) {
      for (EntityCacheItem cacheItem : cachedLogs.values()) {
        ServiceOperations.stopQuietly(cacheItem.getStore());
      }
    }
    CallerContext.setCurrent(null);
    super.serviceStop();
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  int scanActiveLogs() throws IOException {
    long startTime = Time.monotonicNow();
    int logsToScanCount = scanActiveLogs(activeRootPath);
    metrics.addActiveLogDirScanTime(Time.monotonicNow() - startTime);
    return logsToScanCount;
  }

  int scanActiveLogs(Path dir) throws IOException {
    RemoteIterator<FileStatus> iter = list(dir);
    int logsToScanCount = 0;
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      String name = stat.getPath().getName();
      ApplicationId appId = parseApplicationId(name);
      if (appId != null) {
        LOG.debug("scan logs for {} in {}", appId, stat.getPath());
        logsToScanCount++;
        AppLogs logs = getAndSetActiveLog(appId, stat.getPath());
        executor.execute(new ActiveLogParser(logs));
      } else {
        if (stat.isDirectory()) {
          logsToScanCount += scanActiveLogs(stat.getPath());
        } else {
          LOG.warn("Ignoring unexpected file in active directory {}",
              stat.getPath());
        }
      }
    }
    return logsToScanCount;
  }

  /**
   * List a directory, returning an iterator which will fail fast if this
   * service has been stopped
   * @param path path to list
   * @return an iterator over the contents of the directory
   * @throws IOException
   */
  private RemoteIterator<FileStatus> list(Path path) throws IOException {
    return new StoppableRemoteIterator(fs.listStatusIterator(path));
  }

  private AppLogs createAndPutAppLogsIfAbsent(ApplicationId appId,
      Path appDirPath, AppState appState) {
    AppLogs appLogs = new AppLogs(appId, appDirPath, appState);
    AppLogs oldAppLogs = appIdLogMap.putIfAbsent(appId, appLogs);
    if (oldAppLogs != null) {
      appLogs = oldAppLogs;
    }
    return appLogs;
  }

  private AppLogs getAndSetActiveLog(ApplicationId appId, Path appDirPath) {
    AppLogs appLogs = appIdLogMap.get(appId);
    if (appLogs == null) {
      appLogs = createAndPutAppLogsIfAbsent(appId, appDirPath, AppState.ACTIVE);
    }
    return appLogs;
  }

  // searches for the app logs and returns it if found else null
  private AppLogs getAndSetAppLogs(ApplicationId applicationId)
      throws IOException {
    LOG.debug("Looking for app logs mapped for app id {}", applicationId);
    AppLogs appLogs = appIdLogMap.get(applicationId);
    if (appLogs == null) {
      AppState appState = AppState.UNKNOWN;
      Path appDirPath = getDoneAppPath(applicationId);
      if (fs.exists(appDirPath)) {
        appState = AppState.COMPLETED;
      } else {
        appDirPath = getActiveAppPath(applicationId);
        if (fs.exists(appDirPath)) {
          appState = AppState.ACTIVE;
        } else {
          // check for user directory inside active path
          RemoteIterator<FileStatus> iter = list(activeRootPath);
          while (iter.hasNext()) {
            Path child = new Path(iter.next().getPath().getName(),
                applicationId.toString());
            appDirPath = new Path(activeRootPath, child);
            if (fs.exists(appDirPath)) {
              appState = AppState.ACTIVE;
              break;
            }
          }
        }
      }
      if (appState != AppState.UNKNOWN) {
        LOG.debug("Create and try to add new appLogs to appIdLogMap for {}",
            applicationId);
        appLogs = createAndPutAppLogsIfAbsent(
            applicationId, appDirPath, appState);
      }
    }
    return appLogs;
  }

  /**
   * Main function for entity log cleaner. This method performs depth first
   * search from a given dir path for all application log dirs. Once found, it
   * will decide if the directory should be cleaned up and then clean them.
   *
   * @param dirpath the root directory the cleaner should start with. Note that
   *                dirpath should be a directory that contains a set of
   *                application log directories. The cleaner method will not
   *                work if the given dirpath itself is an application log dir.
   * @param retainMillis
   * @throws IOException
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  void cleanLogs(Path dirpath, long retainMillis)
      throws IOException {
    long now = Time.now();
    RemoteIterator<FileStatus> iter = list(dirpath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      if (isValidClusterTimeStampDir(stat)) {
        Path clusterTimeStampPath = stat.getPath();
        MutableBoolean appLogDirPresent = new MutableBoolean(false);
        cleanAppLogDir(clusterTimeStampPath, retainMillis, appLogDirPresent);
        if (appLogDirPresent.isFalse() &&
            (now - stat.getModificationTime() > retainMillis)) {
          deleteDir(clusterTimeStampPath);
        }
      }
    }
  }


  private void cleanAppLogDir(Path dirpath, long retainMillis,
      MutableBoolean appLogDirPresent) throws IOException {
    long now = Time.now();
    // Depth first search from root directory for all application log dirs
    RemoteIterator<FileStatus> iter = list(dirpath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      Path childPath = stat.getPath();
      if (stat.isDirectory()) {
        // If current is an application log dir, decide if we need to remove it
        // and remove if necessary.
        // Otherwise, keep iterating into it.
        ApplicationId appId = parseApplicationId(childPath.getName());
        if (appId != null) { // Application log dir
          appLogDirPresent.setTrue();
          if (shouldCleanAppLogDir(childPath, now, fs, retainMillis)) {
            deleteDir(childPath);
          }
        } else { // Keep cleaning inside
          cleanAppLogDir(childPath, retainMillis, appLogDirPresent);
        }
      }
    }
  }

  private void deleteDir(Path path) {
    try {
      LOG.info("Deleting {}", path);
      if (fs.delete(path, true)) {
        metrics.incrLogsDirsCleaned();
      } else {
        LOG.error("Unable to remove {}", path);
      }
    } catch (IOException e) {
      LOG.error("Unable to remove {}", path, e);
    }
  }

  private boolean isValidClusterTimeStampDir(FileStatus stat) {
    return stat.isDirectory() &&
        StringUtils.isNumeric(stat.getPath().getName());
  }


  private static boolean shouldCleanAppLogDir(Path appLogPath, long now,
      FileSystem fs, long logRetainMillis) throws IOException {
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(appLogPath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      if (now - stat.getModificationTime() <= logRetainMillis) {
        // found a dir entry that is fresh enough to prevent
        // cleaning this directory.
        LOG.debug("{} not being cleaned due to {}", appLogPath, stat.getPath());
        return false;
      }
      // Otherwise, keep searching files inside for directories.
      if (stat.isDirectory()) {
        if (!shouldCleanAppLogDir(stat.getPath(), now, fs, logRetainMillis)) {
          return false;
        }
      }
    }
    return true;
  }

  // converts the String to an ApplicationId or null if conversion failed
  private static ApplicationId parseApplicationId(String appIdStr) {
    try {
      return ApplicationId.fromString(appIdStr);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static ClassLoader createPluginClassLoader(
      final String appClasspath, final String[] systemClasses)
      throws IOException {
    try {
      return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws MalformedURLException {
            return new ApplicationClassLoader(appClasspath,
                EntityGroupFSTimelineStore.class.getClassLoader(),
                Arrays.asList(systemClasses));
          }
        }
      );
    } catch (PrivilegedActionException e) {
      Throwable t = e.getCause();
      if (t instanceof MalformedURLException) {
        throw (MalformedURLException) t;
      }
      throw new IOException(e);
    }
  }

  private Path getActiveAppPath(ApplicationId appId) {
    return new Path(activeRootPath, appId.toString());
  }

  private Path getDoneAppPath(ApplicationId appId) {
    // cut up the app ID into mod(1000) buckets
    int appNum = appId.getId();
    appNum /= 1000;
    int bucket2 = appNum % 1000;
    int bucket1 = appNum / 1000;
    return new Path(doneRootPath,
        String.format(APP_DONE_DIR_PREFIX_FORMAT, appId.getClusterTimestamp(),
            bucket1, bucket2, appId.toString()));
  }

  /**
   * Create and initialize the YARN Client. Tests may override/mock this.
   * If they return null, then {@link #getAppState(ApplicationId)} MUST
   * also be overridden
   * @param conf configuration
   * @return the yarn client, or null.
   *
   */
  @VisibleForTesting
  protected YarnClient createAndInitYarnClient(Configuration conf) {
    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    return client;
  }

  /**
   * Get the application state.
   * @param appId application ID
   * @return the state or {@link AppState#UNKNOWN} if it could not
   * be determined
   * @throws IOException on IO problems
   */
  @VisibleForTesting
  protected AppState getAppState(ApplicationId appId) throws IOException {
    return getAppState(appId, yarnClient);
  }

  /**
   * Get all plugins for tests.
   * @return all plugins
   */
  @VisibleForTesting
  List<TimelineEntityGroupPlugin> getPlugins() {
    return cacheIdPlugins;
  }

  /**
   * Ask the RM for the state of the application.
   * This method has to be synchronized to control traffic to RM
   * @param appId application ID
   * @param yarnClient
   * @return the state or {@link AppState#UNKNOWN} if it could not
   * be determined
   * @throws IOException
   */
  private static synchronized AppState getAppState(ApplicationId appId,
      YarnClient yarnClient) throws IOException {
    AppState appState = AppState.ACTIVE;
    try {
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      if (Apps.isApplicationFinalState(report.getYarnApplicationState())) {
        appState = AppState.COMPLETED;
      }
    } catch (ApplicationNotFoundException e) {
      appState = AppState.UNKNOWN;
    } catch (YarnException e) {
      throw new IOException(e);
    }
    return appState;
  }

  /**
   * Application states,
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  public enum AppState {
    ACTIVE,
    UNKNOWN,
    COMPLETED
  }

  class AppLogs {
    private ApplicationId appId;
    private Path appDirPath;
    private AppState appState;
    private List<LogInfo> summaryLogs = new ArrayList<LogInfo>();
    private List<LogInfo> detailLogs = new ArrayList<LogInfo>();

    public AppLogs(ApplicationId appId, Path appPath, AppState state) {
      this.appId = appId;
      appDirPath = appPath;
      appState = state;
    }

    public synchronized boolean isDone() {
      return appState == AppState.COMPLETED;
    }

    public synchronized ApplicationId getAppId() {
      return appId;
    }

    public synchronized Path getAppDirPath() {
      return appDirPath;
    }

    synchronized List<LogInfo> getSummaryLogs() {
      return summaryLogs;
    }

    synchronized List<LogInfo> getDetailLogs() {
      return detailLogs;
    }

    public synchronized void parseSummaryLogs() throws IOException {
      parseSummaryLogs(summaryTdm);
    }

    @InterfaceAudience.Private
    @VisibleForTesting
    synchronized void parseSummaryLogs(TimelineDataManager tdm)
        throws IOException {
      long startTime = Time.monotonicNow();
      if (!isDone()) {
        LOG.debug("Try to parse summary log for log {} in {}",
            appId, appDirPath);
        appState = getAppState(appId);
        long recentLogModTime = scanForLogs();
        if (appState == AppState.UNKNOWN) {
          if (Time.now() - recentLogModTime > unknownActiveMillis) {
            LOG.info(
                "{} state is UNKNOWN and logs are stale, assuming COMPLETED",
                appId);
            appState = AppState.COMPLETED;
          }
        }
      }
      List<LogInfo> removeList = new ArrayList<LogInfo>();
      for (LogInfo log : summaryLogs) {
        if (fs.exists(log.getPath(appDirPath))) {
          long summaryEntityParsed
              = log.parseForStore(tdm, appDirPath, isDone(), jsonFactory,
              objMapper, fs);
          metrics.incrEntitiesReadToSummary(summaryEntityParsed);
        } else {
          // The log may have been removed, remove the log
          removeList.add(log);
          LOG.info("File {} no longer exists, remove it from log list",
              log.getPath(appDirPath));
        }
      }
      summaryLogs.removeAll(removeList);
      metrics.addSummaryLogReadTime(Time.monotonicNow() - startTime);
    }

    // scans for new logs and returns the modification timestamp of the
    // most recently modified log
    @InterfaceAudience.Private
    @VisibleForTesting
    long scanForLogs() throws IOException {
      LOG.debug("scanForLogs on {}", appDirPath);
      long newestModTime = 0;
      RemoteIterator<FileStatus> iterAttempt = list(appDirPath);
      while (iterAttempt.hasNext()) {
        FileStatus statAttempt = iterAttempt.next();
        LOG.debug("scanForLogs on {}", statAttempt.getPath().getName());
        if (!statAttempt.isDirectory()
            || !statAttempt.getPath().getName()
            .startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
          LOG.debug("Scanner skips for unknown dir/file {}",
              statAttempt.getPath());
          continue;
        }
        String attemptDirName = statAttempt.getPath().getName();
        RemoteIterator<FileStatus> iterCache = list(statAttempt.getPath());
        while (iterCache.hasNext()) {
          FileStatus statCache = iterCache.next();
          if (!statCache.isFile()) {
            continue;
          }
          String filename = statCache.getPath().getName();
          // We should only update time for log files.
          boolean shouldSetTime = true;
          LOG.debug("scan for log file: {}", filename);
          if (filename.startsWith(DOMAIN_LOG_PREFIX)) {
            addSummaryLog(attemptDirName, filename, statCache.getOwner(), true);
          } else if (filename.startsWith(SUMMARY_LOG_PREFIX)) {
            addSummaryLog(attemptDirName, filename, statCache.getOwner(),
                false);
          } else if (filename.startsWith(ENTITY_LOG_PREFIX)) {
            addDetailLog(attemptDirName, filename, statCache.getOwner());
          } else {
            shouldSetTime = false;
          }
          if (shouldSetTime) {
            newestModTime
              = Math.max(statCache.getModificationTime(), newestModTime);
          }
        }
      }

      // if there are no logs in the directory then use the modification
      // time of the directory itself
      if (newestModTime == 0) {
        newestModTime = fs.getFileStatus(appDirPath).getModificationTime();
      }

      return newestModTime;
    }

    private void addSummaryLog(String attemptDirName,
        String filename, String owner, boolean isDomainLog) {
      for (LogInfo log : summaryLogs) {
        if (log.getFilename().equals(filename)
            && log.getAttemptDirName().equals(attemptDirName)) {
          return;
        }
      }
      LOG.debug("Incoming log {} not present in my summaryLogs list, add it",
          filename);
      LogInfo log;
      if (isDomainLog) {
        log = new DomainLogInfo(attemptDirName, filename, owner);
      } else {
        log = new EntityLogInfo(attemptDirName, filename, owner);
      }
      summaryLogs.add(log);
    }

    private synchronized void addDetailLog(String attemptDirName,
        String filename, String owner) {
      for (LogInfo log : detailLogs) {
        if (log.getFilename().equals(filename)
            && log.getAttemptDirName().equals(attemptDirName)) {
          return;
        }
      }
      detailLogs.add(new EntityLogInfo(attemptDirName, filename, owner));
    }

    synchronized void loadDetailLog(TimelineDataManager tdm,
        TimelineEntityGroupId groupId) throws IOException {
      List<LogInfo> removeList = new ArrayList<>();
      for (LogInfo log : detailLogs) {
        LOG.debug("Try refresh logs for {}", log.getFilename());
        // Only refresh the log that matches the cache id
        if (log.matchesGroupId(groupId)) {
          Path dirPath = getAppDirPath();
          if (fs.exists(log.getPath(dirPath))) {
            LOG.debug("Refresh logs for cache id {}", groupId);
            log.parseForStore(tdm, dirPath, isDone(),
                jsonFactory, objMapper, fs);
          } else {
            // The log may have been removed, remove the log
            removeList.add(log);
            LOG.info(
                "File {} no longer exists, removing it from log list",
                log.getPath(dirPath));
          }
        }
      }
      detailLogs.removeAll(removeList);
    }

    public synchronized void moveToDone() throws IOException {
      Path doneAppPath = getDoneAppPath(appId);
      if (!doneAppPath.equals(appDirPath)) {
        Path donePathParent = doneAppPath.getParent();
        if (!fs.exists(donePathParent)) {
          fs.mkdirs(donePathParent);
        }
        LOG.debug("Application {} is done, trying to move to done dir {}",
            appId, doneAppPath);
        if (!fs.rename(appDirPath, doneAppPath)) {
          throw new IOException("Rename " + appDirPath + " to " + doneAppPath
              + " failed");
        } else {
          LOG.info("Moved {} to {}", appDirPath, doneAppPath);
        }
        appDirPath = doneAppPath;
      }
    }
  }

  /**
   * Extract any nested throwable forwarded from IPC operations.
   * @param e exception
   * @return either the exception passed an an argument, or any nested
   * exception which was wrapped inside an {@link UndeclaredThrowableException}
   */
  private Throwable extract(Exception e) {
    Throwable t = e;
    if (e instanceof UndeclaredThrowableException && e.getCause() != null) {
      t = e.getCause();
    }
    return t;
  }

  private class EntityLogScanner implements Runnable {
    @Override
    public void run() {
      LOG.debug("Active scan starting");
      try {
        int scanned = scanActiveLogs();
        LOG.debug("Scanned {} active applications", scanned);
      } catch (Exception e) {
        Throwable t = extract(e);
        if (t instanceof InterruptedException) {
          LOG.info("File scanner interrupted");
        } else {
          LOG.error("Error scanning active files", t);
        }
      }
      LOG.debug("Active scan complete");
    }
  }

  private class ActiveLogParser implements Runnable {
    private AppLogs appLogs;

    public ActiveLogParser(AppLogs logs) {
      appLogs = logs;
    }

    @Override
    public void run() {
      try {
        LOG.debug("Begin parsing summary logs. ");
        appLogs.parseSummaryLogs();
        if (appLogs.isDone()) {
          appLogs.moveToDone();
          appIdLogMap.remove(appLogs.getAppId());
        }
        LOG.debug("End parsing summary logs. ");
      } catch (Exception e) {
        Throwable t = extract(e);
        if (t instanceof InterruptedException) {
          LOG.info("Log parser interrupted");
        } else {
          LOG.error("Error processing logs for " + appLogs.getAppId(), t);
        }
      }
    }
  }

  private class EntityLogCleaner implements Runnable {
    @Override
    public void run() {
      LOG.debug("Cleaner starting");
      long startTime = Time.monotonicNow();
      try {
        cleanLogs(doneRootPath, logRetainMillis);
      } catch (Exception e) {
        Throwable t = extract(e);
        if (t instanceof InterruptedException) {
          LOG.info("Cleaner interrupted");
        } else {
          LOG.error("Error cleaning files", e);
        }
      } finally {
        metrics.addLogCleanTime(Time.monotonicNow() - startTime);
      }
      LOG.debug("Cleaner finished");
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  void setFs(FileSystem incomingFs) {
    this.fs = incomingFs;
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  void setCachedLogs(TimelineEntityGroupId groupId, EntityCacheItem cacheItem) {
    cachedLogs.put(groupId, cacheItem);
  }

  private List<TimelineStore> getTimelineStoresFromCacheIds(
      Set<TimelineEntityGroupId> groupIds, String entityType,
      List<EntityCacheItem> cacheItems)
      throws IOException {
    List<TimelineStore> stores = new LinkedList<TimelineStore>();
    // For now we just handle one store in a context. We return the first
    // non-null storage for the group ids.
    for (TimelineEntityGroupId groupId : groupIds) {
      TimelineStore storeForId = getCachedStore(groupId, cacheItems);
      if (storeForId != null) {
        LOG.debug("Adding {} as a store for the query", storeForId.getName());
        stores.add(storeForId);
        metrics.incrGetEntityToDetailOps();
      }
    }
    if (stores.size() == 0) {
      LOG.debug("Using summary store for {}", entityType);
      stores.add(this.summaryStore);
      metrics.incrGetEntityToSummaryOps();
    }
    return stores;
  }

  protected List<TimelineStore> getTimelineStoresForRead(String entityId,
      String entityType, List<EntityCacheItem> cacheItems)
      throws IOException {
    Set<TimelineEntityGroupId> groupIds = new HashSet<TimelineEntityGroupId>();
    for (TimelineEntityGroupPlugin cacheIdPlugin : cacheIdPlugins) {
      LOG.debug("Trying plugin {} for id {} and type {}",
          cacheIdPlugin.getClass().getName(), entityId, entityType);
      Set<TimelineEntityGroupId> idsFromPlugin
          = cacheIdPlugin.getTimelineEntityGroupId(entityId, entityType);
      if (idsFromPlugin == null) {
        LOG.debug("Plugin returned null " + cacheIdPlugin.getClass().getName());
      } else {
        LOG.debug("Plugin returned ids: " + idsFromPlugin);
      }

      if (idsFromPlugin != null) {
        groupIds.addAll(idsFromPlugin);
        LOG.debug("plugin {} returns a non-null value on query",
            cacheIdPlugin.getClass().getName());
      }
    }
    return getTimelineStoresFromCacheIds(groupIds, entityType, cacheItems);
  }

  private List<TimelineStore> getTimelineStoresForRead(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      List<EntityCacheItem> cacheItems) throws IOException {
    Set<TimelineEntityGroupId> groupIds = new HashSet<TimelineEntityGroupId>();
    for (TimelineEntityGroupPlugin cacheIdPlugin : cacheIdPlugins) {
      Set<TimelineEntityGroupId> idsFromPlugin =
          cacheIdPlugin.getTimelineEntityGroupId(entityType, primaryFilter,
              secondaryFilters);
      if (idsFromPlugin != null) {
        LOG.debug("plugin {} returns a non-null value on query {}",
            cacheIdPlugin.getClass().getName(), idsFromPlugin);
        groupIds.addAll(idsFromPlugin);
      }
    }
    return getTimelineStoresFromCacheIds(groupIds, entityType, cacheItems);
  }

  // find a cached timeline store or null if it cannot be located
  private TimelineStore getCachedStore(TimelineEntityGroupId groupId,
      List<EntityCacheItem> cacheItems) throws IOException {
    EntityCacheItem cacheItem;
    synchronized (this.cachedLogs) {
      // Note that the content in the cache log storage may be stale.
      cacheItem = this.cachedLogs.get(groupId);
      if (cacheItem == null) {
        LOG.debug("Set up new cache item for id {}", groupId);
        cacheItem = new EntityCacheItem(groupId, getConfig());
        AppLogs appLogs = getAndSetAppLogs(groupId.getApplicationId());
        if (appLogs != null) {
          LOG.debug("Set applogs {} for group id {}", appLogs, groupId);
          cacheItem.setAppLogs(appLogs);
          this.cachedLogs.put(groupId, cacheItem);
        } else {
          LOG.warn("AppLogs for groupId {} is set to null!", groupId);
        }
      }
    }
    TimelineStore store = null;
    if (cacheItem.getAppLogs() != null) {
      AppLogs appLogs = cacheItem.getAppLogs();
      LOG.debug("try refresh cache {} {}", groupId, appLogs.getAppId());
      cacheItems.add(cacheItem);
      store = cacheItem.refreshCache(aclManager, metrics);
    } else {
      LOG.warn("AppLogs for group id {} is null", groupId);
    }
    return store;
  }

  @Override
  public TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fieldsToRetrieve, CheckAcl checkAcl) throws IOException {
    LOG.debug("getEntities type={} primary={}", entityType, primaryFilter);
    List<EntityCacheItem> relatedCacheItems = new ArrayList<>();
    List<TimelineStore> stores = getTimelineStoresForRead(entityType,
        primaryFilter, secondaryFilters, relatedCacheItems);
    TimelineEntities returnEntities = new TimelineEntities();
    for (TimelineStore store : stores) {
      LOG.debug("Try timeline store {} for the request", store.getName());
      TimelineEntities entities = store.getEntities(entityType, limit,
          windowStart, windowEnd, fromId, fromTs, primaryFilter,
          secondaryFilters, fieldsToRetrieve, checkAcl);
      if (entities != null) {
        returnEntities.addEntities(entities.getEntities());
      }
    }
    return returnEntities;
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    LOG.debug("getEntity type={} id={}", entityType, entityId);
    List<EntityCacheItem> relatedCacheItems = new ArrayList<>();
    List<TimelineStore> stores = getTimelineStoresForRead(entityId, entityType,
        relatedCacheItems);
    for (TimelineStore store : stores) {
      LOG.debug("Try timeline store {}:{} for the request", store.getName(),
          store.toString());
      TimelineEntity e =
          store.getEntity(entityId, entityType, fieldsToRetrieve);
      if (e != null) {
        return e;
      }
    }
    LOG.debug("getEntity: Found nothing");
    return null;
  }

  @Override
  public TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventTypes) throws IOException {
    LOG.debug("getEntityTimelines type={} ids={}", entityType, entityIds);
    TimelineEvents returnEvents = new TimelineEvents();
    List<EntityCacheItem> relatedCacheItems = new ArrayList<>();

    if (entityIds == null || entityIds.isEmpty()) {
      return returnEvents;
    }

    for (String entityId : entityIds) {
      LOG.debug("getEntityTimeline type={} id={}", entityType, entityId);
      List<TimelineStore> stores
          = getTimelineStoresForRead(entityId, entityType, relatedCacheItems);
      for (TimelineStore store : stores) {
        LOG.debug("Try timeline store {}:{} for the request", store.getName(),
            store.toString());
        SortedSet<String> entityIdSet = new TreeSet<>();
        entityIdSet.add(entityId);
        TimelineEvents events =
            store.getEntityTimelines(entityType, entityIdSet, limit,
                windowStart, windowEnd, eventTypes);
        if (events != null) {
          returnEvents.addEvents(events.getAllEvents());
        }
      }
    }
    return returnEvents;
  }

  @Override
  public TimelineDomain getDomain(String domainId) throws IOException {
    return summaryStore.getDomain(domainId);
  }

  @Override
  public TimelineDomains getDomains(String owner) throws IOException {
    return summaryStore.getDomains(owner);
  }

  @Override
  public TimelinePutResponse put(TimelineEntities data) throws IOException {
    return summaryStore.put(data);
  }

  @Override
  public void put(TimelineDomain domain) throws IOException {
    summaryStore.put(domain);
  }

  /**
   * This is a special remote iterator whose {@link #hasNext()} method
   * returns false if {@link #stopExecutors} is true.
   *
   * This provides an implicit shutdown of all iterative file list and scan
   * operations without needing to implement it in the while loops themselves.
   */
  private class StoppableRemoteIterator implements RemoteIterator<FileStatus> {
    private final RemoteIterator<FileStatus> remote;

    public StoppableRemoteIterator(RemoteIterator<FileStatus> remote) {
      this.remote = remote;
    }

    @Override
    public boolean hasNext() throws IOException {
      return !stopExecutors.get() && remote.hasNext();
    }

    @Override
    public FileStatus next() throws IOException {
      return remote.next();
    }
  }
}
