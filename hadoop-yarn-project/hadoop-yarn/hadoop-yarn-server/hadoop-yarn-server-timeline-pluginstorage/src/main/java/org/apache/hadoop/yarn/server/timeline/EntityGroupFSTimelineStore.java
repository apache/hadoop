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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

/**
 * Plugin timeline storage to support timeline server v1.5 API. This storage
 * uses a file system to store timeline entities in their groups.
 */
public class EntityGroupFSTimelineStore extends AbstractService
    implements TimelineStore {

  static final String DOMAIN_LOG_PREFIX = "domainlog-";
  static final String SUMMARY_LOG_PREFIX = "summarylog-";
  static final String ENTITY_LOG_PREFIX = "entitylog-";

  private static final Logger LOG = LoggerFactory.getLogger(
      EntityGroupFSTimelineStore.class);
  private static final FsPermission ACTIVE_DIR_PERMISSION =
      new FsPermission((short) 01777);
  private static final FsPermission DONE_DIR_PERMISSION =
      new FsPermission((short) 0700);

  private static final EnumSet<YarnApplicationState>
      APP_FINAL_STATES = EnumSet.of(
      YarnApplicationState.FAILED,
      YarnApplicationState.KILLED,
      YarnApplicationState.FINISHED);
  // Active dir: <activeRoot>/appId/attemptId/cacheId.log
  // Done dir: <doneRoot>/cluster_ts/hash1/hash2/appId/attemptId/cacheId.log
  private static final String APP_DONE_DIR_PREFIX_FORMAT =
      "%d" + Path.SEPARATOR     // cluster timestamp
          + "%04d" + Path.SEPARATOR // app num / 1,000,000
          + "%03d" + Path.SEPARATOR // (app num / 1000) % 1000
          + "%s" + Path.SEPARATOR; // full app id

  private YarnClient yarnClient;
  private TimelineStore summaryStore;
  private TimelineACLsManager aclManager;
  private TimelineDataManager summaryTdm;
  private ConcurrentMap<ApplicationId, AppLogs> appIdLogMap =
      new ConcurrentHashMap<ApplicationId, AppLogs>();
  private ScheduledThreadPoolExecutor executor;
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

  public EntityGroupFSTimelineStore() {
    super(EntityGroupFSTimelineStore.class.getSimpleName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    summaryStore = createSummaryStore();
    summaryStore.init(conf);
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
              cacheItem.releaseCache(groupId);
              if (cacheItem.getAppLogs().isDone()) {
                appIdLogMap.remove(groupId.getApplicationId());
              }
              return true;
            }
            return false;
          }
      });
    cacheIdPlugins = loadPlugIns(conf);
    // Initialize yarn client for application status
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    super.serviceInit(conf);
  }

  private List<TimelineEntityGroupPlugin> loadPlugIns(Configuration conf)
      throws RuntimeException {
    Collection<String> pluginNames = conf.getStringCollection(
        YarnConfiguration.TIMELINE_SERVICE_ENTITY_GROUP_PLUGIN_CLASSES);
    List<TimelineEntityGroupPlugin> pluginList
        = new LinkedList<TimelineEntityGroupPlugin>();
    for (final String name : pluginNames) {
      LOG.debug("Trying to load plugin class {}", name);
      TimelineEntityGroupPlugin cacheIdPlugin = null;
      try {
        Class<?> clazz = conf.getClassByName(name);
        cacheIdPlugin =
            (TimelineEntityGroupPlugin) ReflectionUtils.newInstance(
                clazz, conf);
      } catch (Exception e) {
        LOG.warn("Error loading plugin " + name, e);
      }

      if (cacheIdPlugin == null) {
        throw new RuntimeException("No class defined for " + name);
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
    LOG.info("Starting {}", getName());
    yarnClient.start();
    summaryStore.start();

    Configuration conf = getConfig();
    aclManager = new TimelineACLsManager(conf);
    aclManager.setTimelineStore(summaryStore);
    summaryTdm = new TimelineDataManager(summaryStore, aclManager);
    summaryTdm.init(conf);
    summaryTdm.start();
    activeRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR_DEFAULT));
    doneRootPath = new Path(conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR,
        YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_DONE_DIR_DEFAULT));
    fs = activeRootPath.getFileSystem(conf);
    if (!fs.exists(activeRootPath)) {
      fs.mkdirs(activeRootPath);
      fs.setPermission(activeRootPath, ACTIVE_DIR_PERMISSION);
    }
    if (!fs.exists(doneRootPath)) {
      fs.mkdirs(doneRootPath);
      fs.setPermission(doneRootPath, DONE_DIR_PERMISSION);
    }

    objMapper = new ObjectMapper();
    objMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector());
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
    LOG.info("Scanning active directory every {} seconds", scanIntervalSecs);
    LOG.info("Cleaning logs every {} seconds", cleanerIntervalSecs);

    executor = new ScheduledThreadPoolExecutor(numThreads,
        new ThreadFactoryBuilder().setNameFormat("EntityLogPluginWorker #%d")
            .build());
    executor.scheduleAtFixedRate(new EntityLogScanner(), 0, scanIntervalSecs,
        TimeUnit.SECONDS);
    executor.scheduleAtFixedRate(new EntityLogCleaner(), cleanerIntervalSecs,
        cleanerIntervalSecs, TimeUnit.SECONDS);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping {}", getName());
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
    if (summaryTdm != null) {
      summaryTdm.stop();
    }
    if (summaryStore != null) {
      summaryStore.stop();
    }
    if (yarnClient != null) {
      yarnClient.stop();
    }
    synchronized (cachedLogs) {
      for (EntityCacheItem cacheItem : cachedLogs.values()) {
        cacheItem.getStore().close();
      }
    }
    super.serviceStop();
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  void scanActiveLogs() throws IOException {
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(activeRootPath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      ApplicationId appId = parseApplicationId(stat.getPath().getName());
      if (appId != null) {
        LOG.debug("scan logs for {} in {}", appId, stat.getPath());
        AppLogs logs = getAndSetActiveLog(appId, stat.getPath());
        executor.execute(new ActiveLogParser(logs));
      }
    }
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
   * @param fs
   * @param retainMillis
   * @throws IOException
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  static void cleanLogs(Path dirpath, FileSystem fs, long retainMillis)
      throws IOException {
    long now = Time.now();
    // Depth first search from root directory for all application log dirs
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(dirpath);
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      if (stat.isDirectory()) {
        // If current is an application log dir, decide if we need to remove it
        // and remove if necessary.
        // Otherwise, keep iterating into it.
        ApplicationId appId = parseApplicationId(dirpath.getName());
        if (appId != null) { // Application log dir
          if (shouldCleanAppLogDir(dirpath, now, fs, retainMillis)) {
            try {
              LOG.info("Deleting {}", dirpath);
              if (!fs.delete(dirpath, true)) {
                LOG.error("Unable to remove " + dirpath);
              }
            } catch (IOException e) {
              LOG.error("Unable to remove " + dirpath, e);
            }
          }
        } else { // Keep cleaning inside
          cleanLogs(stat.getPath(), fs, retainMillis);
        }
      }
    }
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
    ApplicationId appId = null;
    if (appIdStr.startsWith(ApplicationId.appIdStrPrefix)) {
      try {
        appId = ConverterUtils.toApplicationId(appIdStr);
      } catch (IllegalArgumentException e) {
        appId = null;
      }
    }
    return appId;
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

  // This method has to be synchronized to control traffic to RM
  private static synchronized AppState getAppState(ApplicationId appId,
      YarnClient yarnClient) throws IOException {
    AppState appState = AppState.ACTIVE;
    try {
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState yarnState = report.getYarnApplicationState();
      if (APP_FINAL_STATES.contains(yarnState)) {
        appState = AppState.COMPLETED;
      }
    } catch (ApplicationNotFoundException e) {
      appState = AppState.UNKNOWN;
    } catch (YarnException e) {
      throw new IOException(e);
    }
    return appState;
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  enum AppState {
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
      if (!isDone()) {
        LOG.debug("Try to parse summary log for log {} in {}",
            appId, appDirPath);
        appState = EntityGroupFSTimelineStore.getAppState(appId, yarnClient);
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
          log.parseForStore(tdm, appDirPath, isDone(), jsonFactory,
              objMapper, fs);
        } else {
          // The log may have been removed, remove the log
          removeList.add(log);
          LOG.info("File {} no longer exists, remove it from log list",
              log.getPath(appDirPath));
        }
      }
      summaryLogs.removeAll(removeList);
    }

    // scans for new logs and returns the modification timestamp of the
    // most recently modified log
    @InterfaceAudience.Private
    @VisibleForTesting
    long scanForLogs() throws IOException {
      LOG.debug("scanForLogs on {}", appDirPath);
      long newestModTime = 0;
      RemoteIterator<FileStatus> iterAttempt =
          fs.listStatusIterator(appDirPath);
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
        RemoteIterator<FileStatus> iterCache
            = fs.listStatusIterator(statAttempt.getPath());
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

    private void addDetailLog(String attemptDirName, String filename,
        String owner) {
      for (LogInfo log : detailLogs) {
        if (log.getFilename().equals(filename)
            && log.getAttemptDirName().equals(attemptDirName)) {
          return;
        }
      }
      detailLogs.add(new EntityLogInfo(attemptDirName, filename, owner));
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

  private class EntityLogScanner implements Runnable {
    @Override
    public void run() {
      LOG.debug("Active scan starting");
      try {
        scanActiveLogs();
      } catch (Exception e) {
        LOG.error("Error scanning active files", e);
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
        LOG.error("Error processing logs for " + appLogs.getAppId(), e);
      }
    }
  }

  private class EntityLogCleaner implements Runnable {
    @Override
    public void run() {
      LOG.debug("Cleaner starting");
      try {
        cleanLogs(doneRootPath, fs, logRetainMillis);
      } catch (Exception e) {
        LOG.error("Error cleaning files", e);
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
      Set<TimelineEntityGroupId> groupIds, String entityType)
      throws IOException {
    List<TimelineStore> stores = new LinkedList<TimelineStore>();
    // For now we just handle one store in a context. We return the first
    // non-null storage for the group ids.
    for (TimelineEntityGroupId groupId : groupIds) {
      TimelineStore storeForId = getCachedStore(groupId);
      if (storeForId != null) {
        LOG.debug("Adding {} as a store for the query", storeForId.getName());
        stores.add(storeForId);
      }
    }
    if (stores.size() == 0) {
      LOG.debug("Using summary store for {}", entityType);
      stores.add(this.summaryStore);
    }
    return stores;
  }

  private List<TimelineStore> getTimelineStoresForRead(String entityId,
      String entityType) throws IOException {
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
    return getTimelineStoresFromCacheIds(groupIds, entityType);
  }

  private List<TimelineStore> getTimelineStoresForRead(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters)
      throws IOException {
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
    return getTimelineStoresFromCacheIds(groupIds, entityType);
  }

  // find a cached timeline store or null if it cannot be located
  private TimelineStore getCachedStore(TimelineEntityGroupId groupId)
      throws IOException {
    EntityCacheItem cacheItem;
    synchronized (this.cachedLogs) {
      // Note that the content in the cache log storage may be stale.
      cacheItem = this.cachedLogs.get(groupId);
      if (cacheItem == null) {
        LOG.debug("Set up new cache item for id {}", groupId);
        cacheItem = new EntityCacheItem(getConfig(), fs);
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
      store = cacheItem.refreshCache(groupId, aclManager, jsonFactory,
          objMapper);
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
    List<TimelineStore> stores = getTimelineStoresForRead(entityType,
        primaryFilter, secondaryFilters);
    TimelineEntities returnEntities = new TimelineEntities();
    for (TimelineStore store : stores) {
      LOG.debug("Try timeline store {} for the request", store.getName());
      returnEntities.addEntities(
          store.getEntities(entityType, limit, windowStart, windowEnd, fromId,
              fromTs, primaryFilter, secondaryFilters, fieldsToRetrieve,
              checkAcl).getEntities());
    }
    return returnEntities;
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fieldsToRetrieve) throws IOException {
    LOG.debug("getEntity type={} id={}", entityType, entityId);
    List<TimelineStore> stores = getTimelineStoresForRead(entityId, entityType);
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
    for (String entityId : entityIds) {
      LOG.debug("getEntityTimeline type={} id={}", entityType, entityId);
      List<TimelineStore> stores
          = getTimelineStoresForRead(entityId, entityType);
      for (TimelineStore store : stores) {
        LOG.debug("Try timeline store {}:{} for the request", store.getName(),
            store.toString());
        SortedSet<String> entityIdSet = new TreeSet<>();
        entityIdSet.add(entityId);
        TimelineEvents events =
            store.getEntityTimelines(entityType, entityIdSet, limit,
                windowStart, windowEnd, eventTypes);
        returnEvents.addEvents(events.getAllEvents());
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
}
