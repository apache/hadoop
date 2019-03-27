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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.sun.jersey.api.client.Client;

/**
 * A simple writer class for storing Timeline data in any storage that
 * implements a basic FileSystem interface.
 * This writer is used for ATSv1.5.
 */
@Private
@Unstable
public class FileSystemTimelineWriter extends TimelineWriter{

  private static final Logger LOG = LoggerFactory
      .getLogger(FileSystemTimelineWriter.class);

  // App log directory must be readable by group so server can access logs
  // and writable by group so it can be deleted by server
  private static final short APP_LOG_DIR_PERMISSIONS = 0770;
  // Logs must be readable by group so server can access them
  private static final short FILE_LOG_PERMISSIONS = 0640;
  private static final String DOMAIN_LOG_PREFIX = "domainlog-";
  private static final String SUMMARY_LOG_PREFIX = "summarylog-";
  private static final String ENTITY_LOG_PREFIX = "entitylog-";

  private Path activePath = null;
  private FileSystem fs = null;
  private Set<String> summaryEntityTypes;
  private ObjectMapper objMapper = null;
  private long flushIntervalSecs;
  private long cleanIntervalSecs;
  private long ttl;
  private LogFDsCache logFDsCache = null;
  private boolean isAppendSupported;
  private final AttemptDirCache attemptDirCache;

  public FileSystemTimelineWriter(Configuration conf,
      UserGroupInformation authUgi, Client client, URI resURI)
      throws IOException {
    super(authUgi, client, resURI);

    Configuration fsConf = new Configuration(conf);

    activePath = new Path(fsConf.get(
      YarnConfiguration
          .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR,
      YarnConfiguration
          .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR_DEFAULT));
    fs = FileSystem.newInstance(activePath.toUri(), fsConf);

    // raise FileNotFoundException if the path is not found
    fs.getFileStatus(activePath);
    summaryEntityTypes = new HashSet<String>(
        conf.getStringCollection(YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES));

    flushIntervalSecs = conf.getLong(
        YarnConfiguration
          .TIMELINE_SERVICE_CLIENT_FD_FLUSH_INTERVAL_SECS,
        YarnConfiguration
          .TIMELINE_SERVICE_CLIENT_FD_FLUSH_INTERVAL_SECS_DEFAULT);

    cleanIntervalSecs = conf.getLong(
        YarnConfiguration
          .TIMELINE_SERVICE_CLIENT_FD_CLEAN_INTERVAL_SECS,
        YarnConfiguration
          .TIMELINE_SERVICE_CLIENT_FD_CLEAN_INTERVAL_SECS_DEFAULT);

    ttl = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS,
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS_DEFAULT);

    long timerTaskTTL = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_INTERNAL_TIMERS_TTL_SECS,
        YarnConfiguration
            .TIMELINE_SERVICE_CLIENT_INTERNAL_TIMERS_TTL_SECS_DEFAULT);

    logFDsCache =
        new LogFDsCache(flushIntervalSecs, cleanIntervalSecs, ttl,
            timerTaskTTL);

    this.isAppendSupported = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_FS_SUPPORT_APPEND, true);

    boolean storeInsideUserDir = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_WITH_USER_DIR,
        false);

    objMapper = createObjectMapper();

    int attemptDirCacheSize = conf.getInt(
        YarnConfiguration
            .TIMELINE_SERVICE_CLIENT_INTERNAL_ATTEMPT_DIR_CACHE_SIZE,
        YarnConfiguration
            .DEFAULT_TIMELINE_SERVICE_CLIENT_INTERNAL_ATTEMPT_DIR_CACHE_SIZE);

    attemptDirCache = new AttemptDirCache(attemptDirCacheSize, fs, activePath,
        authUgi, storeInsideUserDir);

    if (LOG.isDebugEnabled()) {
      StringBuilder debugMSG = new StringBuilder();
      debugMSG.append(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_FD_FLUSH_INTERVAL_SECS
              + "=" + flushIntervalSecs + ", " +
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_FD_CLEAN_INTERVAL_SECS
              + "=" + cleanIntervalSecs + ", " +
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS
              + "=" + ttl + ", " +
          YarnConfiguration.TIMELINE_SERVICE_ENTITYFILE_FS_SUPPORT_APPEND
              + "=" + isAppendSupported + ", " +
          YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_WITH_USER_DIR
              + "=" + storeInsideUserDir + ", " +
          YarnConfiguration.TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_ACTIVE_DIR
              + "=" + activePath);

      if (summaryEntityTypes != null && !summaryEntityTypes.isEmpty()) {
        debugMSG.append(", " + YarnConfiguration
            .TIMELINE_SERVICE_ENTITYGROUP_FS_STORE_SUMMARY_ENTITY_TYPES
            + " = " + summaryEntityTypes);
      }
      LOG.debug(debugMSG.toString());
    }
  }

  @Override
  public String toString() {
    return "FileSystemTimelineWriter writing to " + activePath;
  }

  @Override
  public TimelinePutResponse putEntities(
      ApplicationAttemptId appAttemptId, TimelineEntityGroupId groupId,
      TimelineEntity... entities) throws IOException, YarnException {
    if (appAttemptId == null) {
      return putEntities(entities);
    }

    List<TimelineEntity> entitiesToDBStore = new ArrayList<TimelineEntity>();
    List<TimelineEntity> entitiesToSummaryCache
        = new ArrayList<TimelineEntity>();
    List<TimelineEntity> entitiesToEntityCache
        = new ArrayList<TimelineEntity>();
    Path attemptDir = attemptDirCache.getAppAttemptDir(appAttemptId);

    for (TimelineEntity entity : entities) {
      if (summaryEntityTypes.contains(entity.getEntityType())) {
        entitiesToSummaryCache.add(entity);
      } else {
        if (groupId != null) {
          entitiesToEntityCache.add(entity);
        } else {
          entitiesToDBStore.add(entity);
        }
      }
    }

    if (!entitiesToSummaryCache.isEmpty()) {
      Path summaryLogPath =
          new Path(attemptDir, SUMMARY_LOG_PREFIX + appAttemptId.toString());
      LOG.debug("Writing summary log for {} to {}", appAttemptId,
          summaryLogPath);
      this.logFDsCache.writeSummaryEntityLogs(fs, summaryLogPath, objMapper,
          appAttemptId, entitiesToSummaryCache, isAppendSupported);
    }

    if (!entitiesToEntityCache.isEmpty()) {
      Path entityLogPath =
          new Path(attemptDir, ENTITY_LOG_PREFIX + groupId.toString());
      LOG.debug("Writing entity log for {} to {}", groupId, entityLogPath);
      this.logFDsCache.writeEntityLogs(fs, entityLogPath, objMapper,
          appAttemptId, groupId, entitiesToEntityCache, isAppendSupported);
    }

    if (!entitiesToDBStore.isEmpty()) {
      putEntities(entitiesToDBStore.toArray(
          new TimelineEntity[entitiesToDBStore.size()]));
    }

    return new TimelinePutResponse();
  }

  @Override
  public void putDomain(ApplicationAttemptId appAttemptId,
      TimelineDomain domain) throws IOException, YarnException {
    if (appAttemptId == null) {
      putDomain(domain);
    } else {
      writeDomain(appAttemptId, domain);
    }
  }

  @Override
  public synchronized void close() throws Exception {
    if (logFDsCache != null) {
      LOG.debug("Closing cache");
      logFDsCache.flush();
    }
    IOUtils.cleanupWithLogger(LOG, logFDsCache, fs);
  }

  @Override
  public void flush() throws IOException {
    if (logFDsCache != null) {
      LOG.debug("Flushing cache");
      logFDsCache.flush();
    }
  }

  private ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(
        new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, false);
    return mapper;
  }

  private void writeDomain(ApplicationAttemptId appAttemptId,
      TimelineDomain domain) throws IOException {
    Path domainLogPath =
        new Path(attemptDirCache.getAppAttemptDir(appAttemptId),
            DOMAIN_LOG_PREFIX + appAttemptId.toString());
    LOG.debug("Writing domains for {} to {}", appAttemptId, domainLogPath);
    this.logFDsCache.writeDomainLog(
        fs, domainLogPath, objMapper, domain, isAppendSupported);
  }

  private static class DomainLogFD extends LogFD {
    public DomainLogFD(FileSystem fs, Path logPath, ObjectMapper objMapper,
        boolean isAppendSupported) throws IOException {
      super(fs, logPath, objMapper, isAppendSupported);
    }

    public void writeDomain(TimelineDomain domain)
        throws IOException {
      getObjectMapper().writeValue(getJsonGenerator(), domain);
      updateLastModifiedTime(Time.monotonicNow());
    }
  }

  private static class EntityLogFD extends LogFD {
    public EntityLogFD(FileSystem fs, Path logPath, ObjectMapper objMapper,
        boolean isAppendSupported) throws IOException {
      super(fs, logPath, objMapper, isAppendSupported);
    }

    public void writeEntities(List<TimelineEntity> entities)
        throws IOException {
      if (writerClosed()) {
        prepareForWrite();
      }
      LOG.debug("Writing entity list of size {}", entities.size());
      for (TimelineEntity entity : entities) {
        getObjectMapper().writeValue(getJsonGenerator(), entity);
      }
      updateLastModifiedTime(Time.monotonicNow());
    }
  }

  private static class LogFD {
    private FSDataOutputStream stream;
    private ObjectMapper objMapper;
    private JsonGenerator jsonGenerator;
    private long lastModifiedTime;
    private final boolean isAppendSupported;
    private final ReentrantLock fdLock = new ReentrantLock();
    private final FileSystem fs;
    private final Path logPath;

    public LogFD(FileSystem fs, Path logPath, ObjectMapper objMapper,
        boolean isAppendSupported) throws IOException {
      this.fs = fs;
      this.logPath = logPath;
      this.isAppendSupported = isAppendSupported;
      this.objMapper = objMapper;
      prepareForWrite();
    }

    public void close() {
      if (stream != null) {
        IOUtils.cleanupWithLogger(LOG, jsonGenerator);
        IOUtils.cleanupWithLogger(LOG, stream);
        stream = null;
        jsonGenerator = null;
      }
    }

    public void flush() throws IOException {
      if (stream != null) {
        jsonGenerator.flush();
        stream.hflush();
      }
    }

    public long getLastModifiedTime() {
      return this.lastModifiedTime;
    }

    protected void prepareForWrite() throws IOException{
      this.stream = createLogFileStream(fs, logPath);
      this.jsonGenerator = new JsonFactory().createGenerator(
          (OutputStream)stream);
      this.jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
      this.lastModifiedTime = Time.monotonicNow();
    }

    protected boolean writerClosed() {
      return stream == null;
    }

    private FSDataOutputStream createLogFileStream(FileSystem fileSystem,
        Path logPathToCreate)
        throws IOException {
      FSDataOutputStream streamToCreate;
      if (!isAppendSupported) {
        logPathToCreate =
            new Path(logPathToCreate.getParent(),
              (logPathToCreate.getName() + "_" + Time.monotonicNow()));
      }
      if (!fileSystem.exists(logPathToCreate)) {
        streamToCreate = fileSystem.create(logPathToCreate, false);
        fileSystem.setPermission(logPathToCreate,
            new FsPermission(FILE_LOG_PERMISSIONS));
      } else {
        streamToCreate = fileSystem.append(logPathToCreate);
      }
      return streamToCreate;
    }

    public void lock() {
      this.fdLock.lock();
    }

    public void unlock() {
      this.fdLock.unlock();
    }

    protected JsonGenerator getJsonGenerator() {
      return jsonGenerator;
    }

    protected ObjectMapper getObjectMapper() {
      return objMapper;
    }

    protected void updateLastModifiedTime(long updatedTime) {
      this.lastModifiedTime = updatedTime;
    }
  }

  private static class LogFDsCache implements Closeable, Flushable{
    private DomainLogFD domainLogFD;
    private Map<ApplicationAttemptId, EntityLogFD> summanyLogFDs;
    private Map<ApplicationAttemptId, HashMap<TimelineEntityGroupId,
        EntityLogFD>> entityLogFDs;
    private Timer flushTimer = null;
    private Timer cleanInActiveFDsTimer = null;
    private Timer monitorTaskTimer = null;
    private final long ttl;
    private final ReentrantLock domainFDLocker = new ReentrantLock();
    private final ReentrantLock summaryTableLocker = new ReentrantLock();
    private final ReentrantLock entityTableLocker = new ReentrantLock();
    private final ReentrantLock summaryTableCopyLocker = new ReentrantLock();
    private final ReentrantLock entityTableCopyLocker = new ReentrantLock();
    private volatile boolean serviceStopped = false;
    private volatile boolean timerTaskStarted = false;
    private final ReentrantLock timerTaskLocker = new ReentrantLock();
    private final long flushIntervalSecs;
    private final long cleanIntervalSecs;
    private final long timerTaskRetainTTL;
    private volatile long timeStampOfLastWrite = Time.monotonicNow();
    private final ReadLock timerTasksMonitorReadLock;
    private final WriteLock timerTasksMonitorWriteLock;

    public LogFDsCache(long flushIntervalSecs, long cleanIntervalSecs,
        long ttl, long timerTaskRetainTTL) {
      domainLogFD = null;
      summanyLogFDs = new HashMap<ApplicationAttemptId, EntityLogFD>();
      entityLogFDs = new HashMap<ApplicationAttemptId,
          HashMap<TimelineEntityGroupId, EntityLogFD>>();
      this.ttl = ttl * 1000;
      this.flushIntervalSecs = flushIntervalSecs;
      this.cleanIntervalSecs = cleanIntervalSecs;
      long timerTaskRetainTTLVar = timerTaskRetainTTL * 1000;
      if (timerTaskRetainTTLVar > this.ttl) {
        this.timerTaskRetainTTL = timerTaskRetainTTLVar;
      } else {
        this.timerTaskRetainTTL = this.ttl + 2 * 60 * 1000;
        LOG.warn("The specific " + YarnConfiguration
            .TIMELINE_SERVICE_CLIENT_INTERNAL_TIMERS_TTL_SECS + " : "
            + timerTaskRetainTTL + " is invalid, because it is less than or "
            + "equal to " + YarnConfiguration
            .TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS + " : " + ttl + ". Use "
            + YarnConfiguration.TIMELINE_SERVICE_CLIENT_FD_RETAIN_SECS + " : "
            + ttl + " + 120s instead.");
      }
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      this.timerTasksMonitorReadLock = lock.readLock();
      this.timerTasksMonitorWriteLock = lock.writeLock();
    }

    @Override
    public void flush() throws IOException {
      this.domainFDLocker.lock();
      try {
        if (domainLogFD != null) {
          domainLogFD.flush();
        }
      } finally {
        this.domainFDLocker.unlock();
      }

      flushSummaryFDMap(copySummaryLogFDs(summanyLogFDs));

      flushEntityFDMap(copyEntityLogFDs(entityLogFDs));
    }

    private Map<ApplicationAttemptId, EntityLogFD> copySummaryLogFDs(
        Map<ApplicationAttemptId, EntityLogFD> summanyLogFDsToCopy) {
      summaryTableCopyLocker.lock();
      try {
        return new HashMap<ApplicationAttemptId, EntityLogFD>(
            summanyLogFDsToCopy);
      } finally {
        summaryTableCopyLocker.unlock();
      }
    }

    private Map<ApplicationAttemptId, HashMap<TimelineEntityGroupId,
        EntityLogFD>> copyEntityLogFDs(Map<ApplicationAttemptId,
        HashMap<TimelineEntityGroupId, EntityLogFD>> entityLogFDsToCopy) {
      entityTableCopyLocker.lock();
      try {
        return new HashMap<ApplicationAttemptId, HashMap<TimelineEntityGroupId,
            EntityLogFD>>(entityLogFDsToCopy);
      } finally {
        entityTableCopyLocker.unlock();
      }
    }

    private void flushSummaryFDMap(Map<ApplicationAttemptId,
        EntityLogFD> logFDs) throws IOException {
      if (!logFDs.isEmpty()) {
        for (Entry<ApplicationAttemptId, EntityLogFD> logFDEntry : logFDs
            .entrySet()) {
          EntityLogFD logFD = logFDEntry.getValue();
          logFD.lock();
          try {
            logFD.flush();
          } finally {
            logFD.unlock();
          }
        }
      }
    }

    private void flushEntityFDMap(Map<ApplicationAttemptId, HashMap<
        TimelineEntityGroupId, EntityLogFD>> logFDs) throws IOException {
      if (!logFDs.isEmpty()) {
        for (Entry<ApplicationAttemptId, HashMap<TimelineEntityGroupId,
                 EntityLogFD>> logFDMapEntry : logFDs.entrySet()) {
          HashMap<TimelineEntityGroupId, EntityLogFD> logFDMap
              = logFDMapEntry.getValue();
          for (Entry<TimelineEntityGroupId, EntityLogFD> logFDEntry
              : logFDMap.entrySet()) {
            EntityLogFD logFD = logFDEntry.getValue();
            logFD.lock();
            try {
              logFD.flush();
            } finally {
              logFD.unlock();
            }
          }
        }
      }
    }

    private class FlushTimerTask extends TimerTask {
      @Override
      public void run() {
        try {
          flush();
        } catch (Exception e) {
          LOG.debug("{}", e);
        }
      }
    }

    private void cleanInActiveFDs() {
      long currentTimeStamp = Time.monotonicNow();
      this.domainFDLocker.lock();
      try {
        if (domainLogFD != null) {
          if (currentTimeStamp - domainLogFD.getLastModifiedTime() >= ttl) {
            domainLogFD.close();
            domainLogFD = null;
          }
        }
      } finally {
        this.domainFDLocker.unlock();
      }

      cleanInActiveSummaryFDsforMap(copySummaryLogFDs(summanyLogFDs),
          currentTimeStamp);

      cleanInActiveEntityFDsforMap(copyEntityLogFDs(entityLogFDs),
          currentTimeStamp);
    }

    private void cleanInActiveSummaryFDsforMap(
        Map<ApplicationAttemptId, EntityLogFD> logFDs,
        long currentTimeStamp) {
      if (!logFDs.isEmpty()) {
        for (Entry<ApplicationAttemptId, EntityLogFD> logFDEntry : logFDs
            .entrySet()) {
          EntityLogFD logFD = logFDEntry.getValue();
          logFD.lock();
          try {
            if (currentTimeStamp - logFD.getLastModifiedTime() >= ttl) {
              logFD.close();
            }
          } finally {
            logFD.unlock();
          }
        }
      }
    }

    private void cleanInActiveEntityFDsforMap(Map<ApplicationAttemptId,
        HashMap<TimelineEntityGroupId, EntityLogFD>> logFDs,
        long currentTimeStamp) {
      if (!logFDs.isEmpty()) {
        for (Entry<ApplicationAttemptId, HashMap<
                 TimelineEntityGroupId, EntityLogFD>> logFDMapEntry
                : logFDs.entrySet()) {
          HashMap<TimelineEntityGroupId, EntityLogFD> logFDMap
              = logFDMapEntry.getValue();
          for (Entry<TimelineEntityGroupId, EntityLogFD> logFDEntry
              : logFDMap.entrySet()) {
            EntityLogFD logFD = logFDEntry.getValue();
            logFD.lock();
            try {
              if (currentTimeStamp - logFD.getLastModifiedTime() >= ttl) {
                logFD.close();
              }
            } finally {
              logFD.unlock();
            }
          }
        }
      }
    }

    private class CleanInActiveFDsTask extends TimerTask {
      @Override
      public void run() {
        try {
          cleanInActiveFDs();
        } catch (Exception e) {
          LOG.warn(e.toString());
        }
      }
    }

    private class TimerMonitorTask extends TimerTask {
      @Override
      public void run() {
        timerTasksMonitorWriteLock.lock();
        try {
          monitorTimerTasks();
        } finally {
          timerTasksMonitorWriteLock.unlock();
        }
      }
    }

    private void monitorTimerTasks() {
      if (Time.monotonicNow() - this.timeStampOfLastWrite
          >= this.timerTaskRetainTTL) {
        cancelAndCloseTimerTasks();

        timerTaskStarted = false;
      } else {
        if (this.monitorTaskTimer != null) {
          this.monitorTaskTimer.schedule(new TimerMonitorTask(),
              this.timerTaskRetainTTL);
        }
      }
    }

    @Override
    public void close() throws IOException {

      serviceStopped = true;

      cancelAndCloseTimerTasks();
    }

    private void cancelAndCloseTimerTasks() {
      if (flushTimer != null) {
        flushTimer.cancel();
        flushTimer = null;
      }

      if (cleanInActiveFDsTimer != null) {
        cleanInActiveFDsTimer.cancel();
        cleanInActiveFDsTimer = null;
      }

      if (monitorTaskTimer != null) {
        monitorTaskTimer.cancel();
        monitorTaskTimer = null;
      }

      this.domainFDLocker.lock();
      try {
        if (domainLogFD != null) {
          domainLogFD.close();
          domainLogFD = null;
        }
      } finally {
        this.domainFDLocker.unlock();
      }

      closeSummaryFDs(summanyLogFDs);

      closeEntityFDs(entityLogFDs);
    }

    private void closeEntityFDs(Map<ApplicationAttemptId,
        HashMap<TimelineEntityGroupId, EntityLogFD>> logFDs) {
      entityTableLocker.lock();
      try {
        if (!logFDs.isEmpty()) {
          for (Entry<ApplicationAttemptId, HashMap<TimelineEntityGroupId,
                   EntityLogFD>> logFDMapEntry : logFDs.entrySet()) {
            HashMap<TimelineEntityGroupId, EntityLogFD> logFDMap
                = logFDMapEntry.getValue();
            for (Entry<TimelineEntityGroupId, EntityLogFD> logFDEntry
                : logFDMap.entrySet()) {
              EntityLogFD logFD = logFDEntry.getValue();
              try {
                logFD.lock();
                logFD.close();
              } finally {
                logFD.unlock();
              }
            }
          }
        }
      } finally {
        entityTableLocker.unlock();
      }
    }

    private void closeSummaryFDs(
        Map<ApplicationAttemptId, EntityLogFD> logFDs) {
      summaryTableLocker.lock();
      try {
        if (!logFDs.isEmpty()) {
          for (Entry<ApplicationAttemptId, EntityLogFD> logFDEntry
              : logFDs.entrySet()) {
            EntityLogFD logFD = logFDEntry.getValue();
            try {
              logFD.lock();
              logFD.close();
            } finally {
              logFD.unlock();
            }
          }
        }
      } finally {
        summaryTableLocker.unlock();
      }
    }

    public void writeDomainLog(FileSystem fs, Path logPath,
        ObjectMapper objMapper, TimelineDomain domain,
        boolean isAppendSupported) throws IOException {
      checkAndStartTimeTasks();
      this.domainFDLocker.lock();
      try {
        if (this.domainLogFD != null) {
          this.domainLogFD.writeDomain(domain);
        } else {
          this.domainLogFD =
              new DomainLogFD(fs, logPath, objMapper, isAppendSupported);
          this.domainLogFD.writeDomain(domain);
        }
      } finally {
        this.domainFDLocker.unlock();
      }
    }

    public void writeEntityLogs(FileSystem fs, Path entityLogPath,
        ObjectMapper objMapper, ApplicationAttemptId appAttemptId,
        TimelineEntityGroupId groupId, List<TimelineEntity> entitiesToEntity,
        boolean isAppendSupported) throws IOException{
      checkAndStartTimeTasks();
      writeEntityLogs(fs, entityLogPath, objMapper, appAttemptId,
          groupId, entitiesToEntity, isAppendSupported, this.entityLogFDs);
    }

    private void writeEntityLogs(FileSystem fs, Path logPath,
        ObjectMapper objMapper, ApplicationAttemptId attemptId,
        TimelineEntityGroupId groupId, List<TimelineEntity> entities,
        boolean isAppendSupported, Map<ApplicationAttemptId, HashMap<
            TimelineEntityGroupId, EntityLogFD>> logFDs) throws IOException {
      HashMap<TimelineEntityGroupId, EntityLogFD>logMapFD
          = logFDs.get(attemptId);
      if (logMapFD != null) {
        EntityLogFD logFD = logMapFD.get(groupId);
        if (logFD != null) {
          logFD.lock();
          try {
            if (serviceStopped) {
              return;
            }
            logFD.writeEntities(entities);
          } finally {
            logFD.unlock();
          }
        } else {
          createEntityFDandWrite(fs, logPath, objMapper, attemptId, groupId,
              entities, isAppendSupported, logFDs);
        }
      } else {
        createEntityFDandWrite(fs, logPath, objMapper, attemptId, groupId,
            entities, isAppendSupported, logFDs);
      }
    }

    private void createEntityFDandWrite(FileSystem fs, Path logPath,
        ObjectMapper objMapper, ApplicationAttemptId attemptId,
        TimelineEntityGroupId groupId, List<TimelineEntity> entities,
        boolean isAppendSupported, Map<ApplicationAttemptId, HashMap<
            TimelineEntityGroupId, EntityLogFD>> logFDs) throws IOException{
      entityTableLocker.lock();
      try {
        if (serviceStopped) {
          return;
        }
        HashMap<TimelineEntityGroupId, EntityLogFD> logFDMap =
            logFDs.get(attemptId);
        if (logFDMap == null) {
          logFDMap = new HashMap<TimelineEntityGroupId, EntityLogFD>();
        }
        EntityLogFD logFD = logFDMap.get(groupId);
        if (logFD == null) {
          logFD = new EntityLogFD(fs, logPath, objMapper, isAppendSupported);
        }
        logFD.lock();
        try {
          logFD.writeEntities(entities);
          entityTableCopyLocker.lock();
          try {
            logFDMap.put(groupId, logFD);
            logFDs.put(attemptId, logFDMap);
          } finally {
            entityTableCopyLocker.unlock();
          }
        } finally {
          logFD.unlock();
        }
      } finally {
        entityTableLocker.unlock();
      }
    }

    public void writeSummaryEntityLogs(FileSystem fs, Path logPath,
        ObjectMapper objMapper, ApplicationAttemptId attemptId,
        List<TimelineEntity> entities, boolean isAppendSupported)
        throws IOException {
      checkAndStartTimeTasks();
      writeSummmaryEntityLogs(fs, logPath, objMapper, attemptId, entities,
          isAppendSupported, this.summanyLogFDs);
    }

    private void writeSummmaryEntityLogs(FileSystem fs, Path logPath,
        ObjectMapper objMapper, ApplicationAttemptId attemptId,
        List<TimelineEntity> entities, boolean isAppendSupported,
        Map<ApplicationAttemptId, EntityLogFD> logFDs) throws IOException {
      EntityLogFD logFD = null;
      logFD = logFDs.get(attemptId);
      if (logFD != null) {
        logFD.lock();
        try {
          if (serviceStopped) {
            return;
          }
          logFD.writeEntities(entities);
        } finally {
          logFD.unlock();
        }
      } else {
        createSummaryFDAndWrite(fs, logPath, objMapper, attemptId, entities,
            isAppendSupported, logFDs);
      }
    }

    private void createSummaryFDAndWrite(FileSystem fs, Path logPath,
        ObjectMapper objMapper, ApplicationAttemptId attemptId,
        List<TimelineEntity> entities, boolean isAppendSupported,
        Map<ApplicationAttemptId, EntityLogFD> logFDs) throws IOException {
      summaryTableLocker.lock();
      try {
        if (serviceStopped) {
          return;
        }
        EntityLogFD logFD = logFDs.get(attemptId);
        if (logFD == null) {
          logFD = new EntityLogFD(fs, logPath, objMapper, isAppendSupported);
        }
        logFD.lock();
        try {
          logFD.writeEntities(entities);
          summaryTableCopyLocker.lock();
          try {
            logFDs.put(attemptId, logFD);
          } finally {
            summaryTableCopyLocker.unlock();
          }
        } finally {
          logFD.unlock();
        }
      } finally {
        summaryTableLocker.unlock();
      }
    }

    private void createAndStartTimerTasks() {
      this.flushTimer =
          new Timer(LogFDsCache.class.getSimpleName() + "FlushTimer",
              true);
      this.flushTimer.schedule(new FlushTimerTask(), flushIntervalSecs * 1000,
          flushIntervalSecs * 1000);

      this.cleanInActiveFDsTimer =
          new Timer(LogFDsCache.class.getSimpleName()
              + "cleanInActiveFDsTimer", true);
      this.cleanInActiveFDsTimer.schedule(new CleanInActiveFDsTask(),
          cleanIntervalSecs * 1000, cleanIntervalSecs * 1000);

      this.monitorTaskTimer =
          new Timer(LogFDsCache.class.getSimpleName() + "MonitorTimer",
              true);
      this.monitorTaskTimer.schedule(new TimerMonitorTask(),
          this.timerTaskRetainTTL);
    }

    private void checkAndStartTimeTasks() {
      this.timerTasksMonitorReadLock.lock();
      try {
        this.timeStampOfLastWrite = Time.monotonicNow();
        if(!timerTaskStarted) {
          timerTaskLocker.lock();
          try {
            if (!timerTaskStarted) {
              createAndStartTimerTasks();
              timerTaskStarted = true;
            }
          } finally {
            timerTaskLocker.unlock();
          }
        }
      } finally {
        this.timerTasksMonitorReadLock.unlock();
      }
    }
  }

  private static class AttemptDirCache {
    private final int attemptDirCacheSize;
    private final Map<ApplicationAttemptId, Path> attemptDirCache;
    private final FileSystem fs;
    private final Path activePath;
    private final UserGroupInformation authUgi;
    private final boolean storeInsideUserDir;

    public AttemptDirCache(int cacheSize, FileSystem fs, Path activePath,
        UserGroupInformation ugi, boolean storeInsideUserDir) {
      this.attemptDirCacheSize = cacheSize;
      this.attemptDirCache =
          new LinkedHashMap<ApplicationAttemptId, Path>(
              attemptDirCacheSize, 0.75f, true) {
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<ApplicationAttemptId, Path> eldest) {
              return size() > attemptDirCacheSize;
            }
          };
      this.fs = fs;
      this.activePath = activePath;
      this.authUgi = ugi;
      this.storeInsideUserDir = storeInsideUserDir;
    }

    public Path getAppAttemptDir(ApplicationAttemptId attemptId)
        throws IOException {
      Path attemptDir = this.attemptDirCache.get(attemptId);
      if (attemptDir == null) {
        synchronized(this) {
          attemptDir = this.attemptDirCache.get(attemptId);
          if (attemptDir == null) {
            attemptDir = createAttemptDir(attemptId);
            attemptDirCache.put(attemptId, attemptDir);
          }
        }
      }
      return attemptDir;
    }

    private Path createAttemptDir(ApplicationAttemptId appAttemptId)
        throws IOException {
      Path appDir = createApplicationDir(appAttemptId.getApplicationId());

      Path attemptDir = new Path(appDir, appAttemptId.toString());
      if (FileSystem.mkdirs(fs, attemptDir,
          new FsPermission(APP_LOG_DIR_PERMISSIONS))) {
        LOG.debug("New attempt directory created - {}", attemptDir);
      }
      return attemptDir;
    }

    private Path createApplicationDir(ApplicationId appId) throws IOException {
      Path appRootDir = getAppRootDir(authUgi.getShortUserName());
      Path appDir = new Path(appRootDir, appId.toString());
      if (FileSystem.mkdirs(fs, appDir,
          new FsPermission(APP_LOG_DIR_PERMISSIONS))) {
        LOG.debug("New app directory created - {}", appDir);
      }
      return appDir;
    }

    private Path getAppRootDir(String user) throws IOException {
      if (!storeInsideUserDir) {
        return activePath;
      }
      Path userDir = new Path(activePath, user);
      if (FileSystem.mkdirs(fs, userDir,
          new FsPermission(APP_LOG_DIR_PERMISSIONS))) {
        LOG.debug("New user directory created - {}", userDir);
      }
      return userDir;
    }
  }
}
