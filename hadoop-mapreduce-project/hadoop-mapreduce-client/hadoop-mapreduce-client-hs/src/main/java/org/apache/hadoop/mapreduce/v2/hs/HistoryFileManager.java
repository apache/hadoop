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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.jobhistory.JobSummary;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownThreadsHelper;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a way to interact with history files in a thread safe
 * manor.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class HistoryFileManager extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(HistoryFileManager.class);
  private static final Logger SUMMARY_LOG =
      LoggerFactory.getLogger(JobSummary.class);

  private enum HistoryInfoState {
    IN_INTERMEDIATE, IN_DONE, DELETED, MOVE_FAILED
  };
  
  private static String DONE_BEFORE_SERIAL_TAIL = JobHistoryUtils
      .doneSubdirsBeforeSerialTail();

  /**
   * Maps between a serial number (generated based on jobId) and the timestamp
   * component(s) to which it belongs. Facilitates jobId based searches. If a
   * jobId is not found in this list - it will not be found.
   */
  private static class SerialNumberIndex {
    private SortedMap<String, Set<String>> cache;
    private int maxSize;

    public SerialNumberIndex(int maxSize) {
      this.cache = new TreeMap<String, Set<String>>();
      this.maxSize = maxSize;
    }

    public synchronized void add(String serialPart, String timestampPart) {
      if (!cache.containsKey(serialPart)) {
        cache.put(serialPart, new HashSet<String>());
        if (cache.size() > maxSize) {
          String key = cache.firstKey();
          LOG.error("Dropping " + key
              + " from the SerialNumberIndex. We will no "
              + "longer be able to see jobs that are in that serial index for "
              + cache.get(key));
          cache.remove(key);
        }
      }
      Set<String> datePartSet = cache.get(serialPart);
      datePartSet.add(timestampPart);
    }

    public synchronized void remove(String serialPart, String timeStampPart) {
      if (cache.containsKey(serialPart)) {
        Set<String> set = cache.get(serialPart);
        set.remove(timeStampPart);
        if (set.isEmpty()) {
          cache.remove(serialPart);
        }
      }
    }

    public synchronized Set<String> get(String serialPart) {
      Set<String> found = cache.get(serialPart);
      if (found != null) {
        return new HashSet<String>(found);
      }
      return null;
    }
  }

  /**
   * Wrapper around {@link ConcurrentSkipListMap} that maintains size along
   * side for O(1) size() implementation for use in JobListCache.
   *
   * Note: The size is not updated atomically with changes additions/removals.
   * This race can lead to size() returning an incorrect size at times.
   */
  static class JobIdHistoryFileInfoMap {
    private ConcurrentSkipListMap<JobId, HistoryFileInfo> cache;
    private AtomicInteger mapSize;

    JobIdHistoryFileInfoMap() {
      cache = new ConcurrentSkipListMap<JobId, HistoryFileInfo>();
      mapSize = new AtomicInteger();
    }

    public HistoryFileInfo putIfAbsent(JobId key, HistoryFileInfo value) {
      HistoryFileInfo ret = cache.putIfAbsent(key, value);
      if (ret == null) {
        mapSize.incrementAndGet();
      }
      return ret;
    }

    public HistoryFileInfo remove(JobId key) {
      HistoryFileInfo ret = cache.remove(key);
      if (ret != null) {
        mapSize.decrementAndGet();
      }
      return ret;
    }

    /**
     * Returns the recorded size of the internal map. Note that this could be out
     * of sync with the actual size of the map
     * @return "recorded" size
     */
    public int size() {
      return mapSize.get();
    }

    public HistoryFileInfo get(JobId key) {
      return cache.get(key);
    }

    public NavigableSet<JobId> navigableKeySet() {
      return cache.navigableKeySet();
    }

    public Collection<HistoryFileInfo> values() {
      return cache.values();
    }
  }

  static class JobListCache {
    private JobIdHistoryFileInfoMap cache;
    private int maxSize;
    private long maxAge;

    public JobListCache(int maxSize, long maxAge) {
      this.maxSize = maxSize;
      this.maxAge = maxAge;
      this.cache = new JobIdHistoryFileInfoMap();
    }

    public HistoryFileInfo addIfAbsent(HistoryFileInfo fileInfo) {
      JobId jobId = fileInfo.getJobId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding " + jobId + " to job list cache with "
            + fileInfo.getJobIndexInfo());
      }
      HistoryFileInfo old = cache.putIfAbsent(jobId, fileInfo);
      if (cache.size() > maxSize) {
        //There is a race here, where more then one thread could be trying to
        // remove entries.  This could result in too many entries being removed
        // from the cache.  This is considered OK as the size of the cache
        // should be rather large, and we would rather have performance over
        // keeping the cache size exactly at the maximum.
        Iterator<JobId> keys = cache.navigableKeySet().iterator();
        long cutoff = System.currentTimeMillis() - maxAge;

        // MAPREDUCE-6436: In order to reduce the number of logs written
        // in case of a lot of move pending histories.
        JobId firstInIntermediateKey = null;
        int inIntermediateCount = 0;
        JobId firstMoveFailedKey = null;
        int moveFailedCount = 0;

        while (cache.size() > maxSize && keys.hasNext()) {
          JobId key = keys.next();
          HistoryFileInfo firstValue = cache.get(key);
          if (firstValue != null) {
            if (firstValue.isMovePending()) {
              if (firstValue.didMoveFail() &&
                  firstValue.jobIndexInfo.getFinishTime() <= cutoff) {
                cache.remove(key);
                // Now lets try to delete it
                try {
                  firstValue.delete();
                } catch (IOException e) {
                  LOG.error("Error while trying to delete history files" +
                      " that could not be moved to done.", e);
                }
              } else {
                if (firstValue.didMoveFail()) {
                  if (moveFailedCount == 0) {
                    firstMoveFailedKey = key;
                  }
                  moveFailedCount += 1;
                } else {
                  if (inIntermediateCount == 0) {
                    firstInIntermediateKey = key;
                  }
                  inIntermediateCount += 1;
                }
              }
            } else {
              cache.remove(key);
            }
          }
        }
        // Log output only for first jobhisotry in pendings to restrict
        // the total number of logs.
        if (inIntermediateCount > 0) {
          LOG.warn("Waiting to remove IN_INTERMEDIATE state histories " +
                  "(e.g. " + firstInIntermediateKey + ") from JobListCache " +
                  "because it is not in done yet. Total count is " +
                  inIntermediateCount + ".");
        }
        if (moveFailedCount > 0) {
          LOG.warn("Waiting to remove MOVE_FAILED state histories " +
                  "(e.g. " + firstMoveFailedKey + ") from JobListCache " +
                  "because it is not in done yet. Total count is " +
                  moveFailedCount + ".");
        }
      }
      return old;
    }

    public void delete(HistoryFileInfo fileInfo) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing from cache " + fileInfo);
      }
      cache.remove(fileInfo.getJobId());
    }

    public Collection<HistoryFileInfo> values() {
      return new ArrayList<HistoryFileInfo>(cache.values());
    }

    public HistoryFileInfo get(JobId jobId) {
      return cache.get(jobId);
    }

    public boolean isFull() {
      return cache.size() >= maxSize;
    }

    public int size() {
      return cache.size();
    }
  }

  /**
   * This class represents a user dir in the intermediate done directory.  This
   * is mostly for locking purposes. 
   */
  private class UserLogDir {
    long modTime = 0;
    private long scanTime = 0;

    public synchronized void scanIfNeeded(FileStatus fs) {
      long newModTime = fs.getModificationTime();
      // MAPREDUCE-6680: In some Cloud FileSystem, like Azure FS or S3, file's
      // modification time is truncated into seconds. In that case,
      // modTime == newModTime doesn't means no file update in the directory,
      // so we need to have additional check.
      // Note: modTime (X second Y millisecond) could be casted to X second or
      // X+1 second.
      // MAPREDUCE-7101: Some Cloud FileSystems do not currently update the
      // modification time of directories. For these, we scan every time if
      // the 'alwaysScan' is true.
      boolean alwaysScan = conf.getBoolean(
          JHAdminConfig.MR_HISTORY_ALWAYS_SCAN_USER_DIR,
          JHAdminConfig.DEFAULT_MR_HISTORY_ALWAYS_SCAN_USER_DIR);
      if (alwaysScan || modTime != newModTime
          || (scanTime/1000) == (modTime/1000)
          || (scanTime/1000 + 1) == (modTime/1000)) {
        // reset scanTime before scanning happens
        scanTime = System.currentTimeMillis();
        Path p = fs.getPath();
        try {
          scanIntermediateDirectory(p);
          //If scanning fails, we will scan again.  We assume the failure is
          // temporary.
          modTime = newModTime;
        } catch (IOException e) {
          LOG.error("Error while trying to scan the directory " + p, e);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scan not needed of " + fs.getPath());
        }
        // reset scanTime
        scanTime = System.currentTimeMillis();
      }
    }
  }

  public class HistoryFileInfo {
    private Path historyFile;
    private Path confFile;
    private Path summaryFile;
    private JobIndexInfo jobIndexInfo;
    private volatile HistoryInfoState state;

    @VisibleForTesting
    protected HistoryFileInfo(Path historyFile, Path confFile,
        Path summaryFile, JobIndexInfo jobIndexInfo, boolean isInDone) {
      this.historyFile = historyFile;
      this.confFile = confFile;
      this.summaryFile = summaryFile;
      this.jobIndexInfo = jobIndexInfo;
      state = isInDone ? HistoryInfoState.IN_DONE
          : HistoryInfoState.IN_INTERMEDIATE;
    }

    @VisibleForTesting
    boolean isMovePending() {
      return state == HistoryInfoState.IN_INTERMEDIATE
          || state == HistoryInfoState.MOVE_FAILED;
    }

    @VisibleForTesting
    boolean didMoveFail() {
      return state == HistoryInfoState.MOVE_FAILED;
    }

    /**
     * @return true if the files backed by this were deleted.
     */
    public boolean isDeleted() {
      return state == HistoryInfoState.DELETED;
    }

    @Override
    public String toString() {
      return "HistoryFileInfo jobID " + getJobId()
             + " historyFile = " + historyFile;
    }

    @VisibleForTesting
    synchronized void moveToDone() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("moveToDone: " + historyFile);
      }
      if (!isMovePending()) {
        // It was either deleted or is already in done. Either way do nothing
        if (LOG.isDebugEnabled()) {
          LOG.debug("Move no longer pending");
        }
        return;
      }
      try {
        long completeTime = jobIndexInfo.getFinishTime();
        if (completeTime == 0) {
          completeTime = System.currentTimeMillis();
        }
        JobId jobId = jobIndexInfo.getJobId();

        if (historyFile == null) {
          LOG.info("No file for job-history with " + jobId + " found in cache!");
        }

        if (confFile == null) {
          LOG.info("No file for jobConf with " + jobId + " found in cache!");
        }

        if (summaryFile == null || !intermediateDoneDirFc.util().exists(
            summaryFile)) {
          LOG.info("No summary file for job: " + jobId);
        } else {
          String jobSummaryString = getJobSummary(intermediateDoneDirFc,
              summaryFile);
          SUMMARY_LOG.info(jobSummaryString);
          LOG.info("Deleting JobSummary file: [" + summaryFile + "]");
          intermediateDoneDirFc.delete(summaryFile, false);
          summaryFile = null;
        }

        Path targetDir = canonicalHistoryLogPath(jobId, completeTime);
        addDirectoryToSerialNumberIndex(targetDir);
        makeDoneSubdir(targetDir);
        if (historyFile != null) {
          Path toPath = doneDirFc.makeQualified(new Path(targetDir, historyFile
              .getName()));
          if (!toPath.equals(historyFile)) {
            moveToDoneNow(historyFile, toPath);
            historyFile = toPath;
          }
        }
        if (confFile != null) {
          Path toPath = doneDirFc.makeQualified(new Path(targetDir, confFile
              .getName()));
          if (!toPath.equals(confFile)) {
            moveToDoneNow(confFile, toPath);
            confFile = toPath;
          }
        }
        state = HistoryInfoState.IN_DONE;
      } catch (Throwable t) {
        LOG.error("Error while trying to move a job to done", t);
        this.state = HistoryInfoState.MOVE_FAILED;
      } finally {
        notifyAll();
      }
    }

    /**
     * Parse a job from the JobHistoryFile, if the underlying file is not going
     * to be deleted and the number of tasks associated with the job is not
     * greater than maxTasksForLoadedJob.
     * 
     * @return null if the underlying job history file was deleted, or
     *         an {@link UnparsedJob} object representing a partially parsed job
     *           if the job tasks exceeds the configured maximum, or
     *         a {@link CompletedJob} representing a fully parsed job.
     * @throws IOException
     *           if there is an error trying to read the file if parsed.
     */
    public synchronized Job loadJob() throws IOException {
      if(isOversized()) {
        return new UnparsedJob(maxTasksForLoadedJob, jobIndexInfo, this);
      } else {
        return new CompletedJob(conf, jobIndexInfo.getJobId(), historyFile,
            false, jobIndexInfo.getUser(), this, aclsMgr);
      }
    }

    /**
     * Return the history file.
     * @return the history file.
     */
    public synchronized Path getHistoryFile() {
      return historyFile;
    }
    
    protected synchronized void delete() throws IOException {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("deleting " + historyFile + " and " + confFile);
        }
        state = HistoryInfoState.DELETED;
        doneDirFc.delete(doneDirFc.makeQualified(historyFile), false);
        doneDirFc.delete(doneDirFc.makeQualified(confFile), false);
      } finally {
        notifyAll();
      }
    }

    public JobIndexInfo getJobIndexInfo() {
      return jobIndexInfo;
    }

    public JobId getJobId() {
      return jobIndexInfo.getJobId();
    }

    public synchronized Path getConfFile() {
      return confFile;
    }
    
    public synchronized Configuration loadConfFile() throws IOException {
      FileContext fc = FileContext.getFileContext(confFile.toUri(), conf);
      Configuration jobConf = new Configuration(false);
      jobConf.addResource(fc.open(confFile), confFile.toString(), true);
      return jobConf;
    }

    private boolean isOversized() {
      final int totalTasks = jobIndexInfo.getNumReduces() +
          jobIndexInfo.getNumMaps();
      return (maxTasksForLoadedJob > 0) && (totalTasks > maxTasksForLoadedJob);
    }

    public synchronized void waitUntilMoved() {
      while (isMovePending() && !didMoveFail()) {
        try {
          wait();
        } catch (InterruptedException e) {
          LOG.warn("Waiting has been interrupted");
          throw new RuntimeException(e);
        }
      }
    }
  }

  private SerialNumberIndex serialNumberIndex = null;
  protected JobListCache jobListCache = null;

  // Maintains a list of known done subdirectories.
  private final Set<Path> existingDoneSubdirs = Collections
      .synchronizedSet(new HashSet<Path>());

  /**
   * Maintains a mapping between intermediate user directories and the last
   * known modification time.
   */
  private ConcurrentMap<String, UserLogDir> userDirModificationTimeMap = 
    new ConcurrentHashMap<String, UserLogDir>();

  private JobACLsManager aclsMgr;

  @VisibleForTesting
  Configuration conf;

  private String serialNumberFormat;

  private Path doneDirPrefixPath = null; // folder for completed jobs
  private FileContext doneDirFc; // done Dir FileContext

  private Path intermediateDoneDirPath = null; // Intermediate Done Dir Path
  private FileContext intermediateDoneDirFc; // Intermediate Done Dir
                                             // FileContext
  @VisibleForTesting
  protected ThreadPoolExecutor moveToDoneExecutor = null;
  private long maxHistoryAge = 0;

  /**
   * The maximum number of tasks allowed for a job to be loaded.
   */
  private int maxTasksForLoadedJob = -1;

  public HistoryFileManager() {
    super(HistoryFileManager.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;

    int serialNumberLowDigits = 3;
    serialNumberFormat = ("%0"
        + (JobHistoryUtils.SERIAL_NUMBER_DIRECTORY_DIGITS + serialNumberLowDigits)
        + "d");

    long maxFSWaitTime = conf.getLong(
        JHAdminConfig.MR_HISTORY_MAX_START_WAIT_TIME,
        JHAdminConfig.DEFAULT_MR_HISTORY_MAX_START_WAIT_TIME);
    createHistoryDirs(SystemClock.getInstance(), 10 * 1000, maxFSWaitTime);

    maxTasksForLoadedJob = conf.getInt(
        JHAdminConfig.MR_HS_LOADED_JOBS_TASKS_MAX,
        JHAdminConfig.DEFAULT_MR_HS_LOADED_JOBS_TASKS_MAX);

    this.aclsMgr = new JobACLsManager(conf);

    maxHistoryAge = conf.getLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS,
        JHAdminConfig.DEFAULT_MR_HISTORY_MAX_AGE);
    
    jobListCache = createJobListCache();

    serialNumberIndex = new SerialNumberIndex(conf.getInt(
        JHAdminConfig.MR_HISTORY_DATESTRING_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_DATESTRING_CACHE_SIZE));

    int numMoveThreads = conf.getInt(
        JHAdminConfig.MR_HISTORY_MOVE_THREAD_COUNT,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_THREAD_COUNT);
    moveToDoneExecutor = createMoveToDoneThreadPool(numMoveThreads);
    super.serviceInit(conf);
  }

  protected ThreadPoolExecutor createMoveToDoneThreadPool(int numMoveThreads) {
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "MoveIntermediateToDone Thread #%d").build();
    return new HadoopThreadPoolExecutor(numMoveThreads, numMoveThreads,
        1, TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);
  }

  @VisibleForTesting
  void createHistoryDirs(Clock clock, long intervalCheckMillis,
      long timeOutMillis) throws IOException {
    long start = clock.getTime();
    boolean done = false;
    int counter = 0;
    while (!done &&
        ((timeOutMillis == -1) || (clock.getTime() - start < timeOutMillis))) {
      done = tryCreatingHistoryDirs(counter++ % 3 == 0); // log every 3 attempts, 30sec
      if (done) {
        break;
      }
      try {
        Thread.sleep(intervalCheckMillis);
      } catch (InterruptedException ex) {
        throw new YarnRuntimeException(ex);
      }
    }
    if (!done) {
      throw new YarnRuntimeException("Timed out '" + timeOutMillis+
              "ms' waiting for FileSystem to become available");
    }
  }

  /**
   * Check if the NameNode is still not started yet as indicated by the
   * exception type and message.
   * DistributedFileSystem returns a RemoteException with a message stating
   * SafeModeException in it. So this is only way to check it is because of
   * being in safe mode. In addition, Name Node may have not started yet, in
   * which case, the message contains "NameNode still not started".
   */
  private boolean isNameNodeStillNotStarted(Exception ex) {
    String nameNodeNotStartedMsg = NameNode.composeNotStartedMessage(
        HdfsServerConstants.NamenodeRole.NAMENODE);
    return ex.toString().contains("SafeModeException") ||
        (ex instanceof RetriableException && ex.getMessage().contains(
            nameNodeNotStartedMsg));
  }

  /**
   * Returns TRUE if the history dirs were created, FALSE if they could not
   * be created because the FileSystem is not reachable or in safe mode and
   * throws and exception otherwise.
   */
  @VisibleForTesting
  boolean tryCreatingHistoryDirs(boolean logWait) throws IOException {
    boolean succeeded = true;
    String doneDirPrefix = JobHistoryUtils.
        getConfiguredHistoryServerDoneDirPrefix(conf);
    try {
      doneDirPrefixPath = FileContext.getFileContext(conf).makeQualified(
          new Path(doneDirPrefix));
      doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri(), conf);
      doneDirFc.setUMask(JobHistoryUtils.HISTORY_DONE_DIR_UMASK);
      mkdir(doneDirFc, doneDirPrefixPath, new FsPermission(
          JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION));
    } catch (ConnectException ex) {
      if (logWait) {
        LOG.info("Waiting for FileSystem at " +
            doneDirPrefixPath.toUri().getAuthority()  + "to be available");
      }
      succeeded = false;
    } catch (IOException e) {
      if (isNameNodeStillNotStarted(e)) {
        succeeded = false;
        if (logWait) {
          LOG.info("Waiting for FileSystem at " +
              doneDirPrefixPath.toUri().getAuthority() +
              "to be out of safe mode");
        }
      } else {
        throw new YarnRuntimeException("Error creating done directory: ["
            + doneDirPrefixPath + "]", e);
      }
    }
    if (succeeded) {
      String intermediateDoneDirPrefix = JobHistoryUtils.
          getConfiguredHistoryIntermediateDoneDirPrefix(conf);
      try {
        intermediateDoneDirPath = FileContext.getFileContext(conf).makeQualified(
            new Path(intermediateDoneDirPrefix));
        intermediateDoneDirFc = FileContext.getFileContext(
            intermediateDoneDirPath.toUri(), conf);
        mkdir(intermediateDoneDirFc, intermediateDoneDirPath, new FsPermission(
            JobHistoryUtils.HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS.toShort()));
      } catch (ConnectException ex) {
        succeeded = false;
        if (logWait) {
          LOG.info("Waiting for FileSystem at " +
              intermediateDoneDirPath.toUri().getAuthority() +
              "to be available");
        }
      } catch (IOException e) {
        if (isNameNodeStillNotStarted(e)) {
          succeeded = false;
          if (logWait) {
            LOG.info("Waiting for FileSystem at " +
                intermediateDoneDirPath.toUri().getAuthority() +
                "to be out of safe mode");
          }
        } else {
          throw new YarnRuntimeException(
              "Error creating intermediate done directory: ["
              + intermediateDoneDirPath + "]", e);
        }
      }
    }
    return succeeded;
  }

  @Override
  public void serviceStop() throws Exception {
    ShutdownThreadsHelper.shutdownExecutorService(moveToDoneExecutor);
    super.serviceStop();
  }

  protected JobListCache createJobListCache() {
    return new JobListCache(conf.getInt(
        JHAdminConfig.MR_HISTORY_JOBLIST_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_JOBLIST_CACHE_SIZE), maxHistoryAge);
  }

  private void mkdir(FileContext fc, Path path, FsPermission fsp)
      throws IOException {
    if (!fc.util().exists(path)) {
      try {
        fc.mkdir(path, fsp, true);

        FileStatus fsStatus = fc.getFileStatus(path);
        LOG.info("Perms after creating " + fsStatus.getPermission().toShort()
            + ", Expected: " + fsp.toShort());
        if (fsStatus.getPermission().toShort() != fsp.toShort()) {
          LOG.info("Explicitly setting permissions to : " + fsp.toShort()
              + ", " + fsp);
          fc.setPermission(path, fsp);
        }
      } catch (FileAlreadyExistsException e) {
        LOG.info("Directory: [" + path + "] already exists.");
      }
    }
  }

  protected HistoryFileInfo createHistoryFileInfo(Path historyFile,
      Path confFile, Path summaryFile, JobIndexInfo jobIndexInfo,
      boolean isInDone) {
    return new HistoryFileInfo(
        historyFile, confFile, summaryFile, jobIndexInfo, isInDone);
  }

  /**
   * Populates index data structures. Should only be called at initialization
   * times.
   */
  @SuppressWarnings("unchecked")
  void initExisting() throws IOException {
    LOG.info("Initializing Existing Jobs...");
    List<FileStatus> timestampedDirList = findTimestampedDirectories();
    // Sort first just so insertion is in a consistent order
    Collections.sort(timestampedDirList);
    LOG.info("Found " + timestampedDirList.size() + " directories to load");
    for (FileStatus fs : timestampedDirList) {
      // TODO Could verify the correct format for these directories.
      addDirectoryToSerialNumberIndex(fs.getPath());
    }
    final double maxCacheSize = (double) jobListCache.maxSize;
    int prevCacheSize = jobListCache.size();
    for (int i= timestampedDirList.size() - 1;
        i >= 0 && !jobListCache.isFull(); i--) {
      FileStatus fs = timestampedDirList.get(i); 
      addDirectoryToJobListCache(fs.getPath());

      int currCacheSize = jobListCache.size();
      if((currCacheSize - prevCacheSize)/maxCacheSize >= 0.05) {
        LOG.info(currCacheSize * 100.0 / maxCacheSize +
            "% of cache is loaded.");
      }
      prevCacheSize = currCacheSize;
    }
    final double loadedPercent = maxCacheSize == 0.0 ?
        100 : prevCacheSize * 100.0 / maxCacheSize;
    LOG.info("Existing job initialization finished. " +
        loadedPercent + "% of cache is occupied.");
  }

  private void removeDirectoryFromSerialNumberIndex(Path serialDirPath) {
    String serialPart = serialDirPath.getName();
    String timeStampPart = JobHistoryUtils
        .getTimestampPartFromPath(serialDirPath.toString());
    if (timeStampPart == null) {
      LOG.warn("Could not find timestamp portion from path: "
          + serialDirPath.toString() + ". Continuing with next");
      return;
    }
    if (serialPart == null) {
      LOG.warn("Could not find serial portion from path: "
          + serialDirPath.toString() + ". Continuing with next");
      return;
    }
    serialNumberIndex.remove(serialPart, timeStampPart);
  }

  private void addDirectoryToSerialNumberIndex(Path serialDirPath) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding " + serialDirPath + " to serial index");
    }
    String serialPart = serialDirPath.getName();
    String timestampPart = JobHistoryUtils
        .getTimestampPartFromPath(serialDirPath.toString());
    if (timestampPart == null) {
      LOG.warn("Could not find timestamp portion from path: " + serialDirPath
          + ". Continuing with next");
      return;
    }
    if (serialPart == null) {
      LOG.warn("Could not find serial portion from path: "
          + serialDirPath.toString() + ". Continuing with next");
    } else {
      serialNumberIndex.add(serialPart, timestampPart);
    }
  }

  private void addDirectoryToJobListCache(Path path) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding " + path + " to job list cache.");
    }
    List<FileStatus> historyFileList = scanDirectoryForHistoryFiles(path,
        doneDirFc);
    for (FileStatus fs : historyFileList) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding in history for " + fs.getPath());
      }
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
      String confFileName = JobHistoryUtils
          .getIntermediateConfFileName(jobIndexInfo.getJobId());
      String summaryFileName = JobHistoryUtils
          .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
      HistoryFileInfo fileInfo = createHistoryFileInfo(fs.getPath(), new Path(fs
          .getPath().getParent(), confFileName), new Path(fs.getPath()
          .getParent(), summaryFileName), jobIndexInfo, true);
      jobListCache.addIfAbsent(fileInfo);
    }
  }

  @VisibleForTesting
  protected static List<FileStatus> scanDirectory(Path path, FileContext fc,
      PathFilter pathFilter) throws IOException {
    path = fc.makeQualified(path);
    List<FileStatus> jhStatusList = new ArrayList<FileStatus>();
    try {
      RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
      while (fileStatusIter.hasNext()) {
        FileStatus fileStatus = fileStatusIter.next();
        Path filePath = fileStatus.getPath();
        if (fileStatus.isFile() && pathFilter.accept(filePath)) {
          jhStatusList.add(fileStatus);
        }
      }
    } catch (FileNotFoundException fe) {
      LOG.error("Error while scanning directory " + path, fe);
    }
    return jhStatusList;
  }

  protected List<FileStatus> scanDirectoryForHistoryFiles(Path path,
      FileContext fc) throws IOException {
    return scanDirectory(path, fc, JobHistoryUtils.getHistoryFileFilter());
  }
  
  /**
   * Finds all history directories with a timestamp component by scanning the
   * filesystem. Used when the JobHistory server is started.
   * 
   * @return list of history directories
   */
  protected List<FileStatus> findTimestampedDirectories() throws IOException {
    List<FileStatus> fsList = JobHistoryUtils.localGlobber(doneDirFc,
        doneDirPrefixPath, DONE_BEFORE_SERIAL_TAIL);
    return fsList;
  }

  /**
   * Scans the intermediate directory to find user directories. Scans these for
   * history files if the modification time for the directory has changed. Once
   * it finds history files it starts the process of moving them to the done 
   * directory.
   * 
   * @throws IOException
   *           if there was a error while scanning
   */
  void scanIntermediateDirectory() throws IOException {
    // TODO it would be great to limit how often this happens, except in the
    // case where we are looking for a particular job.
    List<FileStatus> userDirList = JobHistoryUtils.localGlobber(
        intermediateDoneDirFc, intermediateDoneDirPath, "");
    LOG.debug("Scanning intermediate dirs");
    for (FileStatus userDir : userDirList) {
      String name = userDir.getPath().getName();
      UserLogDir dir = userDirModificationTimeMap.get(name);
      if(dir == null) {
        dir = new UserLogDir();
        UserLogDir old = userDirModificationTimeMap.putIfAbsent(name, dir);
        if(old != null) {
          dir = old;
        }
      }
      dir.scanIfNeeded(userDir);
    }
  }

  /**
   * Scans the specified path and populates the intermediate cache.
   * 
   * @param absPath
   * @throws IOException
   */
  private void scanIntermediateDirectory(final Path absPath) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning intermediate dir " + absPath);
    }
    List<FileStatus> fileStatusList = scanDirectoryForHistoryFiles(absPath,
        intermediateDoneDirFc);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + fileStatusList.size() + " files");
    }
    for (FileStatus fs : fileStatusList) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("scanning file: "+ fs.getPath());
      }
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
      String confFileName = JobHistoryUtils
          .getIntermediateConfFileName(jobIndexInfo.getJobId());
      String summaryFileName = JobHistoryUtils
          .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
      HistoryFileInfo fileInfo = createHistoryFileInfo(fs.getPath(), new Path(fs
          .getPath().getParent(), confFileName), new Path(fs.getPath()
          .getParent(), summaryFileName), jobIndexInfo, false);

      final HistoryFileInfo old = jobListCache.addIfAbsent(fileInfo);
      if (old == null || old.didMoveFail()) {
        final HistoryFileInfo found = (old == null) ? fileInfo : old;
        long cutoff = System.currentTimeMillis() - maxHistoryAge;
        if(found.getJobIndexInfo().getFinishTime() <= cutoff) {
          try {
            found.delete();
          } catch (IOException e) {
            LOG.warn("Error cleaning up a HistoryFile that is out of date.", e);
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Scheduling move to done of " +found);
          }

          moveToDoneExecutor.execute(new Runnable() {
            @Override
            public void run() {
              try {
                found.moveToDone();
              } catch (IOException e) {
                LOG.info("Failed to process fileInfo for job: " + 
                    found.getJobId(), e);
              }
            }
          });
        }
      } else if (!old.isMovePending()) {
        //This is a duplicate so just delete it
        if (LOG.isDebugEnabled()) {
          LOG.debug("Duplicate: deleting");
        }
        fileInfo.delete();
      }
    }
  }

  /**
   * Searches the job history file FileStatus list for the specified JobId.
   * 
   * @param fileStatusList
   *          fileStatus list of Job History Files.
   * @param jobId
   *          The JobId to find.
   * @return A FileInfo object for the jobId, null if not found.
   * @throws IOException
   */
  private HistoryFileInfo getJobFileInfo(List<FileStatus> fileStatusList,
      JobId jobId) throws IOException {
    for (FileStatus fs : fileStatusList) {
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
      if (jobIndexInfo.getJobId().equals(jobId)) {
        String confFileName = JobHistoryUtils
            .getIntermediateConfFileName(jobIndexInfo.getJobId());
        String summaryFileName = JobHistoryUtils
            .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
        HistoryFileInfo fileInfo = createHistoryFileInfo(fs.getPath(), new Path(
            fs.getPath().getParent(), confFileName), new Path(fs.getPath()
            .getParent(), summaryFileName), jobIndexInfo, true);
        return fileInfo;
      }
    }
    return null;
  }

  /**
   * Scans old directories known by the idToDateString map for the specified
   * jobId. If the number of directories is higher than the supported size of
   * the idToDateString cache, the jobId will not be found.
   * 
   * @param jobId
   *          the jobId.
   * @return
   * @throws IOException
   */
  private HistoryFileInfo scanOldDirsForJob(JobId jobId) throws IOException {
    String boxedSerialNumber = JobHistoryUtils.serialNumberDirectoryComponent(
        jobId, serialNumberFormat);
    Set<String> dateStringSet = serialNumberIndex.get(boxedSerialNumber);
    if (dateStringSet == null) {
      return null;
    }
    for (String timestampPart : dateStringSet) {
      Path logDir = canonicalHistoryLogPath(jobId, timestampPart);
      List<FileStatus> fileStatusList = scanDirectoryForHistoryFiles(logDir,
          doneDirFc);
      HistoryFileInfo fileInfo = getJobFileInfo(fileStatusList, jobId);
      if (fileInfo != null) {
        return fileInfo;
      }
    }
    return null;
  }

  public Collection<HistoryFileInfo> getAllFileInfo() throws IOException {
    scanIntermediateDirectory();
    return jobListCache.values();
  }

  public HistoryFileInfo getFileInfo(JobId jobId) throws IOException {
    // FileInfo available in cache.
    HistoryFileInfo fileInfo = jobListCache.get(jobId);
    if (fileInfo != null) {
      return fileInfo;
    }
    // OK so scan the intermediate to be sure we did not lose it that way
    scanIntermediateDirectory();
    fileInfo = jobListCache.get(jobId);
    if (fileInfo != null) {
      return fileInfo;
    }

    // Intermediate directory does not contain job. Search through older ones.
    fileInfo = scanOldDirsForJob(jobId);
    if (fileInfo != null) {
      return fileInfo;
    }
    return null;
  }

  private void moveToDoneNow(final Path src, final Path target)
      throws IOException {
    LOG.info("Moving " + src.toString() + " to " + target.toString());
    try {
      intermediateDoneDirFc.rename(src, target, Options.Rename.NONE);
    } catch (FileNotFoundException e) {
      if (doneDirFc.util().exists(target)) {
        LOG.info("Source file " + src.toString() + " not found, but target "
            + "file " + target.toString() + " already exists. Move already "
            + "happened.");
      } else {
        throw e;
      }
    }
  }

  private String getJobSummary(FileContext fc, Path path) throws IOException {
    Path qPath = fc.makeQualified(path);
    FSDataInputStream in = null;
    String jobSummaryString = null;
    try {
      in = fc.open(qPath);
      jobSummaryString = in.readUTF();
    } finally {
      if (in != null) {
        in.close();
      }
    }
    return jobSummaryString;
  }

  private void makeDoneSubdir(Path path) throws IOException {
    try {
      doneDirFc.getFileStatus(path);
      existingDoneSubdirs.add(path);
    } catch (FileNotFoundException fnfE) {
      try {
        FsPermission fsp = new FsPermission(
            JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION);
        doneDirFc.mkdir(path, fsp, true);
        FileStatus fsStatus = doneDirFc.getFileStatus(path);
        LOG.info("Perms after creating " + fsStatus.getPermission().toShort()
            + ", Expected: " + fsp.toShort());
        if (fsStatus.getPermission().toShort() != fsp.toShort()) {
          LOG.info("Explicitly setting permissions to : " + fsp.toShort()
              + ", " + fsp);
          doneDirFc.setPermission(path, fsp);
        }
        existingDoneSubdirs.add(path);
      } catch (FileAlreadyExistsException faeE) { // Nothing to do.
      }
    }
  }

  private Path canonicalHistoryLogPath(JobId id, String timestampComponent) {
    return new Path(doneDirPrefixPath, JobHistoryUtils.historyLogSubdirectory(
        id, timestampComponent, serialNumberFormat));
  }

  private Path canonicalHistoryLogPath(JobId id, long millisecondTime) {
    String timestampComponent = JobHistoryUtils
        .timestampDirectoryComponent(millisecondTime);
    return new Path(doneDirPrefixPath, JobHistoryUtils.historyLogSubdirectory(
        id, timestampComponent, serialNumberFormat));
  }

  private long getEffectiveTimestamp(long finishTime, FileStatus fileStatus) {
    if (finishTime == 0) {
      return fileStatus.getModificationTime();
    }
    return finishTime;
  }

  private void deleteJobFromDone(HistoryFileInfo fileInfo) throws IOException {
    jobListCache.delete(fileInfo);
    fileInfo.delete();
  }

  List<FileStatus> getHistoryDirsForCleaning(long cutoff) throws IOException {
      return JobHistoryUtils.
        getHistoryDirsForCleaning(doneDirFc, doneDirPrefixPath, cutoff);
  }

  /**
   * Clean up older history files.
   * 
   * @throws IOException
   *           on any error trying to remove the entries.
   */
  @SuppressWarnings("unchecked")
  void clean() throws IOException {
    long cutoff = System.currentTimeMillis() - maxHistoryAge;
    boolean halted = false;
    List<FileStatus> serialDirList = getHistoryDirsForCleaning(cutoff);
    // Sort in ascending order. Relies on YYYY/MM/DD/Serial
    Collections.sort(serialDirList);
    for (FileStatus serialDir : serialDirList) {
      List<FileStatus> historyFileList = scanDirectoryForHistoryFiles(
          serialDir.getPath(), doneDirFc);
      for (FileStatus historyFile : historyFileList) {
        JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(historyFile
            .getPath().getName());
        long effectiveTimestamp = getEffectiveTimestamp(
            jobIndexInfo.getFinishTime(), historyFile);
        if (effectiveTimestamp <= cutoff) {
          HistoryFileInfo fileInfo = this.jobListCache.get(jobIndexInfo
              .getJobId());
          if (fileInfo == null) {
            String confFileName = JobHistoryUtils
                .getIntermediateConfFileName(jobIndexInfo.getJobId());

            fileInfo = createHistoryFileInfo(historyFile.getPath(), new Path(
                historyFile.getPath().getParent(), confFileName), null,
                jobIndexInfo, true);
          }
          deleteJobFromDone(fileInfo);
        } else {
          halted = true;
          break;
        }
      }
      if (!halted) {
        deleteDir(serialDir);
        removeDirectoryFromSerialNumberIndex(serialDir.getPath());
        existingDoneSubdirs.remove(serialDir.getPath());
      } else {
        break; // Don't scan any more directories.
      }
    }
  }
  
  protected boolean deleteDir(FileStatus serialDir)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return doneDirFc.delete(doneDirFc.makeQualified(serialDir.getPath()), true);
  }

  // for test
  @VisibleForTesting
  void setMaxHistoryAge(long newValue){
    maxHistoryAge=newValue;
  }
}
