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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapreduce.jobhistory.JobSummary;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class provides a way to interact with history files in a thread safe
 * manor.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class HistoryFileManager extends AbstractService {
  private static final Log LOG = LogFactory.getLog(HistoryFileManager.class);
  private static final Log SUMMARY_LOG = LogFactory.getLog(JobSummary.class);

  private static enum HistoryInfoState {
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

  static class JobListCache {
    private ConcurrentSkipListMap<JobId, HistoryFileInfo> cache;
    private int maxSize;
    private long maxAge;

    public JobListCache(int maxSize, long maxAge) {
      this.maxSize = maxSize;
      this.maxAge = maxAge;
      this.cache = new ConcurrentSkipListMap<JobId, HistoryFileInfo>();
    }

    public HistoryFileInfo addIfAbsent(HistoryFileInfo fileInfo) {
      JobId jobId = fileInfo.getJobIndexInfo().getJobId();
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
        while(cache.size() > maxSize && keys.hasNext()) {
          JobId key = keys.next();
          HistoryFileInfo firstValue = cache.get(key);
          if(firstValue != null) {
            synchronized(firstValue) {
              if (firstValue.isMovePending()) {
                if(firstValue.didMoveFail() && 
                    firstValue.jobIndexInfo.getFinishTime() <= cutoff) {
                  cache.remove(key);
                  //Now lets try to delete it
                  try {
                    firstValue.delete();
                  } catch (IOException e) {
                    LOG.error("Error while trying to delete history files" +
                    		" that could not be moved to done.", e);
                  }
                } else {
                  LOG.warn("Waiting to remove " + key
                      + " from JobListCache because it is not in done yet.");
                }
              } else {
                cache.remove(key);
              }
            }
          }
        }
      }
      return old;
    }

    public void delete(HistoryFileInfo fileInfo) {
      cache.remove(fileInfo.getJobId());
    }

    public Collection<HistoryFileInfo> values() {
      return new ArrayList<HistoryFileInfo>(cache.values());
    }

    public HistoryFileInfo get(JobId jobId) {
      return cache.get(jobId);
    }
  }

  /**
   * This class represents a user dir in the intermediate done directory.  This
   * is mostly for locking purposes. 
   */
  private class UserLogDir {
    long modTime = 0;
    
    public synchronized void scanIfNeeded(FileStatus fs) {
      long newModTime = fs.getModificationTime();
      if (modTime != newModTime) {
        Path p = fs.getPath();
        try {
          scanIntermediateDirectory(p);
          //If scanning fails, we will scan again.  We assume the failure is
          // temporary.
          modTime = newModTime;
        } catch (IOException e) {
          LOG.error("Error while trying to scan the directory " + p, e);
        }
      }
    }
  }
  
  public class HistoryFileInfo {
    private Path historyFile;
    private Path confFile;
    private Path summaryFile;
    private JobIndexInfo jobIndexInfo;
    private HistoryInfoState state;

    private HistoryFileInfo(Path historyFile, Path confFile, Path summaryFile,
        JobIndexInfo jobIndexInfo, boolean isInDone) {
      this.historyFile = historyFile;
      this.confFile = confFile;
      this.summaryFile = summaryFile;
      this.jobIndexInfo = jobIndexInfo;
      state = isInDone ? HistoryInfoState.IN_DONE
          : HistoryInfoState.IN_INTERMEDIATE;
    }

    @VisibleForTesting
    synchronized boolean isMovePending() {
      return state == HistoryInfoState.IN_INTERMEDIATE
          || state == HistoryInfoState.MOVE_FAILED;
    }

    @VisibleForTesting
    synchronized boolean didMoveFail() {
      return state == HistoryInfoState.MOVE_FAILED;
    }
    
    /**
     * @return true if the files backed by this were deleted.
     */
    public synchronized boolean isDeleted() {
      return state == HistoryInfoState.DELETED;
    }

    private synchronized void moveToDone() throws IOException {
      if (!isMovePending()) {
        // It was either deleted or is already in done. Either way do nothing
        return;
      }
      try {
        long completeTime = jobIndexInfo.getFinishTime();
        if (completeTime == 0) {
          completeTime = System.currentTimeMillis();
        }
        JobId jobId = jobIndexInfo.getJobId();

        List<Path> paths = new ArrayList<Path>(2);
        if (historyFile == null) {
          LOG.info("No file for job-history with " + jobId + " found in cache!");
        } else {
          paths.add(historyFile);
        }

        if (confFile == null) {
          LOG.info("No file for jobConf with " + jobId + " found in cache!");
        } else {
          paths.add(confFile);
        }

        if (summaryFile == null) {
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
      }
    }

    /**
     * Parse a job from the JobHistoryFile, if the underlying file is not going
     * to be deleted.
     * 
     * @return the Job or null if the underlying file was deleted.
     * @throws IOException
     *           if there is an error trying to read the file.
     */
    public synchronized Job loadJob() throws IOException {
      return new CompletedJob(conf, jobIndexInfo.getJobId(), historyFile,
          false, jobIndexInfo.getUser(), this, aclsMgr);
    }

    /**
     * Return the history file.  This should only be used for testing.
     * @return the history file.
     */
    synchronized Path getHistoryFile() {
      return historyFile;
    }
    
    private synchronized void delete() throws IOException {
      state = HistoryInfoState.DELETED;
      doneDirFc.delete(doneDirFc.makeQualified(historyFile), false);
      doneDirFc.delete(doneDirFc.makeQualified(confFile), false);
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
      jobConf.addResource(fc.open(confFile), confFile.toString());
      return jobConf;
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

  private Configuration conf;

  private String serialNumberFormat;

  private Path doneDirPrefixPath = null; // folder for completed jobs
  private FileContext doneDirFc; // done Dir FileContext

  private Path intermediateDoneDirPath = null; // Intermediate Done Dir Path
  private FileContext intermediateDoneDirFc; // Intermediate Done Dir
                                             // FileContext

  private ThreadPoolExecutor moveToDoneExecutor = null;
  private long maxHistoryAge = 0;
  
  public HistoryFileManager() {
    super(HistoryFileManager.class.getName());
  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;

    int serialNumberLowDigits = 3;
    serialNumberFormat = ("%0"
        + (JobHistoryUtils.SERIAL_NUMBER_DIRECTORY_DIGITS + serialNumberLowDigits)
        + "d");

    String doneDirPrefix = null;
    doneDirPrefix = JobHistoryUtils
        .getConfiguredHistoryServerDoneDirPrefix(conf);
    try {
      doneDirPrefixPath = FileContext.getFileContext(conf).makeQualified(
          new Path(doneDirPrefix));
      doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri(), conf);
      doneDirFc.setUMask(JobHistoryUtils.HISTORY_DONE_DIR_UMASK);
      mkdir(doneDirFc, doneDirPrefixPath, new FsPermission(
          JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION));
    } catch (IOException e) {
      throw new YarnException("Error creating done directory: ["
          + doneDirPrefixPath + "]", e);
    }

    String intermediateDoneDirPrefix = null;
    intermediateDoneDirPrefix = JobHistoryUtils
        .getConfiguredHistoryIntermediateDoneDirPrefix(conf);
    try {
      intermediateDoneDirPath = FileContext.getFileContext(conf).makeQualified(
          new Path(intermediateDoneDirPrefix));
      intermediateDoneDirFc = FileContext.getFileContext(
          intermediateDoneDirPath.toUri(), conf);
      mkdir(intermediateDoneDirFc, intermediateDoneDirPath, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS.toShort()));
    } catch (IOException e) {
      LOG.info("error creating done directory on dfs " + e);
      throw new YarnException("Error creating intermediate done directory: ["
          + intermediateDoneDirPath + "]", e);
    }

    this.aclsMgr = new JobACLsManager(conf);

    maxHistoryAge = conf.getLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS,
        JHAdminConfig.DEFAULT_MR_HISTORY_MAX_AGE);
    
    jobListCache = new JobListCache(conf.getInt(
        JHAdminConfig.MR_HISTORY_JOBLIST_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_JOBLIST_CACHE_SIZE),
        maxHistoryAge);

    serialNumberIndex = new SerialNumberIndex(conf.getInt(
        JHAdminConfig.MR_HISTORY_DATESTRING_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_DATESTRING_CACHE_SIZE));

    int numMoveThreads = conf.getInt(
        JHAdminConfig.MR_HISTORY_MOVE_THREAD_COUNT,
        JHAdminConfig.DEFAULT_MR_HISTORY_MOVE_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
        "MoveIntermediateToDone Thread #%d").build();
    moveToDoneExecutor = new ThreadPoolExecutor(numMoveThreads, numMoveThreads,
        1, TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>(), tf);

    super.init(conf);
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
    for (FileStatus fs : timestampedDirList) {
      // TODO Could verify the correct format for these directories.
      addDirectoryToSerialNumberIndex(fs.getPath());
      addDirectoryToJobListCache(fs.getPath());
    }
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
      HistoryFileInfo fileInfo = new HistoryFileInfo(fs.getPath(), new Path(fs
          .getPath().getParent(), confFileName), new Path(fs.getPath()
          .getParent(), summaryFileName), jobIndexInfo, true);
      jobListCache.addIfAbsent(fileInfo);
    }
  }

  private static List<FileStatus> scanDirectory(Path path, FileContext fc,
      PathFilter pathFilter) throws IOException {
    path = fc.makeQualified(path);
    List<FileStatus> jhStatusList = new ArrayList<FileStatus>();
    RemoteIterator<FileStatus> fileStatusIter = fc.listStatus(path);
    while (fileStatusIter.hasNext()) {
      FileStatus fileStatus = fileStatusIter.next();
      Path filePath = fileStatus.getPath();
      if (fileStatus.isFile() && pathFilter.accept(filePath)) {
        jhStatusList.add(fileStatus);
      }
    }
    return jhStatusList;
  }

  private static List<FileStatus> scanDirectoryForHistoryFiles(Path path,
      FileContext fc) throws IOException {
    return scanDirectory(path, fc, JobHistoryUtils.getHistoryFileFilter());
  }

  /**
   * Finds all history directories with a timestamp component by scanning the
   * filesystem. Used when the JobHistory server is started.
   * 
   * @return
   */
  private List<FileStatus> findTimestampedDirectories() throws IOException {
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
    List<FileStatus> fileStatusList = scanDirectoryForHistoryFiles(absPath,
        intermediateDoneDirFc);
    for (FileStatus fs : fileStatusList) {
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
      String confFileName = JobHistoryUtils
          .getIntermediateConfFileName(jobIndexInfo.getJobId());
      String summaryFileName = JobHistoryUtils
          .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
      HistoryFileInfo fileInfo = new HistoryFileInfo(fs.getPath(), new Path(fs
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
      } else if (old != null && !old.isMovePending()) {
        //This is a duplicate so just delete it
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
        HistoryFileInfo fileInfo = new HistoryFileInfo(fs.getPath(), new Path(
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
    intermediateDoneDirFc.rename(src, target, Options.Rename.NONE);
  }

  private String getJobSummary(FileContext fc, Path path) throws IOException {
    Path qPath = fc.makeQualified(path);
    FSDataInputStream in = fc.open(qPath);
    String jobSummaryString = in.readUTF();
    in.close();
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

  /**
   * Clean up older history files.
   * 
   * @throws IOException
   *           on any error trying to remove the entries.
   */
  @SuppressWarnings("unchecked")
  void clean() throws IOException {
    // TODO this should be replaced by something that knows about the directory
    // structure and will put less of a load on HDFS.
    long cutoff = System.currentTimeMillis() - maxHistoryAge;
    boolean halted = false;
    // TODO Delete YYYY/MM/DD directories.
    List<FileStatus> serialDirList = findTimestampedDirectories();
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

            fileInfo = new HistoryFileInfo(historyFile.getPath(), new Path(
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
        doneDirFc.delete(doneDirFc.makeQualified(serialDir.getPath()), true);
        removeDirectoryFromSerialNumberIndex(serialDir.getPath());
        existingDoneSubdirs.remove(serialDir.getPath());
      } else {
        break; // Don't scan any more directories.
      }
    }
  }
}
