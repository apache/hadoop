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
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobSummary;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.AbstractService;


/*
 * Loads and manages the Job history cache.
 */
public class JobHistory extends AbstractService implements HistoryContext   {

  private static final int DEFAULT_JOBLIST_CACHE_SIZE = 20000;
  private static final int DEFAULT_LOADEDJOB_CACHE_SIZE = 2000;
  private static final int DEFAULT_DATESTRING_CACHE_SIZE = 200000;
  private static final long DEFAULT_MOVE_THREAD_INTERVAL = 3 * 60 * 1000l; //3 minutes
  private static final int DEFAULT_MOVE_THREAD_COUNT = 3;
  
  static final long DEFAULT_HISTORY_MAX_AGE = 7 * 24 * 60 * 60 * 1000L; //1 week
  static final long DEFAULT_RUN_INTERVAL = 1 * 24 * 60 * 60 * 1000l; //1 day
  
  private static final Log LOG = LogFactory.getLog(JobHistory.class);

  private static final Log SUMMARY_LOG = LogFactory.getLog(JobSummary.class);

  private static final Pattern DATE_PATTERN = Pattern
      .compile("([0-1]?[0-9])/([0-3]?[0-9])/((?:2[0-9])[0-9][0-9])");

  /*
   * TODO Get rid of this once JobId has it's own comparator
   */
  private static final Comparator<JobId> JOB_ID_COMPARATOR = new Comparator<JobId>() {
    @Override
    public int compare(JobId o1, JobId o2) {
      if (o1.getAppId().getClusterTimestamp() > o2.getAppId().getClusterTimestamp()) {
        return 1;
      } else if (o1.getAppId().getClusterTimestamp() < o2.getAppId().getClusterTimestamp()) {
        return -1;
      } else {
        return o1.getId() - o2.getId();
      }
    }
  };
  
  private static String DONE_BEFORE_SERIAL_TAIL = JobHistoryUtils.doneSubdirsBeforeSerialTail();
  
  /**
   * Maps between a serial number (generated based on jobId) and the timestamp
   * component(s) to which it belongs.
   * Facilitates jobId based searches.
   * If a jobId is not found in this list - it will not be found.
   */
  private final SortedMap<String, Set<String>> idToDateString = new ConcurrentSkipListMap<String, Set<String>>();

  //Maintains minimal details for recent jobs (parsed from history file name).
  //Sorted on Job Completion Time.
  private final SortedMap<JobId, MetaInfo> jobListCache = new ConcurrentSkipListMap<JobId, MetaInfo>(
      JOB_ID_COMPARATOR);
  
  
  // Re-use exisiting MetaInfo objects if they exist for the specific JobId. (synchronization on MetaInfo)
  // Check for existance of the object when using iterators.
  private final SortedMap<JobId, MetaInfo> intermediateListCache = new ConcurrentSkipListMap<JobId, JobHistory.MetaInfo>(
      JOB_ID_COMPARATOR);
  
  //Maintains a list of known done subdirectories. Not currently used.
  private final Set<Path> existingDoneSubdirs = new HashSet<Path>();
  
  private final SortedMap<JobId, Job> loadedJobCache = new ConcurrentSkipListMap<JobId, Job>(
      JOB_ID_COMPARATOR);

  /**
   * Maintains a mapping between intermediate user directories and the last known modification time.
   */
  private Map<String, Long> userDirModificationTimeMap = new HashMap<String, Long>();
  
  //The number of jobs to maintain in the job list cache.
  private int jobListCacheSize;
  
  //The number of loaded jobs.
  private int loadedJobCacheSize;
  
  //The number of entries in idToDateString
  private int dateStringCacheSize;

  //Time interval for the move thread.
  private long moveThreadInterval;
  
  //Number of move threads.
  private int numMoveThreads;
  
  private Configuration conf;

  private boolean debugMode;
  private int serialNumberLowDigits;
  private String serialNumberFormat;
  

  private Path doneDirPrefixPath = null; // folder for completed jobs
  private FileContext doneDirFc; // done Dir FileContext
  
  private Path intermediateDoneDirPath = null; //Intermediate Done Dir Path
  private FileContext intermediateDoneDirFc; //Intermediate Done Dir FileContext

  private Thread moveIntermediateToDoneThread = null;
  private MoveIntermediateToDoneRunnable moveIntermediateToDoneRunnable = null;
  private ScheduledThreadPoolExecutor cleanerScheduledExecutor = null;
  
  /**
   * Writes out files to the path
   * .....${DONE_DIR}/VERSION_STRING/YYYY/MM/DD/HH/SERIAL_NUM/jh{index_entries}.jhist
   */

  @Override
  public void init(Configuration conf) throws YarnException {
    LOG.info("JobHistory Init");
    this.conf = conf;
    this.appID = RecordFactoryProvider.getRecordFactory(conf)
        .newRecordInstance(ApplicationId.class);
    this.appAttemptID = RecordFactoryProvider.getRecordFactory(conf)
    .newRecordInstance(ApplicationAttemptId.class);

    debugMode = conf.getBoolean(JHAdminConfig.MR_HISTORY_DEBUG_MODE, false);
    serialNumberLowDigits = debugMode ? 1 : 3;
    serialNumberFormat = ("%0"
        + (JobHistoryUtils.SERIAL_NUMBER_DIRECTORY_DIGITS + serialNumberLowDigits) + "d");

    String doneDirPrefix = null;
    doneDirPrefix = JobHistoryUtils.getConfiguredHistoryServerDoneDirPrefix(conf);
    try {
      doneDirPrefixPath = FileContext.getFileContext(conf).makeQualified(
          new Path(doneDirPrefix));
      doneDirFc = FileContext.getFileContext(doneDirPrefixPath.toUri(), conf);
      mkdir(doneDirFc, doneDirPrefixPath, new FsPermission(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION));
    } catch (IOException e) {
      throw new YarnException("Error creating done directory: [" + doneDirPrefixPath + "]", e);
    }

    String intermediateDoneDirPrefix = null;
    intermediateDoneDirPrefix = JobHistoryUtils
        .getConfiguredHistoryIntermediateDoneDirPrefix(conf);
    try {
      intermediateDoneDirPath = FileContext.getFileContext(conf)
          .makeQualified(new Path(intermediateDoneDirPrefix));
      intermediateDoneDirFc = FileContext.getFileContext(
          intermediateDoneDirPath.toUri(), conf);
      mkdir(intermediateDoneDirFc, intermediateDoneDirPath, new FsPermission(JobHistoryUtils.HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS.toShort()));
    } catch (IOException e) {
      LOG.info("error creating done directory on dfs " + e);
      throw new YarnException("Error creating intermediate done directory: [" + intermediateDoneDirPath + "]", e);
    }
    
    
    
    jobListCacheSize = conf.getInt(JHAdminConfig.MR_HISTORY_JOBLIST_CACHE_SIZE, DEFAULT_JOBLIST_CACHE_SIZE);
    loadedJobCacheSize = conf.getInt(JHAdminConfig.MR_HISTORY_LOADED_JOB_CACHE_SIZE, DEFAULT_LOADEDJOB_CACHE_SIZE);
    dateStringCacheSize = conf.getInt(JHAdminConfig.MR_HISTORY_DATESTRING_CACHE_SIZE, DEFAULT_DATESTRING_CACHE_SIZE);
    moveThreadInterval =
        conf.getLong(JHAdminConfig.MR_HISTORY_MOVE_INTERVAL_MS,
            DEFAULT_MOVE_THREAD_INTERVAL);
    numMoveThreads = conf.getInt(JHAdminConfig.MR_HISTORY_MOVE_THREAD_COUNT, DEFAULT_MOVE_THREAD_COUNT);
    try {
    initExisting();
    } catch (IOException e) {
      throw new YarnException("Failed to intialize existing directories", e);
    }
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

  @Override
  public void start() {
    //Start moveIntermediatToDoneThread
    moveIntermediateToDoneRunnable = new MoveIntermediateToDoneRunnable(moveThreadInterval, numMoveThreads);
    moveIntermediateToDoneThread = new Thread(moveIntermediateToDoneRunnable);
    moveIntermediateToDoneThread.setName("MoveIntermediateToDoneScanner");
    moveIntermediateToDoneThread.start();
    
    //Start historyCleaner
    boolean startCleanerService = conf.getBoolean(JHAdminConfig.MR_HISTORY_CLEANER_ENABLE, true);
    if (startCleanerService) {
      long maxAgeOfHistoryFiles = conf.getLong(JHAdminConfig.MR_HISTORY_MAX_AGE_MS,
          DEFAULT_HISTORY_MAX_AGE);
    cleanerScheduledExecutor = new ScheduledThreadPoolExecutor(1);
      long runInterval = conf.getLong(JHAdminConfig.MR_HISTORY_CLEANER_INTERVAL_MS,
          DEFAULT_RUN_INTERVAL);
      cleanerScheduledExecutor
          .scheduleAtFixedRate(new HistoryCleaner(maxAgeOfHistoryFiles),
              30 * 1000l, runInterval, TimeUnit.MILLISECONDS);
    }
    super.start();
  }
  
  @Override
  public void stop() {
    LOG.info("Stopping JobHistory");
    if (moveIntermediateToDoneThread != null) {
      LOG.info("Stopping move thread");
      moveIntermediateToDoneRunnable.stop();
      moveIntermediateToDoneThread.interrupt();
      try {
        LOG.info("Joining on move thread");
        moveIntermediateToDoneThread.join();
      } catch (InterruptedException e) {
        LOG.info("Interrupted while stopping move thread");
      }
    }

    if (cleanerScheduledExecutor != null) {
      LOG.info("Stopping History Cleaner");
      cleanerScheduledExecutor.shutdown();
      boolean interrupted = false;
      long currentTime = System.currentTimeMillis();
      while (!cleanerScheduledExecutor.isShutdown()
          && System.currentTimeMillis() > currentTime + 1000l && !interrupted) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (!cleanerScheduledExecutor.isShutdown()) {
        LOG.warn("HistoryCleanerService shutdown may not have succeeded");
      }
    }
    super.stop();
  }
  
  public JobHistory() {
    super(JobHistory.class.getName());
  }
  
  /**
   * Populates index data structures.
   * Should only be called at initialization times.
   */
  @SuppressWarnings("unchecked")
  private void initExisting() throws IOException {
    List<FileStatus> timestampedDirList = findTimestampedDirectories();
    Collections.sort(timestampedDirList);
    for (FileStatus fs : timestampedDirList) {
      //TODO Could verify the correct format for these directories.
      addDirectoryToSerialNumberIndex(fs.getPath());
      addDirectoryToJobListCache(fs.getPath());
    }
  }
  
  private void removeDirectoryFromSerialNumberIndex(Path serialDirPath) {
    String serialPart = serialDirPath.getName();
    String timeStampPart = JobHistoryUtils.getTimestampPartFromPath(serialDirPath.toString());
    if (timeStampPart == null) {
      LOG.warn("Could not find timestamp portion from path: " + serialDirPath.toString() +". Continuing with next");
      return;
    }
    if (serialPart == null) {
      LOG.warn("Could not find serial portion from path: " + serialDirPath.toString() + ". Continuing with next");
      return;
    }
    if (idToDateString.containsKey(serialPart)) {
      Set<String> set = idToDateString.get(serialPart);
      set.remove(timeStampPart);
      if (set.isEmpty()) {
        idToDateString.remove(serialPart);
      }
    }

  }
  
  private void addDirectoryToSerialNumberIndex(Path serialDirPath) {
    String serialPart = serialDirPath.getName();
    String timestampPart = JobHistoryUtils.getTimestampPartFromPath(serialDirPath.toString());
    if (timestampPart == null) {
      LOG.warn("Could not find timestamp portion from path: " + serialDirPath.toString() +". Continuing with next");
      return;
    }
    if (serialPart == null) {
      LOG.warn("Could not find serial portion from path: " + serialDirPath.toString() + ". Continuing with next");
    }
    addToSerialNumberIndex(serialPart, timestampPart);
  }

  private void addToSerialNumberIndex(String serialPart, String timestampPart) {
      if (!idToDateString.containsKey(serialPart)) {
        idToDateString.put(serialPart, new HashSet<String>());
        if (idToDateString.size() > dateStringCacheSize) {
          idToDateString.remove(idToDateString.firstKey());
        }
      Set<String> datePartSet = idToDateString.get(serialPart);
      datePartSet.add(timestampPart);
    }
  }
  
  private void addDirectoryToJobListCache(Path path) throws IOException {
    List<FileStatus> historyFileList = scanDirectoryForHistoryFiles(path,
        doneDirFc);
    for (FileStatus fs : historyFileList) {
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
      String confFileName = JobHistoryUtils
          .getIntermediateConfFileName(jobIndexInfo.getJobId());
      String summaryFileName = JobHistoryUtils
          .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
      MetaInfo metaInfo = new MetaInfo(fs.getPath(), new Path(fs.getPath()
          .getParent(), confFileName), new Path(fs.getPath().getParent(),
          summaryFileName), jobIndexInfo);
      addToJobListCache(jobIndexInfo.getJobId(), metaInfo);
    }
  }
  
  private static List<FileStatus> scanDirectory(Path path, FileContext fc, PathFilter pathFilter) throws IOException {
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
  
  private static List<FileStatus> scanDirectoryForHistoryFiles(Path path, FileContext fc) throws IOException {
    return scanDirectory(path, fc, JobHistoryUtils.getHistoryFileFilter());
  }
  
  /**
   * Finds all history directories with a timestamp component by scanning 
   * the filesystem.
   * Used when the JobHistory server is started.
   * @return
   */
  private List<FileStatus> findTimestampedDirectories() throws IOException {
    List<FileStatus> fsList = JobHistoryUtils.localGlobber(doneDirFc, doneDirPrefixPath, DONE_BEFORE_SERIAL_TAIL);
    return fsList;
  }
    
  /**
   * Adds an entry to the job list cache. Maintains the size.
   */
  private void addToJobListCache(JobId jobId, MetaInfo metaInfo) {
    jobListCache.put(jobId, metaInfo);
    if (jobListCache.size() > jobListCacheSize) {
      jobListCache.remove(jobListCache.firstKey());
    }
  }

  /**
   * Adds an entry to the loaded job cache. Maintains the size.
   */
  private void  addToLoadedJobCache(Job job) {
    loadedJobCache.put(job.getID(), job);
    if (loadedJobCache.size() > loadedJobCacheSize ) {
      loadedJobCache.remove(loadedJobCache.firstKey());
    }
  }
  
  
  /**
   * Scans the intermediate directory to find user directories. Scans these
   * for history files if the modification time for the directory has changed.
   * @throws IOException
   */
  private void scanIntermediateDirectory() throws IOException {
    List<FileStatus> userDirList = JobHistoryUtils.localGlobber(intermediateDoneDirFc, intermediateDoneDirPath, "");
    
    for (FileStatus userDir : userDirList) {
      String name = userDir.getPath().getName();
      long newModificationTime = userDir.getModificationTime();
      boolean shouldScan = false;
      synchronized (userDirModificationTimeMap) {
        if (!userDirModificationTimeMap.containsKey(name) || newModificationTime > userDirModificationTimeMap.get(name)) {
            shouldScan = true;
            userDirModificationTimeMap.put(name, newModificationTime);
        }  
        }  
      if (shouldScan) {
        scanIntermediateDirectory(userDir.getPath());
      }
    }
  }

  /**
   * Scans the specified path and populates the intermediate cache.
   * @param absPath
   * @throws IOException
   */
  private void scanIntermediateDirectory(final Path absPath)
      throws IOException {
    List<FileStatus> fileStatusList = scanDirectoryForHistoryFiles(absPath,
        intermediateDoneDirFc);
    for (FileStatus fs : fileStatusList) {
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
      String confFileName = JobHistoryUtils
          .getIntermediateConfFileName(jobIndexInfo.getJobId());
      String summaryFileName = JobHistoryUtils
          .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
      MetaInfo metaInfo = new MetaInfo(fs.getPath(), new Path(fs.getPath()
          .getParent(), confFileName), new Path(fs.getPath().getParent(),
          summaryFileName), jobIndexInfo);
      if (!intermediateListCache.containsKey(jobIndexInfo.getJobId())) {
        intermediateListCache.put(jobIndexInfo.getJobId(), metaInfo);
      }
    }
  }
  
  /**
   * Searches the job history file FileStatus list for the specified JobId.
   * 
   * @param fileStatusList fileStatus list of Job History Files.
   * @param jobId The JobId to find.
   * @param checkForDoneFile whether to check for the existance of a done file.
   * @return A MetaInfo object for the jobId, null if not found.
   * @throws IOException
   */
  private MetaInfo getJobMetaInfo(List<FileStatus> fileStatusList, JobId jobId) throws IOException {
    for (FileStatus fs : fileStatusList) {
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath().getName());
      if (jobIndexInfo.getJobId().equals(jobId)) {
        String confFileName = JobHistoryUtils
            .getIntermediateConfFileName(jobIndexInfo.getJobId());
        String summaryFileName = JobHistoryUtils
            .getIntermediateSummaryFileName(jobIndexInfo.getJobId());
        MetaInfo metaInfo = new MetaInfo(fs.getPath(), new Path(fs.getPath()
            .getParent(), confFileName), new Path(fs.getPath().getParent(),
            summaryFileName), jobIndexInfo);
        return metaInfo;
      }
    }
    return null;
  }
  
  /**
   * Scans old directories known by the idToDateString map for the specified 
   * jobId.
   * If the number of directories is higher than the supported size of the
   * idToDateString cache, the jobId will not be found.
   * @param jobId the jobId.
   * @return
   * @throws IOException
   */
  private MetaInfo scanOldDirsForJob(JobId jobId) throws IOException {
    int jobSerialNumber = JobHistoryUtils.jobSerialNumber(jobId);
    String boxedSerialNumber = String.valueOf(jobSerialNumber);
    Set<String> dateStringSet = idToDateString.get(boxedSerialNumber);
    if (dateStringSet == null) {
      return null;
    }
    for (String timestampPart : dateStringSet) {
      Path logDir = canonicalHistoryLogPath(jobId, timestampPart);
      List<FileStatus> fileStatusList = scanDirectoryForHistoryFiles(logDir, doneDirFc);
      MetaInfo metaInfo = getJobMetaInfo(fileStatusList, jobId);
      if (metaInfo != null) {
        return metaInfo;
      }
    }
    return null;
  }
  
  /**
   * Checks for the existence of the job history file in the interemediate directory.
   * @param jobId
   * @return
   * @throws IOException
   */
  private MetaInfo scanIntermediateForJob(JobId jobId) throws IOException {
    scanIntermediateDirectory();
    return intermediateListCache.get(jobId);
  }

  @Override
  public String getApplicationName() {
    return "Job History Server";
  }

  private class MoveIntermediateToDoneRunnable implements Runnable {

    private long sleepTime;
    private ThreadPoolExecutor moveToDoneExecutor = null;
    private boolean running = false;
    
    public void stop() {
      running = false;
    }
    
    MoveIntermediateToDoneRunnable(long sleepTime, int numMoveThreads) {
      this.sleepTime = sleepTime;
      moveToDoneExecutor = new ThreadPoolExecutor(1, numMoveThreads, 1, TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
      running = true;
    }
  
  @Override
    public void run() {
      Thread.currentThread().setName("IntermediateHistoryScanner");
      try {
        while (running) {
          LOG.info("Starting scan to move intermediate done files");
          scanIntermediateDirectory();
          for (final MetaInfo metaInfo : intermediateListCache.values()) {
            moveToDoneExecutor.execute(new Runnable() {
              @Override
              public void run() {
                try {
                moveToDone(metaInfo);
                } catch (IOException e) {
                  LOG.info("Failed to process metaInfo for job: " + metaInfo.jobIndexInfo.getJobId(), e);
                }
              }
            });

          }
          synchronized (this) { // TODO Is this really required.
            try {
              this.wait(sleepTime);
            } catch (InterruptedException e) {
              LOG.info("IntermediateHistoryScannerThread interrupted");
            }
          }
        }
      } catch (IOException e) {
        LOG.warn("Unable to get a list of intermediate files to be moved from: "
            + intermediateDoneDirPath);
      }
    }
  }
  
  private Job loadJob(MetaInfo metaInfo) {
    synchronized(metaInfo) {
      try {
        Job job = new CompletedJob(conf, metaInfo.getJobIndexInfo().getJobId(), 
            metaInfo.getHistoryFile(), true, metaInfo.getJobIndexInfo().getUser());
        addToLoadedJobCache(job);
        return job;
      } catch (IOException e) {
        throw new YarnException("Could not find/load job: " + metaInfo.getJobIndexInfo().getJobId(), e);
      }
    }
  }
  
  private SortedMap<JobId, JobIndexInfo> getAllJobsMetaInfo() {
    SortedMap<JobId, JobIndexInfo> result = new TreeMap<JobId, JobIndexInfo>(JOB_ID_COMPARATOR);
      try {
      scanIntermediateDirectory();
      } catch (IOException e) {
      LOG.warn("Failed to scan intermediate directory", e);
        throw new YarnException(e);
      }
    for (JobId jobId : intermediateListCache.keySet()) {
      MetaInfo mi = intermediateListCache.get(jobId);
      if (mi != null) {
        result.put(jobId, mi.getJobIndexInfo());
      }
    }
    for (JobId jobId : jobListCache.keySet()) {
      MetaInfo mi = jobListCache.get(jobId);
      if (mi != null) {
        result.put(jobId, mi.getJobIndexInfo());
      }
    }
    return result;
  }
  
  private Map<JobId, Job> getAllJobsInternal() {
    //TODO This should ideally be using getAllJobsMetaInfo
    // or get rid of that method once Job has APIs for user, finishTime etc.
    SortedMap<JobId, Job> result = new TreeMap<JobId, Job>(JOB_ID_COMPARATOR);
    try {
      scanIntermediateDirectory();
    } catch (IOException e) {
      LOG.warn("Failed to scan intermediate directory", e);
      throw new YarnException(e);
    }
    for (JobId jobId : intermediateListCache.keySet()) {
      MetaInfo mi = intermediateListCache.get(jobId);
      if (mi != null) {
        result.put(jobId, new PartialJob(mi.getJobIndexInfo(), mi
            .getJobIndexInfo().getJobId()));
      }
    }
    for (JobId jobId : jobListCache.keySet()) {
      MetaInfo mi = jobListCache.get(jobId);
      if (mi != null) {
        result.put(jobId, new PartialJob(mi.getJobIndexInfo(), mi
            .getJobIndexInfo().getJobId()));
      }
    }
    return result;
  }

  /**
   * Helper method for test cases.
   */
  MetaInfo getJobMetaInfo(JobId jobId) throws IOException {
    //MetaInfo available in cache.
    MetaInfo metaInfo = null;
    if (jobListCache.containsKey(jobId)) {
      metaInfo = jobListCache.get(jobId);
    }

    if (metaInfo != null) {
      return metaInfo;
    }
    
    //MetaInfo not available. Check intermediate directory for meta info.
    metaInfo = scanIntermediateForJob(jobId);
    if (metaInfo != null) {
      return metaInfo;
    }
    
    //Intermediate directory does not contain job. Search through older ones.
    metaInfo = scanOldDirsForJob(jobId);
    if (metaInfo != null) {
      return metaInfo;
    }
    return null;
  }
  
  private Job findJob(JobId jobId) throws IOException {
    //Job already loaded.
    if (loadedJobCache.containsKey(jobId)) {
      return loadedJobCache.get(jobId);        
    }
    
    //MetaInfo available in cache.
    MetaInfo metaInfo = null;
    if (jobListCache.containsKey(jobId)) {
      metaInfo = jobListCache.get(jobId);
    }

    if (metaInfo != null) {
      return loadJob(metaInfo);
    }
    
    //MetaInfo not available. Check intermediate directory for meta info.
    metaInfo = scanIntermediateForJob(jobId);
    if (metaInfo != null) {
      return loadJob(metaInfo);
    }
    
    //Intermediate directory does not contain job. Search through older ones.
    metaInfo = scanOldDirsForJob(jobId);
    if (metaInfo != null) {
      return loadJob(metaInfo);
    }
    return null;
  }
  
  /**
   * Searches cached jobs for the specified criteria (AND). Ignores the criteria if null.
   * @param soughtUser
   * @param soughtJobNameSubstring
   * @param soughtDateStrings
   * @return
   */
  private Map<JobId, Job> findJobs(String soughtUser, String soughtJobNameSubstring, String[] soughtDateStrings) {
    boolean searchUser = true;
    boolean searchJobName = true;
    boolean searchDates = true;
    List<Calendar> soughtCalendars = null;
    
    if (soughtUser == null) {
      searchUser = false;
    }
    if (soughtJobNameSubstring == null) {
      searchJobName = false; 
    }
    if (soughtDateStrings == null) {
      searchDates = false;
    } else {
      soughtCalendars = getSoughtDateAsCalendar(soughtDateStrings);
    }
    
    Map<JobId, Job> resultMap = new TreeMap<JobId, Job>();
    
    SortedMap<JobId, JobIndexInfo> allJobs = getAllJobsMetaInfo();
    for (Map.Entry<JobId, JobIndexInfo> entry : allJobs.entrySet()) {
      JobId jobId = entry.getKey();
      JobIndexInfo indexInfo = entry.getValue();
      String jobName = indexInfo.getJobName();
      String jobUser = indexInfo.getUser();
      long finishTime = indexInfo.getFinishTime();
    
      if (searchUser) {
        if (!soughtUser.equals(jobUser)) {
          continue;
        }
      }
      
      if (searchJobName) {
        if (!jobName.contains(soughtJobNameSubstring)) {
          continue;
        }
      }
      
      if (searchDates) {
        boolean matchedDate = false;
        Calendar jobCal = Calendar.getInstance();
        jobCal.setTimeInMillis(finishTime);
        for (Calendar cal : soughtCalendars) {
          if (jobCal.get(Calendar.YEAR) == cal.get(Calendar.YEAR) &&
              jobCal.get(Calendar.MONTH) == cal.get(Calendar.MONTH) &&
              jobCal.get(Calendar.DAY_OF_MONTH) == cal.get(Calendar.DAY_OF_MONTH)) {
            matchedDate = true;
            break;
          }
        }
        if (!matchedDate) {
          break;
        }
      }
      resultMap.put(jobId, new PartialJob(indexInfo, jobId));
    }
    return resultMap;
  }
  
  private List<Calendar> getSoughtDateAsCalendar(String [] soughtDateStrings) {
    List<Calendar> soughtCalendars = new ArrayList<Calendar>();
    for (int i = 0 ; i < soughtDateStrings.length ; i++) {
      String soughtDate = soughtDateStrings[i];
      if (soughtDate.length() != 0) {
        Matcher m = DATE_PATTERN.matcher(soughtDate);
        if (m.matches()) {
          String yyyyPart = m.group(3);
          String mmPart = m.group(1);
          String ddPart = m.group(2);
          
          if (yyyyPart.length() == 2) {
            yyyyPart = "20" + yyyyPart;
          }
          if (mmPart.length() == 1) {
            mmPart = "0" + mmPart;
          }
          if (ddPart.length() == 1) {
            ddPart = "0" + ddPart;
          }
          Calendar soughtCal = Calendar.getInstance();
          soughtCal.set(Calendar.YEAR, Integer.parseInt(yyyyPart));
          soughtCal.set(Calendar.MONTH, Integer.parseInt(mmPart) - 1);
          soughtCal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(ddPart) -1);
          soughtCalendars.add(soughtCal);
        }
      }
    }
    return soughtCalendars;
  }
  
  

  
  private void moveToDone(MetaInfo metaInfo) throws IOException {
    long completeTime = metaInfo.getJobIndexInfo().getFinishTime();
    if (completeTime == 0) completeTime = System.currentTimeMillis();
    JobId jobId = metaInfo.getJobIndexInfo().getJobId();
    
    List<Path> paths = new ArrayList<Path>();
    Path historyFile = metaInfo.getHistoryFile();
    if (historyFile == null) {
      LOG.info("No file for job-history with " + jobId + " found in cache!");
    } else {
      paths.add(historyFile);
    }
    
    Path confFile = metaInfo.getConfFile();
    if (confFile == null) {
      LOG.info("No file for jobConf with " + jobId + " found in cache!");
    } else {
      paths.add(confFile);
    }
    
    //TODO Check all mi getters and setters for the conf path
    Path summaryFile = metaInfo.getSummaryFile();
    if (summaryFile == null) {
      LOG.info("No summary file for job: " + jobId);
    } else {
      try {
        String jobSummaryString = getJobSummary(intermediateDoneDirFc, summaryFile);
        SUMMARY_LOG.info(jobSummaryString);
        LOG.info("Deleting JobSummary file: [" + summaryFile + "]");
        intermediateDoneDirFc.delete(summaryFile, false);
        metaInfo.setSummaryFile(null);
      } catch (IOException e) {
        LOG.warn("Failed to process summary file: [" + summaryFile + "]");
        throw e;
      }
    }

    Path targetDir = canonicalHistoryLogPath(jobId, completeTime);
    addDirectoryToSerialNumberIndex(targetDir);
    try {
      maybeMakeSubdirectory(targetDir);
    } catch (IOException e) {
      LOG.warn("Failed creating subdirectory: " + targetDir + " while attempting to move files for jobId: " + jobId);
      throw e;
    }
    synchronized (metaInfo) {
      if (historyFile != null) {
        Path toPath = doneDirFc.makeQualified(new Path(targetDir, historyFile.getName()));
        try {
          moveToDoneNow(historyFile, toPath);
        } catch (IOException e) {
          LOG.warn("Failed to move file: " + historyFile + " for jobId: " + jobId);
          throw e;
        }
        metaInfo.setHistoryFile(toPath);
      }
      if (confFile != null) {
        Path toPath = doneDirFc.makeQualified(new Path(targetDir, confFile.getName()));
        try {
          moveToDoneNow(confFile, toPath);
        } catch (IOException e) {
          LOG.warn("Failed to move file: " + historyFile + " for jobId: " + jobId);
          throw e;
        }
        metaInfo.setConfFile(toPath);
      }
    }
    addToJobListCache(jobId, metaInfo);
    intermediateListCache.remove(jobId);
  }
  
  private void moveToDoneNow(final Path src, final Path target)
      throws IOException {
    LOG.info("Moving " + src.toString() + " to " + target.toString());
    intermediateDoneDirFc.rename(src, target, Options.Rename.NONE);
    // fc.util().copy(src, target);
    //fc.delete(src, false);
    //intermediateDoneDirFc.setPermission(target, new FsPermission(
    //JobHistoryUtils.HISTORY_DONE_FILE_PERMISSION));
  }
  
  String getJobSummary(FileContext fc, Path path) throws IOException {
    Path qPath = fc.makeQualified(path);
    FSDataInputStream in = fc.open(qPath);
    String jobSummaryString = in.readUTF();
    in.close();
    return jobSummaryString;
  }
  
  private void maybeMakeSubdirectory(Path path) throws IOException {
    boolean existsInExistingCache = false;
    synchronized(existingDoneSubdirs) {
      if (existingDoneSubdirs.contains(path)) existsInExistingCache = true;
    }
    try {
      doneDirFc.getFileStatus(path);
      if (!existsInExistingCache) {
        existingDoneSubdirs.add(path);
        if (debugMode) {
          LOG.info("JobHistory.maybeMakeSubdirectory -- We believed "
                             + path + " already existed, but it didn't.");
        }
      }
    } catch (FileNotFoundException fnfE) {
      try {
        FsPermission fsp = new FsPermission(JobHistoryUtils.HISTORY_DONE_DIR_PERMISSION);
        doneDirFc.mkdir(path, fsp, true);
        FileStatus fsStatus = doneDirFc.getFileStatus(path);
        LOG.info("Perms after creating " + fsStatus.getPermission().toShort()
            + ", Expected: " + fsp.toShort());
        if (fsStatus.getPermission().toShort() != fsp.toShort()) {
          LOG.info("Explicitly setting permissions to : " + fsp.toShort()
              + ", " + fsp);
          doneDirFc.setPermission(path, fsp);
        }
        synchronized(existingDoneSubdirs) {
          existingDoneSubdirs.add(path);
        }
      } catch (FileAlreadyExistsException faeE) { //Nothing to do.
      }
    }
  }
  
  private Path canonicalHistoryLogPath(JobId id, String timestampComponent) {
    return new Path(doneDirPrefixPath, JobHistoryUtils.historyLogSubdirectory(id, timestampComponent, serialNumberFormat));
  }
  
  private Path canonicalHistoryLogPath(JobId id, long millisecondTime) {
    String timestampComponent = JobHistoryUtils.timestampDirectoryComponent(millisecondTime, debugMode);
    return new Path(doneDirPrefixPath, JobHistoryUtils.historyLogSubdirectory(id, timestampComponent, serialNumberFormat));
  }  
  

  @Override
  public synchronized Job getJob(JobId jobId) {
    Job job = null;
    try {
      job = findJob(jobId);
      //This could return a null job.
    } catch (IOException e) {
      throw new YarnException(e);
    }
    return job;
  }

  @Override
  public Map<JobId, Job> getAllJobs(ApplicationId appID) {
    LOG.info("Called getAllJobs(AppId): " + appID);
//    currently there is 1 to 1 mapping between app and job id
    org.apache.hadoop.mapreduce.JobID oldJobID = TypeConverter.fromYarn(appID);
    Map<JobId, Job> jobs = new HashMap<JobId, Job>();
    JobId jobID = TypeConverter.toYarn(oldJobID);
    jobs.put(jobID, getJob(jobID));
    return jobs;
//    return getAllJobs();
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.HistoryContext#getAllJobs()
   * 
   * Returns a recent list of jobs. This may not be the complete set.
   * If a previous jobId is known - it can be queries via the getJob(JobId)
   * method.
   * Size of this list is determined by the size of the job list cache.
   * This can be fixed when pagination is implemented - return the first set of
   * jobs via the cache, go to DFS only when an attempt is made to navigate
   * past the cached list.
   * This does involve a DFS oepration of scanning the intermediate directory.
   */
  public Map<JobId, Job> getAllJobs() {
    return getAllJobsInternal();
        }

  
  
  
  
  static class MetaInfo {
    private Path historyFile;
    private Path confFile; 
    private Path summaryFile;
    JobIndexInfo jobIndexInfo;

    MetaInfo(Path historyFile, Path confFile, Path summaryFile, JobIndexInfo jobIndexInfo) {
      this.historyFile = historyFile;
      this.confFile = confFile;
      this.summaryFile = summaryFile;
      this.jobIndexInfo = jobIndexInfo;
      }

    Path getHistoryFile() { return historyFile; }
    Path getConfFile() { return confFile; }
    Path getSummaryFile() { return summaryFile; }
    JobIndexInfo getJobIndexInfo() { return jobIndexInfo; }
    
    void setHistoryFile(Path historyFile) { this.historyFile = historyFile; }
    void setConfFile(Path confFile) {this.confFile = confFile; }
    void setSummaryFile(Path summaryFile) { this.summaryFile = summaryFile; }
  }
  

  public class HistoryCleaner implements Runnable {
    private long currentTime;
    
    long maxAgeMillis;
    long filesDeleted = 0;
    long dirsDeleted = 0;
    
    public HistoryCleaner(long maxAge) {
      this.maxAgeMillis = maxAge;
    }
    
    @SuppressWarnings("unchecked")
    public void run() {
      LOG.info("History Cleaner started");
      currentTime = System.currentTimeMillis();
      boolean halted = false;
      //TODO Delete YYYY/MM/DD directories.
      try {
        List<FileStatus> serialDirList = findTimestampedDirectories();
        //Sort in ascending order. Relies on YYYY/MM/DD/Serial
        Collections.sort(serialDirList);
        for (FileStatus serialDir : serialDirList) {
          List<FileStatus> historyFileList = scanDirectoryForHistoryFiles(serialDir.getPath(), doneDirFc);
          for (FileStatus historyFile : historyFileList) {
            JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(historyFile.getPath().getName());
            long effectiveTimestamp = getEffectiveTimestamp(jobIndexInfo.getFinishTime(), historyFile);
            if (shouldDelete(effectiveTimestamp)) {
              String confFileName = JobHistoryUtils.getIntermediateConfFileName(jobIndexInfo.getJobId());
              MetaInfo metaInfo = new MetaInfo(historyFile.getPath(), new Path(historyFile.getPath().getParent(), confFileName), null, jobIndexInfo);
              delete(metaInfo);
            } else {
              halted = true;
              break;
            }
          }
          if (!halted) {
            deleteDir(serialDir.getPath());
            removeDirectoryFromSerialNumberIndex(serialDir.getPath());
            synchronized (existingDoneSubdirs) {
              existingDoneSubdirs.remove(serialDir.getPath());  
            }
            
          } else {
            break; //Don't scan any more directories.
    }
  }
      } catch (IOException e) {
        LOG.warn("Error in History cleaner run", e);
      }
      LOG.info("History Cleaner complete");
      LOG.info("FilesDeleted: " + filesDeleted);
      LOG.info("Directories Deleted: " + dirsDeleted);
    }
    
    private boolean shouldDelete(long ts) {
      return ((ts + maxAgeMillis) <= currentTime);
    }
    
    private long getEffectiveTimestamp(long finishTime, FileStatus fileStatus) {
      if (finishTime == 0) {
        return fileStatus.getModificationTime();
      }
      return finishTime;
    }
    
    private void delete(MetaInfo metaInfo) throws IOException {
      deleteFile(metaInfo.getHistoryFile());
      deleteFile(metaInfo.getConfFile());
      jobListCache.remove(metaInfo.getJobIndexInfo().getJobId());
      loadedJobCache.remove(metaInfo.getJobIndexInfo().getJobId());
    }
    
    private void deleteFile(final Path path) throws IOException {
      doneDirFc.delete(doneDirFc.makeQualified(path), false);
      filesDeleted++;
    }
    
    private void deleteDir(Path path) throws IOException {
      doneDirFc.delete(doneDirFc.makeQualified(path), true);
      dirsDeleted++;
    }
    }
    
  
  
  //TODO AppContext - Not Required
  private  ApplicationAttemptId appAttemptID;
  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
  //TODO fixme - bogus appAttemptID for now
    return appAttemptID;
  }  
  
  //TODO AppContext - Not Required
  private ApplicationId appID;
  @Override
  public ApplicationId getApplicationID() {
  //TODO fixme - bogus appID for now
    return appID;
  }
  
  //TODO AppContext - Not Required
  @Override
  public EventHandler getEventHandler() {
    // TODO Auto-generated method stub
    return null;
  }
  
  //TODO AppContext - Not Required
  private String userName;
  @Override
  public CharSequence getUser() {
    if (userName != null) {
      userName = conf.get(MRJobConfig.USER_NAME, "history-user");
    }
    return userName;
  }
  
  //TODO AppContext - Not Required
 @Override
 public Clock getClock() {
   return null;
 }
}
