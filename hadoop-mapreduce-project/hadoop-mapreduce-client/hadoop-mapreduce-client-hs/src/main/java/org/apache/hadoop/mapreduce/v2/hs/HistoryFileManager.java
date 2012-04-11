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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

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

/**
 * This class provides a way to interact with history files in a thread safe
 * manor.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class HistoryFileManager extends AbstractService {
  private static final Log LOG = LogFactory.getLog(HistoryFileManager.class);
  private static final Log SUMMARY_LOG = LogFactory.getLog(JobSummary.class);

  private static String DONE_BEFORE_SERIAL_TAIL = JobHistoryUtils
      .doneSubdirsBeforeSerialTail();

  public static class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private Path summaryFile;
    private JobIndexInfo jobIndexInfo;

    public MetaInfo(Path historyFile, Path confFile, Path summaryFile,
        JobIndexInfo jobIndexInfo) {
      this.historyFile = historyFile;
      this.confFile = confFile;
      this.summaryFile = summaryFile;
      this.jobIndexInfo = jobIndexInfo;
    }

    private Path getHistoryFile() {
      return historyFile;
    }

    private Path getConfFile() {
      return confFile;
    }

    private Path getSummaryFile() {
      return summaryFile;
    }

    public JobIndexInfo getJobIndexInfo() {
      return jobIndexInfo;
    }

    public JobId getJobId() {
      return jobIndexInfo.getJobId();
    }

    private void setHistoryFile(Path historyFile) {
      this.historyFile = historyFile;
    }

    private void setConfFile(Path confFile) {
      this.confFile = confFile;
    }

    private void setSummaryFile(Path summaryFile) {
      this.summaryFile = summaryFile;
    }
  }

  /**
   * Maps between a serial number (generated based on jobId) and the timestamp
   * component(s) to which it belongs. Facilitates jobId based searches. If a
   * jobId is not found in this list - it will not be found.
   */
  private final SortedMap<String, Set<String>> idToDateString = 
    new TreeMap<String, Set<String>>();
  // The number of entries in idToDateString
  private int dateStringCacheSize;

  // Maintains minimal details for recent jobs (parsed from history file name).
  // Sorted on Job Completion Time.
  private final SortedMap<JobId, MetaInfo> jobListCache = 
    new ConcurrentSkipListMap<JobId, MetaInfo>();
  // The number of jobs to maintain in the job list cache.
  private int jobListCacheSize;

  // Re-use existing MetaInfo objects if they exist for the specific JobId.
  // (synchronization on MetaInfo)
  // Check for existence of the object when using iterators.
  private final SortedMap<JobId, MetaInfo> intermediateListCache = 
    new ConcurrentSkipListMap<JobId, MetaInfo>();

  // Maintains a list of known done subdirectories.
  private final Set<Path> existingDoneSubdirs = new HashSet<Path>();

  /**
   * Maintains a mapping between intermediate user directories and the last
   * known modification time.
   */
  private Map<String, Long> userDirModificationTimeMap = 
    new HashMap<String, Long>();

  private JobACLsManager aclsMgr;

  private Configuration conf;

  // TODO Remove me!!!!
  private boolean debugMode;
  private String serialNumberFormat;

  private Path doneDirPrefixPath = null; // folder for completed jobs
  private FileContext doneDirFc; // done Dir FileContext

  private Path intermediateDoneDirPath = null; // Intermediate Done Dir Path
  private FileContext intermediateDoneDirFc; // Intermediate Done Dir
                                             // FileContext

  public HistoryFileManager() {
    super(HistoryFileManager.class.getName());
  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;

    debugMode = conf.getBoolean(JHAdminConfig.MR_HISTORY_DEBUG_MODE, false);
    int serialNumberLowDigits = debugMode ? 1 : 3;
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

    jobListCacheSize = conf.getInt(JHAdminConfig.MR_HISTORY_JOBLIST_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_JOBLIST_CACHE_SIZE);

    dateStringCacheSize = conf.getInt(
        JHAdminConfig.MR_HISTORY_DATESTRING_CACHE_SIZE,
        JHAdminConfig.DEFAULT_MR_HISTORY_DATESTRING_CACHE_SIZE);

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
    synchronized (idToDateString) {
      // TODO make this thread safe without the synchronize
      if (idToDateString.containsKey(serialPart)) {
        Set<String> set = idToDateString.get(serialPart);
        set.remove(timeStampPart);
        if (set.isEmpty()) {
          idToDateString.remove(serialPart);
        }
      }
    }
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
    }
    addToSerialNumberIndex(serialPart, timestampPart);
  }

  private void addToSerialNumberIndex(String serialPart, String timestampPart) {
    synchronized (idToDateString) {
      // TODO make this thread safe without the synchronize
      if (!idToDateString.containsKey(serialPart)) {
        idToDateString.put(serialPart, new HashSet<String>());
        if (idToDateString.size() > dateStringCacheSize) {
          idToDateString.remove(idToDateString.firstKey());
        }
        Set<String> datePartSet = idToDateString.get(serialPart);
        datePartSet.add(timestampPart);
      }
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
      MetaInfo metaInfo = new MetaInfo(fs.getPath(), new Path(fs.getPath()
          .getParent(), confFileName), new Path(fs.getPath().getParent(),
          summaryFileName), jobIndexInfo);
      addToJobListCache(metaInfo);
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

  private void addToJobListCache(MetaInfo metaInfo) {
    JobId jobId = metaInfo.getJobIndexInfo().getJobId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding " + jobId + " to job list cache with "
          + metaInfo.getJobIndexInfo());
    }
    jobListCache.put(jobId, metaInfo);
    if (jobListCache.size() > jobListCacheSize) {
      jobListCache.remove(jobListCache.firstKey());
    }
  }

  /**
   * Scans the intermediate directory to find user directories. Scans these for
   * history files if the modification time for the directory has changed.
   * 
   * @throws IOException
   */
  private void scanIntermediateDirectory() throws IOException {
    List<FileStatus> userDirList = JobHistoryUtils.localGlobber(
        intermediateDoneDirFc, intermediateDoneDirPath, "");

    for (FileStatus userDir : userDirList) {
      String name = userDir.getPath().getName();
      long newModificationTime = userDir.getModificationTime();
      boolean shouldScan = false;
      synchronized (userDirModificationTimeMap) {
        if (!userDirModificationTimeMap.containsKey(name)
            || newModificationTime > userDirModificationTimeMap.get(name)) {
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
   * @param fileStatusList
   *          fileStatus list of Job History Files.
   * @param jobId
   *          The JobId to find.
   * @return A MetaInfo object for the jobId, null if not found.
   * @throws IOException
   */
  private MetaInfo getJobMetaInfo(List<FileStatus> fileStatusList, JobId jobId)
      throws IOException {
    for (FileStatus fs : fileStatusList) {
      JobIndexInfo jobIndexInfo = FileNameIndexUtils.getIndexInfo(fs.getPath()
          .getName());
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
   * jobId. If the number of directories is higher than the supported size of
   * the idToDateString cache, the jobId will not be found.
   * 
   * @param jobId
   *          the jobId.
   * @return
   * @throws IOException
   */
  private MetaInfo scanOldDirsForJob(JobId jobId) throws IOException {
    int jobSerialNumber = JobHistoryUtils.jobSerialNumber(jobId);
    String boxedSerialNumber = String.valueOf(jobSerialNumber);
    Set<String> dateStringSet;
    synchronized (idToDateString) {
      Set<String> found = idToDateString.get(boxedSerialNumber);
      if (found == null) {
        return null;
      } else {
        dateStringSet = new HashSet<String>(found);
      }
    }
    for (String timestampPart : dateStringSet) {
      Path logDir = canonicalHistoryLogPath(jobId, timestampPart);
      List<FileStatus> fileStatusList = scanDirectoryForHistoryFiles(logDir,
          doneDirFc);
      MetaInfo metaInfo = getJobMetaInfo(fileStatusList, jobId);
      if (metaInfo != null) {
        return metaInfo;
      }
    }
    return null;
  }

  /**
   * Checks for the existence of the job history file in the intermediate
   * directory.
   * 
   * @param jobId
   * @return
   * @throws IOException
   */
  private MetaInfo scanIntermediateForJob(JobId jobId) throws IOException {
    scanIntermediateDirectory();
    return intermediateListCache.get(jobId);
  }

  /**
   * Parse a job from the JobHistoryFile, if the underlying file is not going to
   * be deleted.
   * 
   * @param metaInfo
   *          the where the JobHistory is stored.
   * @return the Job or null if the underlying file was deleted.
   * @throws IOException
   *           if there is an error trying to read the file.
   */
  public Job loadJob(MetaInfo metaInfo) throws IOException {
    return new CompletedJob(conf, metaInfo.getJobIndexInfo().getJobId(),
        metaInfo.getHistoryFile(), false, metaInfo.getJobIndexInfo().getUser(),
        metaInfo.getConfFile(), aclsMgr);
  }

  public Collection<MetaInfo> getAllMetaInfo() throws IOException {
    scanIntermediateDirectory();
    ArrayList<MetaInfo> result = new ArrayList<MetaInfo>();
    result.addAll(intermediateListCache.values());
    result.addAll(jobListCache.values());
    return result;
  }

  Collection<MetaInfo> getIntermediateMetaInfos() throws IOException {
    scanIntermediateDirectory();
    return intermediateListCache.values();
  }

  public MetaInfo getMetaInfo(JobId jobId) throws IOException {
    // MetaInfo available in cache.
    MetaInfo metaInfo = null;
    if (jobListCache.containsKey(jobId)) {
      metaInfo = jobListCache.get(jobId);
    }

    if (metaInfo != null) {
      return metaInfo;
    }

    // MetaInfo not available. Check intermediate directory for meta info.
    metaInfo = scanIntermediateForJob(jobId);
    if (metaInfo != null) {
      return metaInfo;
    }

    // Intermediate directory does not contain job. Search through older ones.
    metaInfo = scanOldDirsForJob(jobId);
    if (metaInfo != null) {
      return metaInfo;
    }
    return null;
  }

  void moveToDone(MetaInfo metaInfo) throws IOException {
    long completeTime = metaInfo.getJobIndexInfo().getFinishTime();
    if (completeTime == 0)
      completeTime = System.currentTimeMillis();
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

    // TODO Check all mi getters and setters for the conf path
    Path summaryFile = metaInfo.getSummaryFile();
    if (summaryFile == null) {
      LOG.info("No summary file for job: " + jobId);
    } else {
      try {
        String jobSummaryString = getJobSummary(intermediateDoneDirFc,
            summaryFile);
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
      makeDoneSubdir(targetDir);
    } catch (IOException e) {
      LOG.warn("Failed creating subdirectory: " + targetDir
          + " while attempting to move files for jobId: " + jobId);
      throw e;
    }
    synchronized (metaInfo) {
      if (historyFile != null) {
        Path toPath = doneDirFc.makeQualified(new Path(targetDir, historyFile
            .getName()));
        try {
          moveToDoneNow(historyFile, toPath);
        } catch (IOException e) {
          LOG.warn("Failed to move file: " + historyFile + " for jobId: "
              + jobId);
          throw e;
        }
        metaInfo.setHistoryFile(toPath);
      }
      if (confFile != null) {
        Path toPath = doneDirFc.makeQualified(new Path(targetDir, confFile
            .getName()));
        try {
          moveToDoneNow(confFile, toPath);
        } catch (IOException e) {
          LOG.warn("Failed to move file: " + historyFile + " for jobId: "
              + jobId);
          throw e;
        }
        metaInfo.setConfFile(toPath);
      }
    }
    addToJobListCache(metaInfo);
    intermediateListCache.remove(jobId);
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
    boolean existsInExistingCache = false;
    synchronized (existingDoneSubdirs) {
      if (existingDoneSubdirs.contains(path))
        existsInExistingCache = true;
    }
    try {
      doneDirFc.getFileStatus(path);
      if (!existsInExistingCache) {
        existingDoneSubdirs.add(path);
        if (LOG.isDebugEnabled()) {
          LOG.debug("JobHistory.maybeMakeSubdirectory -- We believed " + path
              + " already existed, but it didn't.");
        }
      }
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
        synchronized (existingDoneSubdirs) {
          existingDoneSubdirs.add(path);
        }
      } catch (FileAlreadyExistsException faeE) { 
        // Nothing to do.
      }
    }
  }

  private Path canonicalHistoryLogPath(JobId id, String timestampComponent) {
    return new Path(doneDirPrefixPath, JobHistoryUtils.historyLogSubdirectory(
        id, timestampComponent, serialNumberFormat));
  }

  private Path canonicalHistoryLogPath(JobId id, long millisecondTime) {
    String timestampComponent = JobHistoryUtils.timestampDirectoryComponent(
        millisecondTime, debugMode);
    return new Path(doneDirPrefixPath, JobHistoryUtils.historyLogSubdirectory(
        id, timestampComponent, serialNumberFormat));
  }

  private long getEffectiveTimestamp(long finishTime, FileStatus fileStatus) {
    if (finishTime == 0) {
      return fileStatus.getModificationTime();
    }
    return finishTime;
  }

  private void deleteJobFromDone(MetaInfo metaInfo) throws IOException {
    jobListCache.remove(metaInfo.getJobId());
    doneDirFc.delete(doneDirFc.makeQualified(metaInfo.getHistoryFile()), false);
    doneDirFc.delete(doneDirFc.makeQualified(metaInfo.getConfFile()), false);
  }

  @SuppressWarnings("unchecked")
  void clean(long cutoff, HistoryStorage storage) throws IOException {
    // TODO this should be replaced by something that knows about the directory
    // structure and will put less of a load on HDFS.
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
          String confFileName = JobHistoryUtils
              .getIntermediateConfFileName(jobIndexInfo.getJobId());
          MetaInfo metaInfo = new MetaInfo(historyFile.getPath(), new Path(
              historyFile.getPath().getParent(), confFileName), null,
              jobIndexInfo);
          storage.jobRemovedFromHDFS(metaInfo.getJobId());
          deleteJobFromDone(metaInfo);
        } else {
          halted = true;
          break;
        }
      }
      if (!halted) {
        doneDirFc.delete(doneDirFc.makeQualified(serialDir.getPath()), true);
        removeDirectoryFromSerialNumberIndex(serialDir.getPath());
        synchronized (existingDoneSubdirs) {
          existingDoneSubdirs.remove(serialDir.getPath());
        }
      } else {
        break; // Don't scan any more directories.
      }
    }
  }
}
