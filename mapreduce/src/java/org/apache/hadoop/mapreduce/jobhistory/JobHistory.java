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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * JobHistory is the class that is responsible for creating and maintaining
 * job history information.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobHistory {

  final Log LOG = LogFactory.getLog(JobHistory.class);

  private long jobHistoryBlockSize;
  private static final Map<JobID, MetaInfo> fileMap =
    Collections.<JobID,MetaInfo>synchronizedMap(new HashMap<JobID,MetaInfo>());
  private ThreadPoolExecutor executor = null;
  static final FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0750); // rwxr-x---

  public static final FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0740); // rwxr-----
 
  private JobTracker jobTracker;

  static final long DEFAULT_HISTORY_MAX_AGE = 7 * 24 * 60 * 60 * 1000L; //week
  private FileSystem logDirFs; // log Dir FS
  private FileSystem doneDirFs; // done Dir FS

  private Path logDir = null;
  private Path done = null; // folder for completed jobs

  private static String DONE_BEFORE_SERIAL_TAIL = doneSubdirsBeforeSerialTail();
  private static String DONE_LEAF_FILES = DONE_BEFORE_SERIAL_TAIL + "/*";

  static final String CONF_FILE_NAME_SUFFIX = "_conf.xml";

  // XXXXX debug mode -- set this to false for production
  private static final boolean DEBUG_MODE = false;

  private static final int SERIAL_NUMBER_DIRECTORY_DIGITS = 6;
  private static final int SERIAL_NUMBER_LOW_DIGITS = DEBUG_MODE ? 1 : 3;

  private static final String SERIAL_NUMBER_FORMAT
    = ("%0"
       + (SERIAL_NUMBER_DIRECTORY_DIGITS + SERIAL_NUMBER_LOW_DIGITS)
       + "d");

  private static final Set<Path> existingDoneSubdirs = new HashSet<Path>();

  private static final SortedMap<Integer, String> idToDateString
    = new TreeMap<Integer, String>();

  private static Pattern historyCleanerParseDirectory
    = Pattern.compile(".+/([0-9]+)/([0-9]+)/([0-9]+)/([0-9]+)/?");
  //  .+ / YYYY / MM / DD / HH /?


  public static final String OLD_SUFFIX = ".old";
  public static final String OLD_FULL_SUFFIX_REGEX_STRING
    = "(?:\\.[0-9]+" + Pattern.quote(OLD_SUFFIX) + ")";
  
  // Version string that will prefix all History Files
  public static final String HISTORY_VERSION = "1.0";

  private HistoryCleaner historyCleanerThread = null;

  private static final int version = 3;
  private static final String LOG_VERSION_STRING = "version-" + version;

  private long jobTrackerStartTime;
  private String jobTrackerHostName;
  private String jobTrackerUniqueName;

  private static final Map<JobID, MovedFileInfo> jobHistoryFileMap = 
    Collections.<JobID,MovedFileInfo>synchronizedMap(
        new LinkedHashMap<JobID, MovedFileInfo>());

  // The invariant is that UnindexedElementsState tracks the identity
  //   of the currently-filling done directory subdirectory, and what
  //   needs to indexed.
  // Has to be locked for each file disposition decision
  private final UnindexedElementsState ueState = new UnindexedElementsState();

  // JobHistory filename regex
  public static final Pattern JOBHISTORY_FILENAME_REGEX = 
    Pattern.compile("(" + JobID.JOBID_REGEX + ")"
                    + OLD_FULL_SUFFIX_REGEX_STRING + "?");
  // JobHistory conf-filename regex
  public static final Pattern CONF_FILENAME_REGEX =
    Pattern.compile("(" + JobID.JOBID_REGEX + ")"
                    + CONF_FILE_NAME_SUFFIX
                    + OLD_FULL_SUFFIX_REGEX_STRING + "?");

  private static final int MAXIMUM_DATESTRING_COUNT = 200000;
  
  private static class MovedFileInfo {
    private final String historyFile;
    private final long timestamp;
    public MovedFileInfo(String historyFile, long timestamp) {
      this.historyFile = historyFile;
      this.timestamp = timestamp;
    }
  }
  /**
   * Initialize Job History Module
   * @param jt Job Tracker handle
   * @param conf Configuration
   * @param hostname Host name of JT
   * @param jobTrackerStartTime Start time of JT
   * @throws IOException
   */
  public void init(JobTracker jt, JobConf conf, String hostname,
      long jobTrackerStartTime) throws IOException {

    jobTrackerHostName = hostname;
    this.jobTrackerStartTime = jobTrackerStartTime;

    this.jobTrackerUniqueName = jobTrackerHostName + "-" + jobTrackerStartTime;

    // Get and create the log folder
    final String logDirLoc = conf.get(JTConfig.JT_JOBHISTORY_LOCATION ,
        "file:///" +
        new File(System.getProperty("hadoop.log.dir")).getAbsolutePath()
        + File.separator + "history");

    LOG.info("History log directory is " + logDirLoc);
    
    logDir = new Path(logDirLoc);
    logDirFs = logDir.getFileSystem(conf);

    if (!logDirFs.exists(logDir)){
      if (!logDirFs.mkdirs(logDir, 
          new FsPermission(HISTORY_DIR_PERMISSION))) {
        throw new IOException("Mkdirs failed to create " +
            logDir.toString());
      }
    }
    conf.set(JTConfig.JT_JOBHISTORY_LOCATION, logDirLoc);

    jobHistoryBlockSize = 
      conf.getLong(JTConfig.JT_JOBHISTORY_BLOCK_SIZE, 
          3 * 1024 * 1024);
    
    jobTracker = jt;
  }

  public static String leafGlobString() {
    return "/" + LOG_VERSION_STRING
      + "/*"                    // job tracker ID
      + "/YYYY/MM/DD/HH"        // time segment
      ;
  }

  public static String globString() {
    return "/" + LOG_VERSION_STRING
      + "/*"                    // job tracker ID
      + "/YYYY/MM/DD"        // time segment
      ;
  }
  
  /** Initialize the done directory and start the history cleaner thread */
  public void initDone(JobConf conf, FileSystem fs) throws IOException {
    //if completed job history location is set, use that
    String doneLocation =
      conf.get(JTConfig.JT_JOBHISTORY_COMPLETED_LOCATION);
    if (doneLocation != null) {
      done = fs.makeQualified(new Path(doneLocation));
      doneDirFs = fs;
    } else {
      done = logDirFs.makeQualified(new Path(logDir, "done"));
      doneDirFs = logDirFs;
    }

    //If not already present create the done folder with appropriate 
    //permission
    if (!doneDirFs.exists(done)) {
      LOG.info("Creating DONE folder at "+ done);
      if (!doneDirFs.mkdirs(done, 
                            new FsPermission(HISTORY_DIR_PERMISSION))) {
        throw new IOException("Mkdirs failed to create " + done.toString());
      }
    }
    LOG.info("Inited the done directory to " + done.toString());

    moveOldFiles();
    startFileMoverThreads();

    // Start the History Cleaner Thread
    long maxAgeOfHistoryFiles = conf.getLong(
        JTConfig.JT_JOBHISTORY_MAXAGE, DEFAULT_HISTORY_MAX_AGE);
    historyCleanerThread = new HistoryCleaner(maxAgeOfHistoryFiles);
    historyCleanerThread.start();
  }

  /**
   * Move the completed job into the completed folder.
   * This assumes that the job history file is closed and 
   * all operations on the job history file is complete.
   * This *should* be the last call to job history for a given job.
   */
  public void markCompleted(JobID id) throws IOException {
    moveToDone(id);
  }

  /** Shut down JobHistory after stopping the History cleaner */
  public void shutDown() {
    LOG.info("Interrupting History Cleaner");
    historyCleanerThread.interrupt();
    try {
      historyCleanerThread.join();
    } catch (InterruptedException e) {
      LOG.info("Error with shutting down history thread");
    }
  }

  /**
   * Get the history location
   */
  public Path getJobHistoryLocation() {
    return logDir;
  }

  /**
   * Get the history location for completed jobs
   */
  public Path getCompletedJobHistoryLocation() {
    return done;
  }

  /**
   * Get the job history file path
   */
  public static Path getJobHistoryFile
        (Path dir, JobID jobId) {
    MetaInfo info = fileMap.get(jobId);

    if (info == null) {
      fileMap.put(jobId, new MetaInfo(null, null, null, System.currentTimeMillis(), null, null));
      return getJobHistoryFile(dir, jobId);
    }            

    return new Path(dir, jobId.toString());
  }

  

  /**
   * Get the JobID from the history file's name. See it's companion method
   * {@link #getJobHistoryFile(Path, JobID, String)} for how history file's name
   * is constructed from a given JobID and userName.
   * 
   * @param jobHistoryFilePath
   * @return jobID
   */
  public static JobID getJobIDFromHistoryFilePath(Path jobHistoryFilePath) {
    String[] jobDetails = jobHistoryFilePath.getName().split("_");
    String jobId =
        jobDetails[0] + "_" + jobDetails[1] + "_" + jobDetails[2];
    return JobID.forName(jobId);
  }

  static String nonOccursString(String logFileName) {
    int adHocIndex = 0;

    String unfoundString = "q" + adHocIndex;

    while (logFileName.contains(unfoundString)) {
      unfoundString = "q" + ++adHocIndex;
    }

    return unfoundString + "q";
  }

  // I tolerate this code because I expect a low number of
  // occurrences in a relatively short string
  static String replaceStringInstances
      (String logFileName, String old, String replacement) {
    int index = logFileName.indexOf(old);

    while (index > 0) {
      logFileName = (logFileName.substring(0, index)
                     + replacement
                     + replaceStringInstances
                         (logFileName.substring(index + old.length()),
                          old, replacement));

      index = logFileName.indexOf(old);
    }

    return logFileName;
  }      

  /**
   * Given the job id, return the history file path from the cache
   */
  public String getHistoryFilePath(JobID jobId) {
    MovedFileInfo info = jobHistoryFileMap.get(jobId);
    if (info == null) {
      return null;
    }
    return info.historyFile;
  }

  /**
   * Given the job id, return the conf.xml file path from the cache
   */
  public String getConfFilePath(JobID jobId) {
    MovedFileInfo info = jobHistoryFileMap.get(jobId);
    if (info == null) {
      return null;
    }
    final Path historyFileDir
      = (new Path(getHistoryFilePath(jobId))).getParent();
    return getConfFile(historyFileDir, jobId).toString();
  }
    
  /**
   * Get the job name from the job conf
   */
  static String getJobName(JobConf jobConf) {
    String jobName = jobConf.getJobName();
    if (jobName == null || jobName.length() == 0) {
      jobName = "NA";
    }
    return jobName;
  }

  /**
   * Create an event writer for the Job represented by the jobID.
   * This should be the first call to history for a job
   * @param jobId
   * @param jobConf
   * @throws IOException
   */
  public void setupEventWriter(JobID jobId, JobConf jobConf)
  throws IOException {
    if (logDir == null) {
      LOG.info("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);

    long submitTime = (oldFi == null ? System.currentTimeMillis() : oldFi.submitTime);

    String user = getUserName(jobConf);
    String jobName = getJobName(jobConf);
    
    Path logFile = getJobHistoryFile(logDir, jobId);
  
    int defaultBufferSize = 
      logDirFs.getConf().getInt("io.file.buffer.size", 4096);
  
    LOG.info("SetupWriter, creating file " + logFile);
  
    FSDataOutputStream out = logDirFs.create(logFile, 
        new FsPermission(JobHistory.HISTORY_FILE_PERMISSION),
        true, defaultBufferSize, 
        logDirFs.getDefaultReplication(), 
        jobHistoryBlockSize, null);
  
    EventWriter writer = new EventWriter(out);
  
    /* Storing the job conf on the log dir */
  
    Path logDirConfPath = getConfFile(logDir, jobId);
    LOG.info("LogDirConfPath is " + logDirConfPath);
  
    FSDataOutputStream jobFileOut = null;
    try {
      if (logDirConfPath != null) {
        defaultBufferSize =
          logDirFs.getConf().getInt("io.file.buffer.size", 4096);
        if (!logDirFs.exists(logDirConfPath)) {
          jobFileOut = logDirFs.create(logDirConfPath,
              new FsPermission(JobHistory.HISTORY_FILE_PERMISSION),
              true, defaultBufferSize,
              logDirFs.getDefaultReplication(),
              logDirFs.getDefaultBlockSize(), null);
          jobConf.writeXml(jobFileOut);
          jobFileOut.close();
        }
      }
    } catch (IOException e) {
      LOG.info("Failed to close the job configuration file " 
          + StringUtils.stringifyException(e));
    }
  
    MetaInfo fi = new MetaInfo(logFile, logDirConfPath, writer, submitTime, user, jobName);
    fileMap.put(jobId, fi);
  }

  /** Close the event writer for this id */
  public void closeWriter(JobID id) {
    try {
      final MetaInfo mi = fileMap.get(id);
      if (mi != null) {
        mi.closeWriter();
      }
    } catch (IOException e) {
      LOG.info("Error closing writer for JobID: " + id);
    }
  }

  /**
   * Method to log the specified event
   * @param event The event to log
   * @param id The Job ID of the event
   */
  public void logEvent(HistoryEvent event, JobID id) {
    try {
      final MetaInfo mi = fileMap.get(id);
      if (mi != null) {
        mi.writeEvent(event);
      }
    } catch (IOException e) {
      LOG.error("Error Logging event, " + e.getMessage());
    }
  }


  private void moveToDoneNow(Path fromPath, Path toPath) throws IOException {
    //check if path exists, in case of retries it may not exist
    if (logDirFs.exists(fromPath)) {
      LOG.info("Moving " + fromPath.toString() + " to " +
          toPath.toString());
      doneDirFs.moveFromLocalFile(fromPath, toPath);
      doneDirFs.setPermission(toPath,
          new FsPermission(JobHistory.HISTORY_FILE_PERMISSION));
    }
  }
  
  private void startFileMoverThreads() {
    executor = new ThreadPoolExecutor(3, 5, 1, 
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
  }

  /**
   * Get the job conf file for the given jobId
   * 
   * @param logDir
   * @param jobId
   * @return the jobconf.xml path
   */
  public static Path getConfFile(Path logDir, JobID jobId) {
    Path jobFilePath = null;
    if (logDir != null) {
      jobFilePath = new Path(logDir + File.separator +
          jobId.toString() + CONF_FILE_NAME_SUFFIX);
    }
    return jobFilePath;
  }

  /**
   * Generates a suffix for old/stale jobhistory files
   * Pattern : . + identifier + JobHistory.OLD_SUFFIX
   */
  public static String getOldFileSuffix(String identifier) {
    return "." + identifier + JobHistory.OLD_SUFFIX;
  }
  
  private void moveOldFiles() throws IOException {
    //move the log files remaining from last run to the DONE folder
    //suffix the file name based on Job tracker identifier so that history
    //files with same job id don't get over written in case of recovery.
    FileStatus[] files = logDirFs.listStatus(logDir);
    String fileSuffix = getOldFileSuffix(jobTracker.getTrackerIdentifier());
    // We use the same millisecond time for all files so the config file
    //  and job history file flow to the same subdirectory
    long millisecondTime = ueState.monotonicTime();
    for (FileStatus fileStatus : files) {
      Path fromPath = fileStatus.getPath();
      if (fromPath.equals(done)) { //DONE can be a subfolder of log dir
        continue;
      }
      LOG.info("Moving log file from last run: " + fromPath);
      Path resultDir
        = canonicalHistoryLogDir(null, millisecondTime);
      Path toPath = new Path(resultDir, fromPath.getName() + fileSuffix);
      try {
        moveToDoneNow(fromPath, toPath);
      } catch (ChecksumException e) {
        // If there is an exception moving the file to done because of
        // a checksum exception, just delete it
        LOG.warn("Unable to move " + fromPath +", deleting it");
        try {
          boolean b = logDirFs.delete(fromPath, false);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deletion of corrupt file " + fromPath + " returned " + b);
          }
        } catch (IOException ioe) {
          // Cannot delete either? Just log and carry on
          LOG.warn("Unable to delete " + fromPath + "Exception: " +
              ioe.getMessage());
        }
      } catch (IOException e) {
        // Exceptions other than checksum, just log and continue
        LOG.warn("Error moving file " + fromPath + " to done folder." +
            "Ignoring.");
      }
    }
  }
    
  
  private void moveToDone(final JobID id) {
    final List<Path> paths = new ArrayList<Path>();
    final MetaInfo metaInfo = fileMap.get(id);
    if (metaInfo == null || metaInfo.getHistoryFile() == null) {
      LOG.info("No file for job-history with " + id + " found in cache!");
      return;
    }

    final Path historyFile = metaInfo.getHistoryFile();
    if (historyFile == null) {
      LOG.info("No file for job-history with " + id + " found in cache!");
    } else {
      paths.add(historyFile);
    }

    final Path confPath = metaInfo.getConfFile();
    if (confPath == null) {
      LOG.info("No file for jobconf with " + id + " found in cache!");
    } else {
      paths.add(confPath);
    }

    executor.execute(new Runnable() {
      static final int SPONTANEOUSLY_CLOSE_INDEX_INTERVAL = 30 * 1000;

      static final int SPONTANEOUS_INTERIM_INDEX_INTERVAL = 300 * 1000;

      public void run() {
        boolean iShouldMonitor = false;

        Path resultDir = null;

        String historyFileDonePath = null;

        Path failedJobHistoryIndexBuildPath = null;
        Throwable failedHistoryMoveException = null;

        synchronized (ueState) {
          // needed because it's possible for system time to go backward
          long millisecondTime = ueState.monotonicTime();

          resultDir = canonicalHistoryLogDir(id, millisecondTime);

          if (!resultDir.equals(ueState.currentDoneSubdirectory)) {
            if (ueState.currentDoneSubdirectory != null) {
              try {
                ueState.closeCurrentDirectory();
              } catch (IOException e) {
                failedJobHistoryIndexBuildPath = ueState.currentDoneSubdirectory;
              }
            }

            iShouldMonitor = true;

            ueState.indexableElements = new LinkedList<JobHistoryIndexElement>();
            ueState.currentDoneSubdirectory = resultDir;

            ueState.monitoredDirectory = resultDir;
          }

          // We need to make the JobHistoryIndexElement here, because after
          //  we've copied the file the info might disappear, but before we've
          //  closed the previous subdirectory [if we do that] it would go into
          //  the wrong subdirectory index.
          ueState.indexableElements.
            add(new JobHistoryIndexElement(millisecondTime, id, metaInfo));

          //move the files to a DONE canonical subfolder
          try {
            for (Path path : paths) { 
              moveToDoneNow(path, new Path(resultDir, path.getName()));
            }
          } catch (Throwable e) {
            failedHistoryMoveException = e;
          }
          if (historyFile != null) {
            historyFileDonePath = new Path(resultDir, 
                                           historyFile.getName()).toString();
          }
          jobHistoryFileMap.put(id, new MovedFileInfo(historyFileDonePath, 
                                                      millisecondTime));
        }

        if (failedJobHistoryIndexBuildPath != null) {
          LOG.warn("Couldn't build a Job History index for "
                   + failedJobHistoryIndexBuildPath);
        }
        if (failedHistoryMoveException != null) {
          LOG.error("Can't move history file to DONE canonical subfolder.",
                    failedHistoryMoveException);
        }
          

        jobTracker.retireJob(org.apache.hadoop.mapred.JobID.downgrade(id),
                             historyFileDonePath);

        //purge the job from the cache
        fileMap.remove(id);

        // Except ephemerally, only one task will be in this code at a
        // time, because iShouldMonitor is only set true when
        // ueState.monitoredDirectory changes, which will force the
        // current incumbent to abend at the earliest opportunity.
        while (iShouldMonitor) {
          int roundCounter = 0;

          int interruptionsToAbort = 2;

          try {
            Thread.sleep(SPONTANEOUSLY_CLOSE_INDEX_INTERVAL);
          } catch (InterruptedException e) {
            if (--interruptionsToAbort == 0) {
              return;
            }
          }

          Path unbuildableJobHistoryIndex = null;
          Path unbuildableInterimJobHistoryIndex = null;

          synchronized (ueState) {
            if (ueState.monitoredDirectory != resultDir) {
              // someone else closed out the directory I was monitoring
              iShouldMonitor = false;
            } else {
              interruptionsToAbort = 2;

              long millisecondTime = ueState.monotonicTime();

              Path newResultDir = canonicalHistoryLogDir(id, millisecondTime);

              if (!newResultDir.equals(resultDir)) {
                try {
                  ueState.closeCurrentDirectory();
                } catch (IOException e) {
                  unbuildableJobHistoryIndex = ueState.currentDoneSubdirectory;
                }
                iShouldMonitor = false;
              }
            }

            if (iShouldMonitor
                && (++roundCounter
                    % (SPONTANEOUS_INTERIM_INDEX_INTERVAL
                       / SPONTANEOUSLY_CLOSE_INDEX_INTERVAL)
                    == 0)) {
              // called for side effect -- a 5 minute checkpoint to
              // reduce possible unindexed jobs on a JT crash
              try {
                ueState.getACurrentIndex(ueState.currentDoneSubdirectory);
              } catch (IOException e) {
                unbuildableInterimJobHistoryIndex
                  = ueState.currentDoneSubdirectory;
              }
            }
          }

          if (unbuildableJobHistoryIndex != null) {
            LOG.warn("Couldn't build a Job History index for "
                     + unbuildableJobHistoryIndex);
          }

          if (unbuildableInterimJobHistoryIndex != null) {
            LOG.warn("Couldn't build an interim Job History index for "
                     + unbuildableInterimJobHistoryIndex);
          }
        }
      }
    });
  }

  public String[] currentIndex(Path theDoneSubdirectory)
        throws IOException {
    return ueState.currentIndex(theDoneSubdirectory);
  }

  // we only create one instance per JobHistory
  class UnindexedElementsState {
    long monotonicTime = Long.MIN_VALUE;
    Path currentDoneSubdirectory = null;
    private List<JobHistoryIndexElement> indexableElements = null;
    Path monitoredDirectory = null;
    int indexIndex = 0;
    int indexedElementCount = 0;

    private void buildIndex(String indexName) throws IOException {
      Path tempPath = new Path(currentDoneSubdirectory, "nascent-index");
      Path indexPath = new Path(currentDoneSubdirectory, indexName);

      OutputStream newIndexOStream = null;
      PrintStream newIndexPStream = null;

      indexedElementCount = indexableElements.size();
 
      try {
        newIndexOStream
          = FileSystem.create(doneDirFs, tempPath, HISTORY_FILE_PERMISSION);

        newIndexPStream = new PrintStream(newIndexOStream);

        for (JobHistoryIndexElement elt : indexableElements) {
          newIndexPStream.println(elt.toString());
        }
      } finally {
        if (newIndexPStream != null) {
          newIndexPStream.close();

          if (doneDirFs.exists(tempPath)) {
            doneDirFs.rename(tempPath, indexPath);
          } 
        } else if (newIndexOStream != null) {
          newIndexOStream.close();
          doneDirFs.delete(tempPath, false);
        }
      }
    }

    synchronized String[] currentIndex(Path theDoneSubdirectory)
        throws IOException {
      Path subdirIndex = getACurrentIndex(theDoneSubdirectory);

      List<String> indexAsRead = new ArrayList<String>();

      InputStream iStream = null;
      InputStreamReader isReader = null;
      BufferedReader breader = null;

      try {
        iStream = doneDirFs.open(subdirIndex);
        isReader = new InputStreamReader(iStream);
        breader = new BufferedReader(isReader);

        String thisRecord = breader.readLine();

        while (thisRecord != null) {
          indexAsRead.add(thisRecord);
          thisRecord = breader.readLine();
        }

        String[] result = indexAsRead.toArray(new String[0]);

        Arrays.sort(result);

        return result;
      } finally {
        if (breader != null) {
          breader.close();
        } else if (isReader != null) {
          isReader.close();
        } else if (iStream != null) {
          iStream.close();
        }
      }
    }
      
    // If this is the block that's now being built, we build a new
    // index and return that.  This shouldn't be called on an empty
    // subdirectory.
    // 
    // getACurrentIndex must be called within a synchronized(this) block.
    // Currently there are two calls, both of which qualify.
    Path getACurrentIndex(Path theDoneSubdirectory) throws IOException {
      if (!theDoneSubdirectory.equals(currentDoneSubdirectory)) {
        return new Path(theDoneSubdirectory, "index");
      }

      if (indexedElementCount == indexableElements.size()) {
        return new Path(theDoneSubdirectory, "index-" + indexIndex);
      }

      String indexName = "index-" + ++indexIndex;

      buildIndex(indexName);

      return new Path(theDoneSubdirectory, indexName);
    }

    // not synchronized, because calls must be in a larger synchronized context
    private void closeCurrentDirectory() throws IOException {
      if (currentDoneSubdirectory == null) {
        return;
      }

      buildIndex("index");
    }

    synchronized long monotonicTime() {
      monotonicTime = Math.max(monotonicTime, System.currentTimeMillis());
      return monotonicTime;
    }
  }

  static class JobHistoryIndexElement {
    // id and millisecondTime are currently unused.
    final JobID id;
    final long millisecondTime;
    final MetaInfo metaInfo;

    JobHistoryIndexElement(long millisecondTime, JobID id, MetaInfo metaInfo) {
      this.id = id;
      this.millisecondTime = millisecondTime;
      this.metaInfo = metaInfo;
    }

    public String toString() {
      String user = metaInfo.user;
      String jobName = metaInfo.jobName;

      if (jobName.length() > 50) {
        jobName = jobName.substring(0, 50);
      }

      String adHocBarEscape = "";

      if (user.indexOf('|') >= 0 || jobName.indexOf('|') >= 0) {
        adHocBarEscape = nonOccursString(user + jobName);

        user = replaceStringInstances(user, "|", adHocBarEscape);
        jobName = replaceStringInstances(jobName, "|", adHocBarEscape);
      }

      return (metaInfo.getHistoryFile().getName()
              + "|" + millisecondTime
              + "|" + adHocBarEscape
              + "|" + user
              + "|" + jobName); 
    }
  }

  // several methods for manipulating the subdirectories of the DONE
  // directory 

  // directory components may contain internal slashes, but do NOT
  // contain slashes at either end.

  // In this nest of code, id can be null.  In that case it is an error to call
  //  more than once to get a single filename.  This can happen when we're moving
  //  files from an old run into the new context.  See moveOldFiles() .
  private static String timestampDirectoryComponent(JobID id, long millisecondTime) {
    Integer boxedSerialNumber = null;

    if (id != null) {
      boxedSerialNumber = id.getId();
    }

    // don't want to do this inside the lock
    Calendar timestamp = Calendar.getInstance();
    timestamp.setTimeInMillis(millisecondTime);

    synchronized (idToDateString) {
      String dateString
        = (boxedSerialNumber == null ? null : idToDateString.get(boxedSerialNumber));

      if (dateString == null) {

        dateString = String.format
          ("%04d/%02d/%02d/%02d",
           timestamp.get(Calendar.YEAR),
           timestamp.get(DEBUG_MODE ? Calendar.HOUR_OF_DAY : Calendar.MONTH) + 1,
           timestamp.get(DEBUG_MODE ? Calendar.MINUTE : Calendar.DAY_OF_MONTH),
           timestamp.get(DEBUG_MODE ? Calendar.SECOND : Calendar.HOUR_OF_DAY));

        dateString = dateString.intern();

        if (boxedSerialNumber != null) {
          idToDateString.put(boxedSerialNumber, dateString);

          if (idToDateString.size() > MAXIMUM_DATESTRING_COUNT) {
            idToDateString.remove(idToDateString.firstKey());
          }
        }
      }

      return dateString;
    }
  }

  // returns false iff the directory already existed
  private boolean maybeMakeSubdirectory(JobID id, long millisecondTime)
          throws IOException {
    Path dir = canonicalHistoryLogDir(id, millisecondTime);

    String deferredErrorPrintout = null;
    String deferredInfoLogging = null;

    try {
      synchronized (existingDoneSubdirs) {
        if (existingDoneSubdirs.contains(dir)) {
          if (DEBUG_MODE && !doneDirFs.exists(dir)) {
            deferredErrorPrintout
              = ("JobHistory.maybeMakeSubdirectory -- We believed "
                 + dir + " already existed, but it didn't.");
          }
          
          return true;
        }

        if (!doneDirFs.exists(dir)) {
          deferredInfoLogging = "Creating DONE subfolder at " + dir;

          if (!doneDirFs.mkdirs(dir,
                                new FsPermission(HISTORY_DIR_PERMISSION))) {
            throw new IOException("Mkdirs failed to create " + dir.toString());
          }

          existingDoneSubdirs.add(dir);

          return false;
        } else {
          if (DEBUG_MODE) {
            deferredErrorPrintout
              = ("JobHistory.maybeMakeSubdirectory -- We believed "
                 + dir + " didn't already exist, but it did.");
          }

          return false;
        }
      }
    } finally {
      if (deferredErrorPrintout != null) {
        System.err.println(deferredErrorPrintout);
      }

      if (deferredInfoLogging != null) {
        LOG.info(deferredInfoLogging);
      }
    }
  }

  // Previous versions of this code used the id argument, when the
  //  directory structure was a bit different.
  // I'm leaving the currently unused id argument in place, in case we
  //  decide to start using it again in the future.
  private Path canonicalHistoryLogDir(JobID id, long millisecondTime) {
    return new Path(done, historyLogSubdirectory(id, millisecondTime));
  }

  private String historyLogSubdirectory(JobID id, long millisecondTime) {
    String result = LOG_VERSION_STRING
      + "/" + jobtrackerDirectoryComponent(id);

    result = (result
              + "/" + timestampDirectoryComponent(id, millisecondTime)
              + "/")
      .intern();

    return result;
  }

  private String jobtrackerDirectoryComponent(JobID id) {
    return jobTrackerUniqueName;
  }

  private static String doneSubdirsBeforeSerialTail() {
    // job tracker ID + date
    String result = "/*/*/*/*/*";   // job tracker instance ID/YYYY/MM/DD/HH

    return result;
  }

  
  private String getUserName(JobConf jobConf) {
    String user = jobConf.getUser();
    if (user == null) {
      user = "";
    }
    return user;
  }
  

  // hasMismatches is just used to return a second value if you want
  // one.  I would have used MutableBoxedBoolean if such had been provided.
  static Path[] filteredStat2Paths
          (FileStatus[] stats, boolean dirs, AtomicBoolean hasMismatches) {
    int resultCount = 0;

    if (hasMismatches == null) {
      hasMismatches = new AtomicBoolean(false);
    }

    for (int i = 0; i < stats.length; ++i) {
      if (stats[i].isDir() == dirs) {
        stats[resultCount++] = stats[i];
      } else {
        hasMismatches.set(true);
      }
    }

    Path[] paddedResult = FileUtil.stat2Paths(stats);

    Path[] result = new Path[resultCount];

    System.arraycopy(paddedResult, 0, result, 0, resultCount);

    return result;
  }

  public FileStatus[] getAllHistoryConfFiles() throws IOException {
    return localGlobber
      (doneDirFs, done, "/" + LOG_VERSION_STRING + "/*/*/*/*/*");
  }

  public static FileStatus[] localGlobber
        (FileSystem fs, Path root, String tail) 
      throws IOException {
    return localGlobber(fs, root, tail, null);
  }

  public static FileStatus[] localGlobber
        (FileSystem fs, Path root, String tail, PathFilter filter) 
      throws IOException {
    return localGlobber(fs, root, tail, filter, null);
  }
  
  private static FileStatus[] nullToEmpty(FileStatus[] result) {
    return result == null ? new FileStatus[0] : result;
  }
      
  private static FileStatus[] listFilteredStatus
        (FileSystem fs, Path root, PathFilter filter)
     throws IOException {
    return filter == null ? fs.listStatus(root) : fs.listStatus(root, filter);
  }

  // hasMismatches is just used to return a second value if you want
  // one.  I would have used MutableBoxedBoolean if such had been provided.
  static FileStatus[] localGlobber
    (FileSystem fs, Path root, String tail, PathFilter filter,
     AtomicBoolean hasFlatFiles)
      throws IOException {
    if (tail.equals("")) {
      return nullToEmpty(listFilteredStatus(fs, root, filter));
    }

    if (tail.startsWith("/*")) {
      Path[] subdirs = filteredStat2Paths(nullToEmpty(fs.listStatus(root)),
                                          true, hasFlatFiles);

      FileStatus[][] subsubdirs = new FileStatus[subdirs.length][];

      int subsubdirCount = 0;

      if (subsubdirs.length == 0) {
        return new FileStatus[0];
      }

      String newTail = tail.substring(2);

      for (int i = 0; i < subdirs.length; ++i) {
        subsubdirs[i] = localGlobber(fs, subdirs[i], newTail, filter, null);
        subsubdirCount += subsubdirs[i].length;
      }

      FileStatus[] result = new FileStatus[subsubdirCount];

      int segmentStart = 0;

      for (int i = 0; i < subsubdirs.length; ++i) {
        System.arraycopy(subsubdirs[i], 0, result, segmentStart, subsubdirs[i].length);
        segmentStart += subsubdirs[i].length;
      }

      return result;
    }

    if (tail.startsWith("/")) {
      int split = tail.indexOf('/', 1);

      try {
        if (split < 0) {
          return nullToEmpty
            (listFilteredStatus(fs, new Path(root, tail.substring(1)), filter));
        } else {
          String thisSegment = tail.substring(1, split);
          String newTail = tail.substring(split);
          return localGlobber
            (fs, new Path(root, thisSegment), newTail, filter, hasFlatFiles);
        }
      } catch (FileNotFoundException ignored) {
        return new FileStatus[0];
      }
    }

    IOException e = new IOException("localGlobber: bad tail");

    throw e;
  }

  private static class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;
    long submitTime;
    String user;
    String jobName;

    MetaInfo(Path historyFile, Path conf, EventWriter writer, long submitTime,
             String user, String jobName) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
      this.submitTime = submitTime;
      this.user = user;
      this.jobName = jobName;
    }

    Path getHistoryFile() { return historyFile; }
    Path getConfFile() { return confFile; }

    synchronized void closeWriter() throws IOException {
      if (writer != null) {
        writer.close();
      }
      writer = null;
    }

    synchronized void writeEvent(HistoryEvent event) throws IOException {
      if (writer != null) {
        writer.write(event);
      }
    }
  }

  /**
   * Returns a job history log Path generator
   * We return all Path's that exist in the history at time of call, subject to
   *   four conjunctive criteria, one for each of parameter.  Each parameter is 
   *   null or the empty string if caller doesn't want that filtering.
   * It's perfectly alright to call this with no restrictions.
   *
   * @param user the desired username 
   *           [or null or the empty string, if no specific user]
   * @param jobnameSubstring a substring of the job names
   * @param dateStrings an array of date strings, format MM/DD/YYYY .  This 
   *           criterion accepts logs with ANY of the dates
   * @param soughtJobid the String naming the jobID we want, if any.  Note
   *           that this criterion names a unique job; you may not want
   *           to specify any other criteria if you specify this one.
   * @throws IOException
   */
  public JobHistoryRecordRetriever getMatchingJobs
          (String user, String jobnameSubstring,
           String[] dateStrings, String soughtJobid)
        throws IOException {
    return new JobHistoryRecordRetriever
      (user, jobnameSubstring, dateStrings, soughtJobid);
  }
    
  public static class JobHistoryJobRecord {
    private Path basePath;
    private String recordText;
    private String[] recordSplits = null;

    JobHistoryJobRecord(Path basePath, String recordText) {
      this.basePath = basePath;
      this.recordText = recordText;
    }

    private String[] getSplits(boolean noCache) {
      if (recordSplits != null) {
        return recordSplits;
      }

      String[] result = recordText.split("\\|");

      if (!noCache) {
        recordSplits = result;
      }

      return result;
    }

    public Path getPath() {
      return getPath(false);
    }

    public Path getPath(boolean noCache) {
      return new Path(basePath, getSplits(noCache)[0]);
    }

    public String getJobIDString() {
      return getJobIDString(false);
    }

    public String getJobIDString(boolean noCache) {
      return getSplits(noCache)[0];
    }    

    public long getSubmitTime() {
      return getSubmitTime(false);
    }

    public long getSubmitTime(boolean noCache) {
      return Long.parseLong(getSplits(noCache)[1]);
    }

    public String getUserName() {
      return getUserName(false);
    }

    public String getUserName(boolean noCache) {
      String[] splits = getSplits(noCache);

      String result = splits[3];

      if (splits[2].length() != 0) {
        result = replaceStringInstances(result, splits[2], "|");
      }

      return result;
    }

    public String getJobName() {
      return getJobName(false);
    }

    public String getJobName(boolean noCache) {
      String[] splits = getSplits(noCache);

      String result = splits[4];

      if (splits[2].length() != 0) {
        result = replaceStringInstances(result, splits[2], "|");
      }

      return result;
    }
  }

  public class JobHistoryRecordRetriever implements Iterator<JobHistoryJobRecord> {
    private final Pattern DATE_PATTERN
      = Pattern.compile("([0-1]?[0-9])/([0-3]?[0-9])/((?:2[0-9])[0-9][0-9])");
    

    private class BaseElements {
      Path basePath;
      List<String> records = new LinkedList<String>();
    }

    // Internal contract -- elts contains no empty BaseElements's
    private List<BaseElements> elts = new LinkedList<BaseElements>();

    private Iterator<BaseElements> eltsCursor;
    private Iterator<String> currentEltCursor;

    private BaseElements currentBE = null;

    private int numberMatches;

    @Override
    public boolean hasNext() {
      return currentEltCursor.hasNext() || eltsCursor.hasNext();
    }

    @Override
    public JobHistoryJobRecord next() throws NoSuchElementException {
      if (currentEltCursor.hasNext()) {
        return new JobHistoryJobRecord(currentBE.basePath, currentEltCursor.next());
      }

      currentBE = eltsCursor.next();
      currentEltCursor = currentBE.records.iterator();
      return next();
    }

    @Override
    public void remove() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("no remove() operation");
    }

    public int numberMatches() {
      return numberMatches;
    }

    public JobHistoryRecordRetriever
          (String soughtUser, String soughtJobName, String[] dateStrings, String soughtJobid)
        throws IOException {
      numberMatches = 0;

      soughtUser = soughtUser == null ? "" : soughtUser;
      soughtJobName = soughtJobName == null ? "" : soughtJobName;
      soughtJobid = soughtJobid == null ? "" : soughtJobid;

      if (dateStrings == null || dateStrings.length == 0) {
        dateStrings = new String[1];
        dateStrings[0] = "";
      }

      for (int i = 0; i < dateStrings.length; ++i) {
        String soughtDate = dateStrings[i];
        String globString = globString();
        

        String yyyyGlobPart = "*";
        String mmGlobPart = "*";
        String ddGlobPart = "*";
        String hhGlobPart = "*";

        if (soughtDate.length() != 0) {
          Matcher m = DATE_PATTERN.matcher(soughtDate);
          if (m.matches()) {
            yyyyGlobPart = m.group(3);
            mmGlobPart = m.group(1);
            ddGlobPart = m.group(2);

            if (yyyyGlobPart.length() == 2) {
              yyyyGlobPart = "20" + yyyyGlobPart;
            }

            if (mmGlobPart.length() == 1) {
              mmGlobPart = "0" + mmGlobPart;
            }

            if (ddGlobPart.length() == 1) {
              ddGlobPart = "0" + ddGlobPart;
            }
          }
        }

        globString = globString.replace("YYYY", yyyyGlobPart);
        globString = globString.replace("MM", mmGlobPart);
        globString = globString.replace("DD", ddGlobPart);
        globString = globString.replace("HH", hhGlobPart);

        if (doneDirFs == null) {
          if (DEBUG_MODE) {
            System.out.println("Null file system. May be namenode is in safemode!");
          }
          return;
        }

        Path[] jobDirectories
          = FileUtil.stat2Paths
              (JobHistory.localGlobber
               (doneDirFs, done, globString));

        for (int jd = 0; jd < jobDirectories.length; jd++) {
          Path doneSubdirectory = jobDirectories[jd];

          String[] subdirectoryIndex = new String[0];

          BaseElements be = new BaseElements();

          be.basePath = doneSubdirectory;

          try {
            subdirectoryIndex = currentIndex(doneSubdirectory);
          } catch (FileNotFoundException e) {
            // no code -- should we log here?
          }

          for (int j = 0; j < subdirectoryIndex.length; ++j) {
            String[] segments = subdirectoryIndex[j].split("\\|");

            // segments are [0] jobid [also file name]
            //              [1] submit time [milliseconds]
            //              [2] ad hoc '|' substitution
            //              [3] user name
            //              [4] trimmed job jame
            //

            String user = segments[3];
            String jobName = segments[4];

            if (segments[2].length() > 0) {
              user = replaceStringInstances(user, segments[2], "|");
              jobName = replaceStringInstances(jobName, segments[2], "|");
            }

            if ((soughtJobid.equals("") || segments[0].equalsIgnoreCase(soughtJobid))
                && (soughtUser.equals("") || user.equalsIgnoreCase(soughtUser))
                && (soughtJobName.equals("") || jobName.contains(soughtJobName))) {
              be.records.add(subdirectoryIndex[j]);
            }
          }

          if (be.records.size() != 0) {
            elts.add(be);

            numberMatches += be.records.size();
          }
        }
      }

      eltsCursor = elts.iterator();

      currentEltCursor = new LinkedList<String>().iterator();
    }
  }

  static long directoryTime(String year, String seg2, String seg3, String seg4) {
    // set to current time.  In debug mode, this is where the month
    // and day get set.
    Calendar result = Calendar.getInstance();
    // canonicalize by filling in unset fields
    result.setTimeInMillis(System.currentTimeMillis());

    result.set(Calendar.YEAR, Integer.parseInt(year));

    int seg2int = Integer.parseInt(seg2);
    if (!DEBUG_MODE) {
      --seg2int;
    }

    result.set(DEBUG_MODE ? Calendar.HOUR_OF_DAY : Calendar.MONTH,
               seg2int);
    result.set(DEBUG_MODE ? Calendar.MINUTE : Calendar.DAY_OF_MONTH,
               Integer.parseInt(seg3));
    result.set(DEBUG_MODE ? Calendar.SECOND : Calendar.HOUR_OF_DAY,
               Integer.parseInt(seg4));

    return result.getTimeInMillis();
  }

  /**
   * Delete history files older than a specified time duration.
   */
  class HistoryCleaner extends Thread {
    static final long ONE_DAY_IN_MS = 24 * 60 * 60 * 1000L;

    static final long DIRECTORY_LIFE_IN_MS
      = DEBUG_MODE ? 20 * 60 * 1000L : 30 * ONE_DAY_IN_MS;
    static final long RUN_INTERVAL
      = DEBUG_MODE ? 10L * 60L * 1000L : ONE_DAY_IN_MS;

    private long cleanupFrequency;
    private long maxAgeOfHistoryFiles;

    public HistoryCleaner(long maxAge) {
      setName("Thread for cleaning up History files");
      setDaemon(true);
      this.maxAgeOfHistoryFiles = maxAge;
      cleanupFrequency = Math.min(RUN_INTERVAL, maxAgeOfHistoryFiles);
      LOG.info("Job History Cleaner Thread started." +
          " MaxAge is " + 
          maxAge + " ms(" + ((float)maxAge)/(ONE_DAY_IN_MS) + " days)," +
          " Cleanup Frequency is " +
          + cleanupFrequency + " ms (" +
          ((float)cleanupFrequency)/ONE_DAY_IN_MS + " days)");
    }
  
    @Override
    public void run(){
  
      while (true) {
        try {
          doCleanup(); 
          Thread.sleep(cleanupFrequency);
        }
        catch (InterruptedException e) {
          LOG.info("History Cleaner thread exiting");
          return;
        }
        catch (Throwable t) {
          LOG.warn("History cleaner thread threw an exception", t);
        }
      }
    }
  
    private void doCleanup() {
      long now = ueState.monotonicTime();

      boolean printedOneDeletee = false;

      Set<String> deletedPathnames = new HashSet<String>();

      try {
        Path[] datedDirectories
          = FileUtil.stat2Paths(localGlobber(doneDirFs, done,
                                             DONE_BEFORE_SERIAL_TAIL, null));

        // fild old directories
        for (int i = 0; i < datedDirectories.length; ++i) {
          String thisDir = datedDirectories[i].toString();
          Matcher pathMatcher = historyCleanerParseDirectory.matcher(thisDir);

          if (pathMatcher.matches()) {
            long dirTime = directoryTime(pathMatcher.group(1),
                                         pathMatcher.group(2),
                                         pathMatcher.group(3),
                                         pathMatcher.group(4));

            if (DEBUG_MODE) {
              System.err.println("HistoryCleaner.run just parsed " + thisDir
                                 + " as year/month/day/hour = "
                               + pathMatcher.group(1)
                               + "/" + pathMatcher.group(2)
                               + "/" + pathMatcher.group(3)
                               + "/" + pathMatcher.group(4));
            }

            if (dirTime < now - DIRECTORY_LIFE_IN_MS) {
              if (DEBUG_MODE) {
                Calendar then = Calendar.getInstance();
                then.setTimeInMillis(dirTime);
                Calendar nnow = Calendar.getInstance();
                nnow.setTimeInMillis(now);
                
                System.err.println("HistoryCleaner.run directory: " + thisDir
                                   + " because its time is " + then
                                   + " but it's now " + nnow);
                System.err.println("then = " + dirTime);
                System.err.println("now  = " + now);
              }

              // remove every file in the directory and save the name
              // so we can remove it from jobHistoryFileMap
              Path[] deletees
                = FileUtil.stat2Paths(localGlobber(doneDirFs,
                                                   datedDirectories[i],
                                                   "/*", // individual files
                                                   null));

              for (int j = 0; j < deletees.length; ++j) {

                if (DEBUG_MODE && !printedOneDeletee) {
                  System.err.println("HistoryCleaner.run deletee: " + deletees[j].toString());
                  printedOneDeletee = true;
                }

                LOG.info("Deleting old history file : " + deletees[j]);

                doneDirFs.delete(deletees[j]);
                deletedPathnames.add(deletees[j].toString());
              }

              synchronized (existingDoneSubdirs) {
                if (!existingDoneSubdirs.contains(datedDirectories[i]))
                  {
                    LOG.warn("JobHistory: existingDoneSubdirs doesn't contain "
                             + datedDirectories[i] + ", but should.");
                  }
                doneDirFs.delete(datedDirectories[i], true);
                existingDoneSubdirs.remove(datedDirectories[i]);
              }
            }
          }
        }

        //walking over the map to purge entries from jobHistoryFileMap
        synchronized (jobHistoryFileMap) {
          Iterator<Entry<JobID, MovedFileInfo>> it = 
            jobHistoryFileMap.entrySet().iterator();
          while (it.hasNext()) {
            MovedFileInfo info = it.next().getValue();
            if (deletedPathnames.contains(info.historyFile)) {
              it.remove();
            }
          }
        }
      } catch (IOException ie) {
        LOG.info("Error cleaning up history directory" + 
            StringUtils.stringifyException(ie));
      }
    }
  }
}
