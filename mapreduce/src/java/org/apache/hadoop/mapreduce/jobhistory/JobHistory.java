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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  private final Map<JobID, MetaInfo> fileMap =
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

  public static final String OLD_SUFFIX = ".old";
  
  // Version string that will prefix all History Files
  public static final String HISTORY_VERSION = "1.0";

  private HistoryCleaner historyCleanerThread = null;

  private Map<JobID, MovedFileInfo> jobHistoryFileMap = 
    Collections.<JobID,MovedFileInfo>synchronizedMap(
        new LinkedHashMap<JobID, MovedFileInfo>());

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
      if (! doneDirFs.mkdirs(done, 
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
  public static Path getJobHistoryFile(Path dir, JobID jobId, 
      String user) {
    return new Path(dir, jobId.toString() + "_" + user);
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

  /**
   * Get the user name of the job-submitter from the history file's name. See
   * it's companion method {@link #getJobHistoryFile(Path, JobID, String)} for
   * how history file's name is constructed from a given JobID and username.
   * 
   * @param jobHistoryFilePath
   * @return the user-name
   */
  public static String getUserFromHistoryFilePath(Path jobHistoryFilePath) {
    String[] jobDetails = jobHistoryFilePath.getName().split("_");
    return jobDetails[3];
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
   * Create an event writer for the Job represented by the jobID.
   * This should be the first call to history for a job
   * @param jobId
   * @param jobConf
   * @throws IOException
   */
  public void setupEventWriter(JobID jobId, JobConf jobConf)
  throws IOException {
    Path logFile = getJobHistoryFile(logDir, jobId, getUserName(jobConf));
  
    if (logDir == null) {
      LOG.info("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }
  
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
  
    MetaInfo fi = new MetaInfo(logFile, logDirConfPath, writer);
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
    executor = new ThreadPoolExecutor(1, 3, 1, 
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
          jobId.toString() + "_conf.xml");
    }
    return jobFilePath;
  }

  private void moveOldFiles() throws IOException {
    //move the log files remaining from last run to the DONE folder
    //suffix the file name based on Job tracker identifier so that history
    //files with same job id don't get over written in case of recovery.
    FileStatus[] files = logDirFs.listStatus(logDir);
    String jtIdentifier = jobTracker.getTrackerIdentifier();
    String fileSuffix = "." + jtIdentifier + JobHistory.OLD_SUFFIX;
    for (FileStatus fileStatus : files) {
      Path fromPath = fileStatus.getPath();
      if (fromPath.equals(done)) { //DONE can be a subfolder of log dir
        continue;
      }
      LOG.info("Moving log file from last run: " + fromPath);
      Path toPath = new Path(done, fromPath.getName() + fileSuffix);
      try {
        moveToDoneNow(fromPath, toPath);
      } catch (ChecksumException e) {
        // If there is an exception moving the file to done because of
        // a checksum exception, just delete it
        LOG.warn("Unable to move " + fromPath +", deleting it");
        try {
          boolean b = logDirFs.delete(fromPath, false);
          LOG.debug("Deletion of corrupt file " + fromPath + " returned " + b);
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
    if (metaInfo == null) {
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

      public void run() {
        //move the files to DONE folder
        try {
          for (Path path : paths) { 
            moveToDoneNow(path, new Path(done, path.getName()));
          }
        } catch (Throwable e) {
          LOG.error("Unable to move history file to DONE folder.", e);
        }
        String historyFileDonePath = null;
        if (historyFile != null) {
          historyFileDonePath = new Path(done, 
              historyFile.getName()).toString();
        }
        jobHistoryFileMap.put(id, new MovedFileInfo(historyFileDonePath, 
            System.currentTimeMillis()));
        jobTracker.retireJob(org.apache.hadoop.mapred.JobID.downgrade(id),
            historyFileDonePath);

        //purge the job from the cache
        fileMap.remove(id);
      }

    });
  }
  
  private String getUserName(JobConf jobConf) {
    String user = jobConf.getUser();
    if (user == null) {
      user = "";
    }
    return user;
  }
  
  private static class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;

    MetaInfo(Path historyFile, Path conf, EventWriter writer) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
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
   * Delete history files older than a specified time duration.
   */
  class HistoryCleaner extends Thread {
    static final long ONE_DAY_IN_MS = 24 * 60 * 60 * 1000L;
    private long cleanupFrequency;
    private long maxAgeOfHistoryFiles;
  
    public HistoryCleaner(long maxAge) {
      setName("Thread for cleaning up History files");
      setDaemon(true);
      this.maxAgeOfHistoryFiles = maxAge;
      cleanupFrequency = Math.min(ONE_DAY_IN_MS, maxAgeOfHistoryFiles);
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
      long now = System.currentTimeMillis();
      try {
        FileStatus[] historyFiles = doneDirFs.listStatus(done);
        if (historyFiles != null) {
          for (FileStatus f : historyFiles) {
            if (now - f.getModificationTime() > maxAgeOfHistoryFiles) {
              doneDirFs.delete(f.getPath(), true); 
              LOG.info("Deleting old history file : " + f.getPath());
            }
          }
        }
        //walking over the map to purge entries from jobHistoryFileMap
        synchronized (jobHistoryFileMap) {
          Iterator<Entry<JobID, MovedFileInfo>> it = 
            jobHistoryFileMap.entrySet().iterator();
          while (it.hasNext()) {
            MovedFileInfo info = it.next().getValue();
            if (now - info.timestamp > maxAgeOfHistoryFiles) {
              it.remove();
            } else {
              //since entries are in sorted timestamp order, no more entries
              //are required to be checked
              break;
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
