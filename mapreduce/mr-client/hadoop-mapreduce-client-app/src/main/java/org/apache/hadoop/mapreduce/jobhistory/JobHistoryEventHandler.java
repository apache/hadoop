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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * The job history events get routed to this class. This class writes the Job
 * history events to the DFS directly into a staging dir and then moved to a
 * done-dir. JobHistory implementation is in this package to access package
 * private classes.
 */
public class JobHistoryEventHandler extends AbstractService
    implements EventHandler<JobHistoryEvent> {

  private final AppContext context;
  private final int startCount;

  //TODO Does the FS object need to be different ? 
  private FileSystem stagingDirFS; // log Dir FileSystem
  private FileSystem doneDirFS; // done Dir FileSystem

  private Configuration conf;

  private Path stagingDirPath = null;
  private Path doneDirPrefixPath = null; // folder for completed jobs


  private BlockingQueue<JobHistoryEvent> eventQueue =
    new LinkedBlockingQueue<JobHistoryEvent>();
  private Thread eventHandlingThread;
  private volatile boolean stopped;
  private final Object lock = new Object();

  private static final Log LOG = LogFactory.getLog(
      JobHistoryEventHandler.class);

  private static final Map<JobId, MetaInfo> fileMap =
    Collections.<JobId,MetaInfo>synchronizedMap(new HashMap<JobId,MetaInfo>());

  public JobHistoryEventHandler(AppContext context, int startCount) {
    super("JobHistoryEventHandler");
    this.context = context;
    this.startCount = startCount;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.service.AbstractService#init(org.apache.hadoop.conf.Configuration)
   * Initializes the FileSystem and Path objects for the log and done directories.
   * Creates these directories if they do not already exist.
   */
  @Override
  public void init(Configuration conf) {

    this.conf = conf;

    String stagingDirStr = null;
    String doneDirStr = null;
    String userDoneDirStr = null;
    try {
      stagingDirStr = JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf);
      doneDirStr =
          JobHistoryUtils.getConfiguredHistoryIntermediateDoneDirPrefix(conf);
      userDoneDirStr =
          JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf);
    } catch (IOException e) {
      LOG.error("Failed while getting the configured log directories", e);
      throw new YarnException(e);
    }

    //Check for the existence of the history staging dir. Maybe create it. 
    try {
      stagingDirPath =
          FileSystem.get(conf).makeQualified(new Path(stagingDirStr));
      stagingDirFS = FileSystem.get(stagingDirPath.toUri(), conf);
      mkdir(stagingDirFS, stagingDirPath, new FsPermission(
          JobHistoryUtils.HISTORY_STAGING_DIR_PERMISSIONS));
    } catch (IOException e) {
      LOG.error("Failed while checking for/creating  history staging path: ["
          + stagingDirPath + "]", e);
      throw new YarnException(e);
    }

    //Check for the existence of intermediate done dir.
    Path doneDirPath = null;
    try {
      doneDirPath = FileSystem.get(conf).makeQualified(new Path(doneDirStr));
      doneDirFS = FileSystem.get(doneDirPath.toUri(), conf);
      // This directory will be in a common location, or this may be a cluster
      // meant for a single user. Creating based on the conf. Should ideally be
      // created by the JobHistoryServer or as part of deployment.
      if (JobHistoryUtils.shouldCreateNonUserDirectory(conf)) {
        LOG.info("Creating intermediate history logDir: ["
            + doneDirPath
            + "] + based on conf. Should ideally be created by the JobHistoryServer: "
            + JHConfig.CREATE_HISTORY_INTERMEDIATE_BASE_DIR_KEY);
        mkdir(doneDirFS, doneDirPath, new FsPermission(
            JobHistoryUtils.HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS
                .toShort()));
        // TODO Temporary toShort till new FsPermission(FsPermissions) respects
        // sticky
      } else {
        String message =
            "Not creating intermediate history logDir: ["
                + doneDirPath
                + "] based on conf: "
                + JHConfig.CREATE_HISTORY_INTERMEDIATE_BASE_DIR_KEY
                + ". Either set to true or pre-create this directory with appropriate permissions";
        LOG.error(message);
        throw new YarnException(message);
      }
    } catch (IOException e) {
      LOG.error("Failed checking for the existance of history intermediate done directory: ["
          + doneDirPath + "]");
      throw new YarnException(e);
    }

    //Check/create user directory under intermediate done dir.
    try {
      doneDirPrefixPath =
          FileSystem.get(conf).makeQualified(new Path(userDoneDirStr));
      mkdir(doneDirFS, doneDirPrefixPath, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_USER_DIR_PERMISSIONS));
    } catch (IOException e) {
      LOG.error("Error creating user intermediate history done directory: [ "
          + doneDirPrefixPath + "]", e);
      throw new YarnException(e);
    }

    super.init(conf);
  }

  private void mkdir(FileSystem fs, Path path, FsPermission fsp)
      throws IOException {
    if (!fs.exists(path)) {
      try {
        fs.mkdirs(path, fsp);
        FileStatus fsStatus = fs.getFileStatus(path);
        LOG.info("Perms after creating " + fsStatus.getPermission().toShort()
            + ", Expected: " + fsp.toShort());
        if (fsStatus.getPermission().toShort() != fsp.toShort()) {
          LOG.info("Explicitly setting permissions to : " + fsp.toShort()
              + ", " + fsp);
          fs.setPermission(path, fsp);
        }
      } catch (FileAlreadyExistsException e) {
        LOG.info("Directory: [" + path + "] already exists.");
      }
    }
  }

  @Override
  public void start() {
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        JobHistoryEvent event = null;
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }
          // If an event has been removed from the queue. Handle it.
          // The rest of the queue is handled via stop()
          // Clear the interrupt status if it's set before calling handleEvent
          // and set it if it was set before calling handleEvent. 
          // Interrupts received from other threads during handleEvent cannot be
          // dealth with - Shell.runCommand() ignores them.
          synchronized (lock) {
            boolean isInterrupted = Thread.interrupted();
            handleEvent(event);
            if (isInterrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping JobHistoryEventHandler");
    stopped = true;
    //do not interrupt while event handling is in progress
    synchronized(lock) {
      eventHandlingThread.interrupt();
    }

    try {
      eventHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info("Interruped Exception while stopping", ie);
    }
    //write all the events remaining in queue
    Iterator<JobHistoryEvent> it = eventQueue.iterator();
    while(it.hasNext()) {
      JobHistoryEvent ev = it.next();
      LOG.info("In stop, writing event " + ev.getType());
      handleEvent(ev);
    }
    
    //close all file handles
    for (MetaInfo mi : fileMap.values()) {
      try {
        mi.closeWriter();
      } catch (IOException e) {
        LOG.info("Exception while closing file " + e.getMessage());
      }
    }
    LOG.info("Stopped JobHistoryEventHandler. super.stop()");
    super.stop();
  }

  /**
   * Create an event writer for the Job represented by the jobID.
   * Writes out the job configuration to the log directory.
   * This should be the first call to history for a job
   * 
   * @param jobId the jobId.
   * @throws IOException
   */
  protected void setupEventWriter(JobId jobId, JobSubmittedEvent jse)
      throws IOException {
    if (stagingDirPath == null) {
      LOG.error("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);
    Configuration conf = getConfig();

    long submitTime = oldFi == null ? jse.getSubmitTime() : oldFi
        .getJobIndexInfo().getSubmitTime();
    
    // TODO Ideally this should be written out to the job dir
    // (.staging/jobid/files - RecoveryService will need to be patched)
    Path historyFile = JobHistoryUtils.getStagingJobHistoryFile(
        stagingDirPath, jobId, startCount);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    if (user == null) {
      throw new IOException(
          "User is null while setting up jobhistory eventwriter");
    }
    String jobName = TypeConverter.fromYarn(jobId).toString();
    EventWriter writer = (oldFi == null) ? null : oldFi.writer;
 
    if (writer == null) {
      try {
        FSDataOutputStream out = stagingDirFS.create(historyFile, true);
        writer = new EventWriter(out);
        LOG.info("Event Writer setup for JobId: " + jobId + ", File: "
            + historyFile);
      } catch (IOException ioe) {
        LOG.info("Could not create log file: [" + historyFile + "] + for job "
            + "[" + jobName + "]");
        throw ioe;
      }
    }
    
    Path logDirConfPath = null;
    if (conf != null) {
      // TODO Ideally this should be written out to the job dir
      // (.staging/jobid/files - RecoveryService will need to be patched)
      logDirConfPath = JobHistoryUtils.getStagingConfFile(stagingDirPath, jobId,
          startCount);
      FSDataOutputStream jobFileOut = null;
      try {
        if (logDirConfPath != null) {
          jobFileOut = stagingDirFS.create(logDirConfPath, true);
          conf.writeXml(jobFileOut);
          jobFileOut.close();
        }
      } catch (IOException e) {
        LOG.info("Failed to write the job configuration file", e);
        throw e;
      }
    }
    
    MetaInfo fi = new MetaInfo(historyFile, logDirConfPath, writer, submitTime,
        user, jobName, jobId);
    fi.getJobSummary().setJobId(jobId);
    fi.getJobSummary().setJobSubmitTime(submitTime);
    fileMap.put(jobId, fi);
  }

  /** Close the event writer for this id 
   * @throws IOException */
  public void closeWriter(JobId id) throws IOException {
    try {
      final MetaInfo mi = fileMap.get(id);
      if (mi != null) {
        mi.closeWriter();
      }
      
    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + id);
      throw e;
    }
  }

  @Override
  public void handle(JobHistoryEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  protected void handleEvent(JobHistoryEvent event) {
    synchronized (lock) {

      // If this is JobSubmitted Event, setup the writer
      if (event.getHistoryEvent().getEventType() == EventType.JOB_SUBMITTED) {
        try {
          JobSubmittedEvent jobSubmittedEvent =
              (JobSubmittedEvent) event.getHistoryEvent();
          setupEventWriter(event.getJobID(), jobSubmittedEvent);
        } catch (IOException ioe) {
          LOG.error("Error JobHistoryEventHandler in handleEvent: " + event,
              ioe);
          throw new YarnException(ioe);
        }
      }

      // For all events
      // (1) Write it out
      // (2) Process it for JobSummary
      MetaInfo mi = fileMap.get(event.getJobID());
      try {
        HistoryEvent historyEvent = event.getHistoryEvent();
        mi.writeEvent(historyEvent);
        processEventForJobSummary(event.getHistoryEvent(), mi.getJobSummary());
        LOG.info("In HistoryEventHandler "
            + event.getHistoryEvent().getEventType());
      } catch (IOException e) {
        LOG.error("Error writing History Event: " + event.getHistoryEvent(),
            e);
        throw new YarnException(e);
      }

      // If this is JobFinishedEvent, close the writer and setup the job-index
      if (event.getHistoryEvent().getEventType() == EventType.JOB_FINISHED) {
        try {
          JobFinishedEvent jFinishedEvent =
              (JobFinishedEvent) event.getHistoryEvent();
          mi.getJobIndexInfo().setFinishTime(jFinishedEvent.getFinishTime());
          mi.getJobIndexInfo().setNumMaps(jFinishedEvent.getFinishedMaps());
          mi.getJobIndexInfo().setNumReduces(
              jFinishedEvent.getFinishedReduces());
          closeEventWriter(event.getJobID());
        } catch (IOException e) {
          throw new YarnException(e);
        }
      }

      if (event.getHistoryEvent().getEventType() == EventType.JOB_FAILED
          || event.getHistoryEvent().getEventType() == EventType.JOB_KILLED) {
        try {
          JobUnsuccessfulCompletionEvent jucEvent = (JobUnsuccessfulCompletionEvent) event
              .getHistoryEvent();
          mi.getJobIndexInfo().setFinishTime(jucEvent.getFinishTime());
          mi.getJobIndexInfo().setNumMaps(jucEvent.getFinishedMaps());
          mi.getJobIndexInfo().setNumReduces(jucEvent.getFinishedReduces());
          closeEventWriter(event.getJobID());
        } catch (IOException e) {
          throw new YarnException(e);
        }
      }
    }
  }

  private void processEventForJobSummary(HistoryEvent event, JobSummary summary) {
    // context.getJob could be used for some of this info as well.
    switch (event.getEventType()) {
    case JOB_SUBMITTED:
      JobSubmittedEvent jse = (JobSubmittedEvent) event;
      summary.setUser(jse.getUserName());
      summary.setQueue(jse.getJobQueueName());
      break;
    case JOB_INITED:
      JobInitedEvent jie = (JobInitedEvent) event;
      summary.setJobLaunchTime(jie.getLaunchTime());
      break;
    case MAP_ATTEMPT_STARTED:
      TaskAttemptStartedEvent mtase = (TaskAttemptStartedEvent) event;
      if (summary.getFirstMapTaskLaunchTime() == 0)
        summary.setFirstMapTaskLaunchTime(mtase.getStartTime());
      break;
    case REDUCE_ATTEMPT_STARTED:
      TaskAttemptStartedEvent rtase = (TaskAttemptStartedEvent) event;
      if (summary.getFirstReduceTaskLaunchTime() == 0)
        summary.setFirstReduceTaskLaunchTime(rtase.getStartTime());
      break;
    case JOB_FINISHED:
      JobFinishedEvent jfe = (JobFinishedEvent) event;
      summary.setJobFinishTime(jfe.getFinishTime());
      summary.setNumFinishedMaps(jfe.getFinishedMaps());
      summary.setNumFailedMaps(jfe.getFailedMaps());
      summary.setNumFinishedReduces(jfe.getFinishedReduces());
      summary.setNumFailedReduces(jfe.getFailedReduces());
      if (summary.getJobStatus() == null)
        summary
            .setJobStatus(org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED
                .toString());
      // TODO JOB_FINISHED does not have state. Effectively job history does not
      // have state about the finished job.
      break;
    case JOB_FAILED:
    case JOB_KILLED:
      JobUnsuccessfulCompletionEvent juce = (JobUnsuccessfulCompletionEvent) event;
      summary.setJobStatus(juce.getStatus());
      break;
    // TODO Verify: MRV2 + MRV1. A JOB_FINISHED event will always come in after
    // this. Stats on taskCounts can be set via that.
    }
  }

  protected void closeEventWriter(JobId jobId) throws IOException {

    final MetaInfo mi = fileMap.get(jobId);
    if (mi == null) {
      throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
    }

    if (!mi.isWriterActive()) {
      throw new IOException(
          "Inactive Writer: Likely received multiple JobFinished / JobUnsuccessful events for JobId: ["
              + jobId + "]");
    }

    // Close the Writer
    try {
      mi.closeWriter();
    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + jobId);
      throw e;
    }
     
    if (mi.getHistoryFile() == null) {
      LOG.warn("No file for job-history with " + jobId + " found in cache!");
    }
    if (mi.getConfFile() == null) {
      LOG.warn("No file for jobconf with " + jobId + " found in cache!");
    }
      
    // Writing out the summary file.
    // TODO JH enhancement - reuse this file to store additional indexing info
    // like ACLs, etc. JHServer can use HDFS append to build an index file
    // with more info than is available via the filename.
    Path qualifiedSummaryDoneFile = null;
    FSDataOutputStream summaryFileOut = null;
    try {
      String doneSummaryFileName = getTempFileName(JobHistoryUtils
          .getIntermediateSummaryFileName(jobId));
      qualifiedSummaryDoneFile = doneDirFS.makeQualified(new Path(
          doneDirPrefixPath, doneSummaryFileName));
      summaryFileOut = doneDirFS.create(qualifiedSummaryDoneFile, true);
      summaryFileOut.writeUTF(mi.getJobSummary().getJobSummaryString());
      summaryFileOut.close();
    } catch (IOException e) {
      LOG.info("Unable to write out JobSummaryInfo to ["
          + qualifiedSummaryDoneFile + "]", e);
      throw e;
    }

    try {

      // Move historyFile to Done Folder.
      Path qualifiedDoneFile = null;
      if (mi.getHistoryFile() != null) {
        Path historyFile = mi.getHistoryFile();
        Path qualifiedLogFile = stagingDirFS.makeQualified(historyFile);
        String doneJobHistoryFileName =
            getTempFileName(FileNameIndexUtils.getDoneFileName(mi
                .getJobIndexInfo()));
        qualifiedDoneFile =
            doneDirFS.makeQualified(new Path(doneDirPrefixPath,
                doneJobHistoryFileName));
        moveToDoneNow(qualifiedLogFile, qualifiedDoneFile);
      }

      // Move confFile to Done Folder
      Path qualifiedConfDoneFile = null;
      if (mi.getConfFile() != null) {
        Path confFile = mi.getConfFile();
        Path qualifiedConfFile = stagingDirFS.makeQualified(confFile);
        String doneConfFileName =
            getTempFileName(JobHistoryUtils
                .getIntermediateConfFileName(jobId));
        qualifiedConfDoneFile =
            doneDirFS.makeQualified(new Path(doneDirPrefixPath,
                doneConfFileName));
        moveToDoneNow(qualifiedConfFile, qualifiedConfDoneFile);
      }
      
      moveTmpToDone(qualifiedSummaryDoneFile);
      moveTmpToDone(qualifiedConfDoneFile);
      moveTmpToDone(qualifiedDoneFile);

    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + jobId);
      throw e;
    }
  }

  private class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;
    JobIndexInfo jobIndexInfo;
    JobSummary jobSummary;

    MetaInfo(Path historyFile, Path conf, EventWriter writer, long submitTime,
             String user, String jobName, JobId jobId) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
      this.jobIndexInfo = new JobIndexInfo(submitTime, -1, user, jobName, jobId, -1, -1);
      this.jobSummary = new JobSummary();
    }

    Path getHistoryFile() { return historyFile; }

    Path getConfFile() {return confFile; } 

    JobIndexInfo getJobIndexInfo() { return jobIndexInfo; }

    JobSummary getJobSummary() { return jobSummary; }

    boolean isWriterActive() {return writer != null ; }

    void closeWriter() throws IOException {
      synchronized (lock) {
      if (writer != null) {
        writer.close();
      }
      writer = null;
    }
    }

    void writeEvent(HistoryEvent event) throws IOException {
      synchronized (lock) {
      if (writer != null) {
        writer.write(event);
        writer.flush();
      }
    }
  }
  }

  private void moveTmpToDone(Path tmpPath) throws IOException {
    if (tmpPath != null) {
      String tmpFileName = tmpPath.getName();
      String fileName = getFileNameFromTmpFN(tmpFileName);
      Path path = new Path(tmpPath.getParent(), fileName);
      doneDirFS.rename(tmpPath, path);
      LOG.info("Moved tmp to done: " + tmpPath + " to " + path);
    }
  }
  
  // TODO If the FS objects are the same, this should be a rename instead of a
  // copy.
  private void moveToDoneNow(Path fromPath, Path toPath) throws IOException {
    // check if path exists, in case of retries it may not exist
    if (stagingDirFS.exists(fromPath)) {
      LOG.info("Moving " + fromPath.toString() + " to " + toPath.toString());
      // TODO temporarily removing the existing dst
      if (doneDirFS.exists(toPath)) {
        doneDirFS.delete(toPath, true);
      }
      boolean copied = FileUtil.copy(stagingDirFS, fromPath, doneDirFS, toPath,
          false, conf);

      if (copied)
        LOG.info("Copied to done location: " + toPath);
      else 
          LOG.info("copy failed");
      doneDirFS.setPermission(toPath, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_FILE_PERMISSIONS));
      
      stagingDirFS.delete(fromPath, false);
    }
    }

  boolean pathExists(FileSystem fileSys, Path path) throws IOException {
    return fileSys.exists(path);
  }

  private String getTempFileName(String srcFile) {
    return srcFile + "_tmp";
  }
  
  private String getFileNameFromTmpFN(String tmpFileName) {
    //TODO. Some error checking here.
    return tmpFileName.substring(0, tmpFileName.length()-4);
  }
}
