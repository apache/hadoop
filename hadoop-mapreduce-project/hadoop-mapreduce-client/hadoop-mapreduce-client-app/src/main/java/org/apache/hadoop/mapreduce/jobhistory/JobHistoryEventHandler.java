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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.ClientHandlerException;

/**
 * The job history events get routed to this class. This class writes the Job
 * history events to the DFS directly into a staging dir and then moved to a
 * done-dir. JobHistory implementation is in this package to access package
 * private classes.
 */
public class JobHistoryEventHandler extends AbstractService
    implements EventHandler<JobHistoryEvent> {
  private static final JsonNodeFactory FACTORY =
      new ObjectMapper().getNodeFactory();

  private final AppContext context;
  private final int startCount;

  private int eventCounter;

  // Those file systems may differ from the job configuration
  // See org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils
  // #ensurePathInDefaultFileSystem
  private FileSystem stagingDirFS; // log Dir FileSystem
  private FileSystem doneDirFS; // done Dir FileSystem


  private Path stagingDirPath = null;
  private Path doneDirPrefixPath = null; // folder for completed jobs

  private int maxUnflushedCompletionEvents;
  private int postJobCompletionMultiplier;
  private long flushTimeout;
  private int minQueueSizeForBatchingFlushes; // TODO: Rename

  private int numUnflushedCompletionEvents = 0;
  private boolean isTimerActive;
  private EventWriter.WriteMode jhistMode =
      EventWriter.WriteMode.JSON;

  protected BlockingQueue<JobHistoryEvent> eventQueue =
    new LinkedBlockingQueue<JobHistoryEvent>();
  protected Thread eventHandlingThread;
  private volatile boolean stopped;
  private final Object lock = new Object();

  private static final Log LOG = LogFactory.getLog(
      JobHistoryEventHandler.class);

  protected static final Map<JobId, MetaInfo> fileMap =
    Collections.<JobId,MetaInfo>synchronizedMap(new HashMap<JobId,MetaInfo>());

  // should job completion be force when the AM shuts down?
  protected volatile boolean forceJobCompletion = false;

  protected TimelineClient timelineClient;

  private boolean timelineServiceV2Enabled = false;

  private static String MAPREDUCE_JOB_ENTITY_TYPE = "MAPREDUCE_JOB";
  private static String MAPREDUCE_TASK_ENTITY_TYPE = "MAPREDUCE_TASK";
  private static final String MAPREDUCE_TASK_ATTEMPT_ENTITY_TYPE =
      "MAPREDUCE_TASK_ATTEMPT";

  public JobHistoryEventHandler(AppContext context, int startCount) {
    super("JobHistoryEventHandler");
    this.context = context;
    this.startCount = startCount;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.service.AbstractService#init(org.
   * apache.hadoop.conf.Configuration)
   * Initializes the FileSystem and Path objects for the log and done directories.
   * Creates these directories if they do not already exist.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String jobId =
      TypeConverter.fromYarn(context.getApplicationID()).toString();
    
    String stagingDirStr = null;
    String doneDirStr = null;
    String userDoneDirStr = null;
    try {
      stagingDirStr = JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf,
          jobId);
      doneDirStr =
          JobHistoryUtils.getConfiguredHistoryIntermediateDoneDirPrefix(conf);
      userDoneDirStr =
          JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf);
    } catch (IOException e) {
      LOG.error("Failed while getting the configured log directories", e);
      throw new YarnRuntimeException(e);
    }

    //Check for the existence of the history staging dir. Maybe create it. 
    try {
      stagingDirPath =
          FileContext.getFileContext(conf).makeQualified(new Path(stagingDirStr));
      stagingDirFS = FileSystem.get(stagingDirPath.toUri(), conf);
      mkdir(stagingDirFS, stagingDirPath, new FsPermission(
          JobHistoryUtils.HISTORY_STAGING_DIR_PERMISSIONS));
    } catch (IOException e) {
      LOG.error("Failed while checking for/creating  history staging path: ["
          + stagingDirPath + "]", e);
      throw new YarnRuntimeException(e);
    }

    //Check for the existence of intermediate done dir.
    Path doneDirPath = null;
    try {
      doneDirPath = FileContext.getFileContext(conf).makeQualified(new Path(doneDirStr));
      doneDirFS = FileSystem.get(doneDirPath.toUri(), conf);
      // This directory will be in a common location, or this may be a cluster
      // meant for a single user. Creating based on the conf. Should ideally be
      // created by the JobHistoryServer or as part of deployment.
      if (!doneDirFS.exists(doneDirPath)) {
      if (JobHistoryUtils.shouldCreateNonUserDirectory(conf)) {
        LOG.info("Creating intermediate history logDir: ["
            + doneDirPath
            + "] + based on conf. Should ideally be created by the JobHistoryServer: "
            + MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR);
          mkdir(
              doneDirFS,
              doneDirPath,
              new FsPermission(
            JobHistoryUtils.HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS
                .toShort()));
          // TODO Temporary toShort till new FsPermission(FsPermissions)
          // respects
        // sticky
      } else {
          String message = "Not creating intermediate history logDir: ["
                + doneDirPath
                + "] based on conf: "
                + MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR
                + ". Either set to true or pre-create this directory with" +
                " appropriate permissions";
        LOG.error(message);
        throw new YarnRuntimeException(message);
      }
      }
    } catch (IOException e) {
      LOG.error("Failed checking for the existance of history intermediate " +
      		"done directory: [" + doneDirPath + "]");
      throw new YarnRuntimeException(e);
    }

    //Check/create user directory under intermediate done dir.
    try {
      doneDirPrefixPath =
          FileContext.getFileContext(conf).makeQualified(new Path(userDoneDirStr));
      mkdir(doneDirFS, doneDirPrefixPath, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_USER_DIR_PERMISSIONS));
    } catch (IOException e) {
      LOG.error("Error creating user intermediate history done directory: [ "
          + doneDirPrefixPath + "]", e);
      throw new YarnRuntimeException(e);
    }

    // Maximum number of unflushed completion-events that can stay in the queue
    // before flush kicks in.
    maxUnflushedCompletionEvents =
        conf.getInt(MRJobConfig.MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS);
    // We want to cut down flushes after job completes so as to write quicker,
    // so we increase maxUnflushedEvents post Job completion by using the
    // following multiplier.
    postJobCompletionMultiplier =
        conf.getInt(
            MRJobConfig.MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER);
    // Max time until which flush doesn't take place.
    flushTimeout =
        conf.getLong(MRJobConfig.MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS);
    minQueueSizeForBatchingFlushes =
        conf.getInt(
            MRJobConfig.MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD);

    // TODO replace MR specific configurations on timeline service with getting
    // configuration from RM through registerApplicationMaster() in
    // ApplicationMasterProtocol with return value for timeline service
    // configuration status: off, on_with_v1 or on_with_v2.
    if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA,
        MRJobConfig.DEFAULT_MAPREDUCE_JOB_EMIT_TIMELINE_DATA)) {
      LOG.info("Emitting job history data to the timeline service is enabled");
      if (YarnConfiguration.timelineServiceEnabled(conf)) {

        timelineClient =
            ((MRAppMaster.RunningAppContext)context).getTimelineClient();
        timelineClient.init(conf);
        timelineServiceV2Enabled =
            YarnConfiguration.timelineServiceV2Enabled(conf);
        LOG.info("Timeline service is enabled; version: " +
            YarnConfiguration.getTimelineServiceVersion(conf));
      } else {
        LOG.info("Timeline service is not enabled");
      }
    } else {
      LOG.info("Emitting job history data to the timeline server is not " +
          "enabled");
    }

    // Flag for setting
    String jhistFormat = conf.get(JHAdminConfig.MR_HS_JHIST_FORMAT,
        JHAdminConfig.DEFAULT_MR_HS_JHIST_FORMAT);
    if (jhistFormat.equals("json")) {
      jhistMode = EventWriter.WriteMode.JSON;
    } else if (jhistFormat.equals("binary")) {
      jhistMode = EventWriter.WriteMode.BINARY;
    } else {
      LOG.warn("Unrecognized value '" + jhistFormat + "' for property " +
          JHAdminConfig.MR_HS_JHIST_FORMAT + ".  Valid values are " +
          "'json' or 'binary'.  Falling back to default value '" +
          JHAdminConfig.DEFAULT_MR_HS_JHIST_FORMAT + "'.");
    }

    super.serviceInit(conf);
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
  protected void serviceStart() throws Exception {
    if (timelineClient != null) {
      timelineClient.start();
    }
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        JobHistoryEvent event = null;
        while (!stopped && !Thread.currentThread().isInterrupted()) {

          // Log the size of the history-event-queue every so often.
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            eventCounter = 0;
            LOG.info("Size of the JobHistory event queue is "
                + eventQueue.size());
          } else {
            eventCounter++;
          }

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
                LOG.debug("Event handling interrupted");
                Thread.currentThread().interrupt();
              }
            }
          }
        }
    }, "eventHandlingThread");
    eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping JobHistoryEventHandler. "
        + "Size of the outstanding queue size is " + eventQueue.size());
    stopped = true;
    //do not interrupt while event handling is in progress
    synchronized(lock) {
      if (eventHandlingThread != null) {
        LOG.debug("Interrupting Event Handling thread");
        eventHandlingThread.interrupt();
      } else {
        LOG.debug("Null event handling thread");
      }
    }

    try {
      if (eventHandlingThread != null) {
        LOG.debug("Waiting for Event Handling thread to complete");
        eventHandlingThread.join();
      }
    } catch (InterruptedException ie) {
      LOG.info("Interrupted Exception while stopping", ie);
    }

    // Cancel all timers - so that they aren't invoked during or after
    // the metaInfo object is wrapped up.
    for (MetaInfo mi : fileMap.values()) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Shutting down timer for " + mi);
        }
        mi.shutDownTimer();
      } catch (IOException e) {
        LOG.info("Exception while canceling delayed flush timer. "
            + "Likely caused by a failed flush " + e.getMessage());
      }
    }

    //write all the events remaining in queue
    Iterator<JobHistoryEvent> it = eventQueue.iterator();
    while(it.hasNext()) {
      JobHistoryEvent ev = it.next();
      LOG.info("In stop, writing event " + ev.getType());
      handleEvent(ev);
    }

    // Process JobUnsuccessfulCompletionEvent for jobIds which still haven't
    // closed their event writers
    if(forceJobCompletion) {
      for (Map.Entry<JobId,MetaInfo> jobIt : fileMap.entrySet()) {
        JobId toClose = jobIt.getKey();
        MetaInfo mi = jobIt.getValue();
        if(mi != null && mi.isWriterActive()) {
          LOG.warn("Found jobId " + toClose
            + " to have not been closed. Will close");
          //Create a JobFinishEvent so that it is written to the job history
          final Job job = context.getJob(toClose);
          JobUnsuccessfulCompletionEvent jucEvent =
            new JobUnsuccessfulCompletionEvent(TypeConverter.fromYarn(toClose),
                System.currentTimeMillis(), job.getCompletedMaps(),
                job.getCompletedReduces(),
                createJobStateForJobUnsuccessfulCompletionEvent(
                    mi.getForcedJobStateOnShutDown()),
                job.getDiagnostics());
          JobHistoryEvent jfEvent = new JobHistoryEvent(toClose, jucEvent);
          //Bypass the queue mechanism which might wait. Call the method directly
          handleEvent(jfEvent);
        }
      }
    }

    //close all file handles
    for (MetaInfo mi : fileMap.values()) {
      try {
        mi.closeWriter();
      } catch (IOException e) {
        LOG.info("Exception while closing file " + e.getMessage());
      }
    }
    if (timelineClient != null) {
      timelineClient.stop();
    }
    LOG.info("Stopped JobHistoryEventHandler. super.stop()");
    super.serviceStop();
  }

  protected EventWriter createEventWriter(Path historyFilePath)
      throws IOException {
    FSDataOutputStream out = stagingDirFS.create(historyFilePath, true);
    return new EventWriter(out, this.jhistMode);
  }
  
  /**
   * Create an event writer for the Job represented by the jobID.
   * Writes out the job configuration to the log directory.
   * This should be the first call to history for a job
   * 
   * @param jobId the jobId.
   * @param amStartedEvent
   * @throws IOException
   */
  protected void setupEventWriter(JobId jobId, AMStartedEvent amStartedEvent)
      throws IOException {
    if (stagingDirPath == null) {
      LOG.error("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }

    MetaInfo oldFi = fileMap.get(jobId);
    Configuration conf = getConfig();

    // TODO Ideally this should be written out to the job dir
    // (.staging/jobid/files - RecoveryService will need to be patched)
    Path historyFile = JobHistoryUtils.getStagingJobHistoryFile(
        stagingDirPath, jobId, startCount);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    if (user == null) {
      throw new IOException(
          "User is null while setting up jobhistory eventwriter");
    }

    String jobName = context.getJob(jobId).getName();
    EventWriter writer = (oldFi == null) ? null : oldFi.writer;
 
    Path logDirConfPath =
        JobHistoryUtils.getStagingConfFile(stagingDirPath, jobId, startCount);
    if (writer == null) {
      try {
        writer = createEventWriter(historyFile);
        LOG.info("Event Writer setup for JobId: " + jobId + ", File: "
            + historyFile);
      } catch (IOException ioe) {
        LOG.info("Could not create log file: [" + historyFile + "] + for job "
            + "[" + jobName + "]");
        throw ioe;
      }
      
      //Write out conf only if the writer isn't already setup.
      if (conf != null) {
        // TODO Ideally this should be written out to the job dir
        // (.staging/jobid/files - RecoveryService will need to be patched)
        if (logDirConfPath != null) {
          Configuration redactedConf = new Configuration(conf);
          MRJobConfUtil.redact(redactedConf);
          try (FSDataOutputStream jobFileOut = stagingDirFS
              .create(logDirConfPath, true)) {
            redactedConf.writeXml(jobFileOut);
          } catch (IOException e) {
            LOG.info("Failed to write the job configuration file", e);
            throw e;
          }
        }
      }
    }

    String queueName = JobConf.DEFAULT_QUEUE_NAME;
    if (conf != null) {
      queueName = conf.get(MRJobConfig.QUEUE_NAME, JobConf.DEFAULT_QUEUE_NAME);
    }

    MetaInfo fi = new MetaInfo(historyFile, logDirConfPath, writer,
        user, jobName, jobId, amStartedEvent.getForcedJobStateOnShutDown(),
        queueName);
    fi.getJobSummary().setJobId(jobId);
    fi.getJobSummary().setJobLaunchTime(amStartedEvent.getStartTime());
    fi.getJobSummary().setJobSubmitTime(amStartedEvent.getSubmitTime());
    fi.getJobIndexInfo().setJobStartTime(amStartedEvent.getStartTime());
    fi.getJobIndexInfo().setSubmitTime(amStartedEvent.getSubmitTime());
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
      if (isJobCompletionEvent(event.getHistoryEvent())) {
        // When the job is complete, flush slower but write faster.
        maxUnflushedCompletionEvents =
            maxUnflushedCompletionEvents * postJobCompletionMultiplier;
      }

      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  private boolean isJobCompletionEvent(HistoryEvent historyEvent) {
    if (EnumSet.of(EventType.JOB_FINISHED, EventType.JOB_FAILED,
        EventType.JOB_KILLED).contains(historyEvent.getEventType())) {
      return true;
    }
    return false;
  }

  @Private
  public void handleEvent(JobHistoryEvent event) {
    synchronized (lock) {

      // If this is JobSubmitted Event, setup the writer
      if (event.getHistoryEvent().getEventType() == EventType.AM_STARTED) {
        try {
          AMStartedEvent amStartedEvent =
              (AMStartedEvent) event.getHistoryEvent();
          setupEventWriter(event.getJobID(), amStartedEvent);
        } catch (IOException ioe) {
          LOG.error("Error JobHistoryEventHandler in handleEvent: " + event,
              ioe);
          throw new YarnRuntimeException(ioe);
        }
      }

      // For all events
      // (1) Write it out
      // (2) Process it for JobSummary
      // (3) Process it for ATS (if enabled)
      MetaInfo mi = fileMap.get(event.getJobID());
      try {
        HistoryEvent historyEvent = event.getHistoryEvent();
        if (! (historyEvent instanceof NormalizedResourceEvent)) {
          mi.writeEvent(historyEvent);
        }
        processEventForJobSummary(event.getHistoryEvent(), mi.getJobSummary(),
            event.getJobID());
        if (timelineClient != null) {
          if (timelineServiceV2Enabled) {
            processEventForNewTimelineService(historyEvent, event.getJobID(),
                event.getTimestamp());
          } else {
            processEventForTimelineServer(historyEvent, event.getJobID(),
                event.getTimestamp());
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("In HistoryEventHandler "
              + event.getHistoryEvent().getEventType());
        }
      } catch (IOException e) {
        LOG.error("Error writing History Event: " + event.getHistoryEvent(),
            e);
        throw new YarnRuntimeException(e);
      }

      if (event.getHistoryEvent().getEventType() == EventType.JOB_SUBMITTED) {
        JobSubmittedEvent jobSubmittedEvent =
            (JobSubmittedEvent) event.getHistoryEvent();
        mi.getJobIndexInfo().setSubmitTime(jobSubmittedEvent.getSubmitTime());
        mi.getJobIndexInfo().setQueueName(jobSubmittedEvent.getJobQueueName());
      }
      //initialize the launchTime in the JobIndexInfo of MetaInfo
      if(event.getHistoryEvent().getEventType() == EventType.JOB_INITED ){
        JobInitedEvent jie = (JobInitedEvent) event.getHistoryEvent();
        mi.getJobIndexInfo().setJobStartTime(jie.getLaunchTime());
      }
      
      if (event.getHistoryEvent().getEventType() == EventType.JOB_QUEUE_CHANGED) {
        JobQueueChangeEvent jQueueEvent =
            (JobQueueChangeEvent) event.getHistoryEvent();
        mi.getJobIndexInfo().setQueueName(jQueueEvent.getJobQueueName());
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
          mi.getJobIndexInfo().setJobStatus(JobState.SUCCEEDED.toString());
          closeEventWriter(event.getJobID());
          processDoneFiles(event.getJobID());
        } catch (IOException e) {
          throw new YarnRuntimeException(e);
        }
      }
      // In case of JOB_ERROR, only process all the Done files(e.g. job
      // summary, job history file etc.) if it is last AM retry.
      if (event.getHistoryEvent().getEventType() == EventType.JOB_ERROR) {
        try {
          JobUnsuccessfulCompletionEvent jucEvent =
              (JobUnsuccessfulCompletionEvent) event.getHistoryEvent();
          mi.getJobIndexInfo().setFinishTime(jucEvent.getFinishTime());
          mi.getJobIndexInfo().setNumMaps(jucEvent.getFinishedMaps());
          mi.getJobIndexInfo().setNumReduces(jucEvent.getFinishedReduces());
          mi.getJobIndexInfo().setJobStatus(jucEvent.getStatus());
          closeEventWriter(event.getJobID());
          if(context.isLastAMRetry())
            processDoneFiles(event.getJobID());
        } catch (IOException e) {
          throw new YarnRuntimeException(e);
        }
      }

      if (event.getHistoryEvent().getEventType() == EventType.JOB_FAILED
          || event.getHistoryEvent().getEventType() == EventType.JOB_KILLED) {
        try {
          JobUnsuccessfulCompletionEvent jucEvent = 
              (JobUnsuccessfulCompletionEvent) event
              .getHistoryEvent();
          mi.getJobIndexInfo().setFinishTime(jucEvent.getFinishTime());
          mi.getJobIndexInfo().setNumMaps(jucEvent.getFinishedMaps());
          mi.getJobIndexInfo().setNumReduces(jucEvent.getFinishedReduces());
          mi.getJobIndexInfo().setJobStatus(jucEvent.getStatus());
          closeEventWriter(event.getJobID());
          processDoneFiles(event.getJobID());
        } catch (IOException e) {
          throw new YarnRuntimeException(e);
        }
      }
    }
  }

  public void processEventForJobSummary(HistoryEvent event, JobSummary summary, 
      JobId jobId) {
    // context.getJob could be used for some of this info as well.
    switch (event.getEventType()) {
    case JOB_SUBMITTED:
      JobSubmittedEvent jse = (JobSubmittedEvent) event;
      summary.setUser(jse.getUserName());
      summary.setQueue(jse.getJobQueueName());
      summary.setJobSubmitTime(jse.getSubmitTime());
      summary.setJobName(jse.getJobName());
      break;
    case NORMALIZED_RESOURCE:
      NormalizedResourceEvent normalizedResourceEvent = 
            (NormalizedResourceEvent) event;
      if (normalizedResourceEvent.getTaskType() == TaskType.MAP) {
        summary.setResourcesPerMap((int) normalizedResourceEvent.getMemory());
      } else if (normalizedResourceEvent.getTaskType() == TaskType.REDUCE) {
        summary.setResourcesPerReduce((int) normalizedResourceEvent.getMemory());
      }
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
      setSummarySlotSeconds(summary, jfe.getTotalCounters());
      break;
    case JOB_FAILED:
    case JOB_KILLED:
      JobUnsuccessfulCompletionEvent juce = (JobUnsuccessfulCompletionEvent) event;
      summary.setJobStatus(juce.getStatus());
      summary.setNumFinishedMaps(context.getJob(jobId).getTotalMaps());
      summary.setNumFinishedReduces(context.getJob(jobId).getTotalReduces());
      summary.setJobFinishTime(juce.getFinishTime());
      setSummarySlotSeconds(summary, context.getJob(jobId).getAllCounters());
      break;
    default:
      break;
    }
  }

  private void processEventForTimelineServer(HistoryEvent event, JobId jobId,
          long timestamp) {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(StringUtils.toUpperCase(event.getEventType().name()));
    tEvent.setTimestamp(timestamp);
    TimelineEntity tEntity = new TimelineEntity();

    switch (event.getEventType()) {
      case JOB_SUBMITTED:
        JobSubmittedEvent jse =
            (JobSubmittedEvent) event;
        tEvent.addEventInfo("SUBMIT_TIME", jse.getSubmitTime());
        tEvent.addEventInfo("QUEUE_NAME", jse.getJobQueueName());
        tEvent.addEventInfo("JOB_NAME", jse.getJobName());
        tEvent.addEventInfo("USER_NAME", jse.getUserName());
        tEvent.addEventInfo("JOB_CONF_PATH", jse.getJobConfPath());
        tEvent.addEventInfo("ACLS", jse.getJobAcls());
        tEvent.addEventInfo("JOB_QUEUE_NAME", jse.getJobQueueName());
        tEvent.addEventInfo("WORKFLOW_ID", jse.getWorkflowId());
        tEvent.addEventInfo("WORKFLOW_NAME", jse.getWorkflowName());
        tEvent.addEventInfo("WORKFLOW_NAME_NAME", jse.getWorkflowNodeName());
        tEvent.addEventInfo("WORKFLOW_ADJACENCIES",
                jse.getWorkflowAdjacencies());
        tEvent.addEventInfo("WORKFLOW_TAGS", jse.getWorkflowTags());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_STATUS_CHANGED:
        JobStatusChangedEvent jsce = (JobStatusChangedEvent) event;
        tEvent.addEventInfo("STATUS", jsce.getStatus());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_INFO_CHANGED:
        JobInfoChangeEvent jice = (JobInfoChangeEvent) event;
        tEvent.addEventInfo("SUBMIT_TIME", jice.getSubmitTime());
        tEvent.addEventInfo("LAUNCH_TIME", jice.getLaunchTime());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_INITED:
        JobInitedEvent jie = (JobInitedEvent) event;
        tEvent.addEventInfo("START_TIME", jie.getLaunchTime());
        tEvent.addEventInfo("STATUS", jie.getStatus());
        tEvent.addEventInfo("TOTAL_MAPS", jie.getTotalMaps());
        tEvent.addEventInfo("TOTAL_REDUCES", jie.getTotalReduces());
        tEvent.addEventInfo("UBERIZED", jie.getUberized());
        tEntity.setStartTime(jie.getLaunchTime());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_PRIORITY_CHANGED:
        JobPriorityChangeEvent jpce = (JobPriorityChangeEvent) event;
        tEvent.addEventInfo("PRIORITY", jpce.getPriority().toString());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_QUEUE_CHANGED:
        JobQueueChangeEvent jqe = (JobQueueChangeEvent) event;
        tEvent.addEventInfo("QUEUE_NAMES", jqe.getJobQueueName());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_FAILED:
      case JOB_KILLED:
      case JOB_ERROR:
        JobUnsuccessfulCompletionEvent juce =
              (JobUnsuccessfulCompletionEvent) event;
        tEvent.addEventInfo("FINISH_TIME", juce.getFinishTime());
        tEvent.addEventInfo("NUM_MAPS", juce.getFinishedMaps());
        tEvent.addEventInfo("NUM_REDUCES", juce.getFinishedReduces());
        tEvent.addEventInfo("JOB_STATUS", juce.getStatus());
        tEvent.addEventInfo("DIAGNOSTICS", juce.getDiagnostics());
        tEvent.addEventInfo("FINISHED_MAPS", juce.getFinishedMaps());
        tEvent.addEventInfo("FINISHED_REDUCES", juce.getFinishedReduces());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case JOB_FINISHED:
        JobFinishedEvent jfe = (JobFinishedEvent) event;
        tEvent.addEventInfo("FINISH_TIME", jfe.getFinishTime());
        tEvent.addEventInfo("NUM_MAPS", jfe.getFinishedMaps());
        tEvent.addEventInfo("NUM_REDUCES", jfe.getFinishedReduces());
        tEvent.addEventInfo("FAILED_MAPS", jfe.getFailedMaps());
        tEvent.addEventInfo("FAILED_REDUCES", jfe.getFailedReduces());
        tEvent.addEventInfo("FINISHED_MAPS", jfe.getFinishedMaps());
        tEvent.addEventInfo("FINISHED_REDUCES", jfe.getFinishedReduces());
        tEvent.addEventInfo("MAP_COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(jfe.getMapCounters()));
        tEvent.addEventInfo("REDUCE_COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(jfe.getReduceCounters()));
        tEvent.addEventInfo("TOTAL_COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(jfe.getTotalCounters()));
        tEvent.addEventInfo("JOB_STATUS", JobState.SUCCEEDED.toString());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      case TASK_STARTED:
        TaskStartedEvent tse = (TaskStartedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", tse.getTaskType().toString());
        tEvent.addEventInfo("START_TIME", tse.getStartTime());
        tEvent.addEventInfo("SPLIT_LOCATIONS", tse.getSplitLocations());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tse.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case TASK_FAILED:
        TaskFailedEvent tfe = (TaskFailedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", tfe.getTaskType().toString());
        tEvent.addEventInfo("STATUS", TaskStatus.State.FAILED.toString());
        tEvent.addEventInfo("FINISH_TIME", tfe.getFinishTime());
        tEvent.addEventInfo("ERROR", tfe.getError());
        tEvent.addEventInfo("FAILED_ATTEMPT_ID",
                tfe.getFailedAttemptID() == null ?
                "" : tfe.getFailedAttemptID().toString());
        tEvent.addEventInfo("COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(tfe.getCounters()));
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tfe.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case TASK_UPDATED:
        TaskUpdatedEvent tue = (TaskUpdatedEvent) event;
        tEvent.addEventInfo("FINISH_TIME", tue.getFinishTime());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tue.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case TASK_FINISHED:
        TaskFinishedEvent tfe2 = (TaskFinishedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", tfe2.getTaskType().toString());
        tEvent.addEventInfo("COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(tfe2.getCounters()));
        tEvent.addEventInfo("FINISH_TIME", tfe2.getFinishTime());
        tEvent.addEventInfo("STATUS", TaskStatus.State.SUCCEEDED.toString());
        tEvent.addEventInfo("SUCCESSFUL_TASK_ATTEMPT_ID",
            tfe2.getSuccessfulTaskAttemptId() == null ?
            "" : tfe2.getSuccessfulTaskAttemptId().toString());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tfe2.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case MAP_ATTEMPT_STARTED:
      case CLEANUP_ATTEMPT_STARTED:
      case REDUCE_ATTEMPT_STARTED:
      case SETUP_ATTEMPT_STARTED:
        TaskAttemptStartedEvent tase = (TaskAttemptStartedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", tase.getTaskType().toString());
        tEvent.addEventInfo("TASK_ATTEMPT_ID",
            tase.getTaskAttemptId().toString());
        tEvent.addEventInfo("START_TIME", tase.getStartTime());
        tEvent.addEventInfo("HTTP_PORT", tase.getHttpPort());
        tEvent.addEventInfo("TRACKER_NAME", tase.getTrackerName());
        tEvent.addEventInfo("SHUFFLE_PORT", tase.getShufflePort());
        tEvent.addEventInfo("CONTAINER_ID", tase.getContainerId() == null ?
            "" : tase.getContainerId().toString());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tase.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case MAP_ATTEMPT_FAILED:
      case CLEANUP_ATTEMPT_FAILED:
      case REDUCE_ATTEMPT_FAILED:
      case SETUP_ATTEMPT_FAILED:
      case MAP_ATTEMPT_KILLED:
      case CLEANUP_ATTEMPT_KILLED:
      case REDUCE_ATTEMPT_KILLED:
      case SETUP_ATTEMPT_KILLED:
        TaskAttemptUnsuccessfulCompletionEvent tauce =
                (TaskAttemptUnsuccessfulCompletionEvent) event;
        tEvent.addEventInfo("TASK_TYPE", tauce.getTaskType().toString());
        tEvent.addEventInfo("TASK_ATTEMPT_ID",
            tauce.getTaskAttemptId() == null ?
            "" : tauce.getTaskAttemptId().toString());
        tEvent.addEventInfo("FINISH_TIME", tauce.getFinishTime());
        tEvent.addEventInfo("ERROR", tauce.getError());
        tEvent.addEventInfo("STATUS", tauce.getTaskStatus());
        tEvent.addEventInfo("HOSTNAME", tauce.getHostname());
        tEvent.addEventInfo("PORT", tauce.getPort());
        tEvent.addEventInfo("RACK_NAME", tauce.getRackName());
        tEvent.addEventInfo("SHUFFLE_FINISH_TIME", tauce.getFinishTime());
        tEvent.addEventInfo("SORT_FINISH_TIME", tauce.getFinishTime());
        tEvent.addEventInfo("MAP_FINISH_TIME", tauce.getFinishTime());
        tEvent.addEventInfo("COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(tauce.getCounters()));
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tauce.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case MAP_ATTEMPT_FINISHED:
        MapAttemptFinishedEvent mafe = (MapAttemptFinishedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", mafe.getTaskType().toString());
        tEvent.addEventInfo("FINISH_TIME", mafe.getFinishTime());
        tEvent.addEventInfo("STATUS", mafe.getTaskStatus());
        tEvent.addEventInfo("STATE", mafe.getState());
        tEvent.addEventInfo("MAP_FINISH_TIME", mafe.getMapFinishTime());
        tEvent.addEventInfo("COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(mafe.getCounters()));
        tEvent.addEventInfo("HOSTNAME", mafe.getHostname());
        tEvent.addEventInfo("PORT", mafe.getPort());
        tEvent.addEventInfo("RACK_NAME", mafe.getRackName());
        tEvent.addEventInfo("ATTEMPT_ID", mafe.getAttemptId() == null ?
            "" : mafe.getAttemptId().toString());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(mafe.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case REDUCE_ATTEMPT_FINISHED:
        ReduceAttemptFinishedEvent rafe = (ReduceAttemptFinishedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", rafe.getTaskType().toString());
        tEvent.addEventInfo("ATTEMPT_ID", rafe.getAttemptId() == null ?
            "" : rafe.getAttemptId().toString());
        tEvent.addEventInfo("FINISH_TIME", rafe.getFinishTime());
        tEvent.addEventInfo("STATUS", rafe.getTaskStatus());
        tEvent.addEventInfo("STATE", rafe.getState());
        tEvent.addEventInfo("SHUFFLE_FINISH_TIME", rafe.getShuffleFinishTime());
        tEvent.addEventInfo("SORT_FINISH_TIME", rafe.getSortFinishTime());
        tEvent.addEventInfo("COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(rafe.getCounters()));
        tEvent.addEventInfo("HOSTNAME", rafe.getHostname());
        tEvent.addEventInfo("PORT", rafe.getPort());
        tEvent.addEventInfo("RACK_NAME", rafe.getRackName());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(rafe.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case SETUP_ATTEMPT_FINISHED:
      case CLEANUP_ATTEMPT_FINISHED:
        TaskAttemptFinishedEvent tafe = (TaskAttemptFinishedEvent) event;
        tEvent.addEventInfo("TASK_TYPE", tafe.getTaskType().toString());
        tEvent.addEventInfo("ATTEMPT_ID", tafe.getAttemptId() == null ?
            "" : tafe.getAttemptId().toString());
        tEvent.addEventInfo("FINISH_TIME", tafe.getFinishTime());
        tEvent.addEventInfo("STATUS", tafe.getTaskStatus());
        tEvent.addEventInfo("STATE", tafe.getState());
        tEvent.addEventInfo("COUNTERS_GROUPS",
            JobHistoryEventUtils.countersToJSON(tafe.getCounters()));
        tEvent.addEventInfo("HOSTNAME", tafe.getHostname());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(tafe.getTaskId().toString());
        tEntity.setEntityType(MAPREDUCE_TASK_ENTITY_TYPE);
        tEntity.addRelatedEntity(MAPREDUCE_JOB_ENTITY_TYPE, jobId.toString());
        break;
      case AM_STARTED:
        AMStartedEvent ase = (AMStartedEvent) event;
        tEvent.addEventInfo("APPLICATION_ATTEMPT_ID",
                ase.getAppAttemptId() == null ?
                "" : ase.getAppAttemptId().toString());
        tEvent.addEventInfo("CONTAINER_ID", ase.getContainerId() == null ?
                "" : ase.getContainerId().toString());
        tEvent.addEventInfo("NODE_MANAGER_HOST", ase.getNodeManagerHost());
        tEvent.addEventInfo("NODE_MANAGER_PORT", ase.getNodeManagerPort());
        tEvent.addEventInfo("NODE_MANAGER_HTTP_PORT",
                ase.getNodeManagerHttpPort());
        tEvent.addEventInfo("START_TIME", ase.getStartTime());
        tEvent.addEventInfo("SUBMIT_TIME", ase.getSubmitTime());
        tEntity.addEvent(tEvent);
        tEntity.setEntityId(jobId.toString());
        tEntity.setEntityType(MAPREDUCE_JOB_ENTITY_TYPE);
        break;
      default:
        break;
    }

    try {
      TimelinePutResponse response = timelineClient.putEntities(tEntity);
      List<TimelinePutResponse.TimelinePutError> errors = response.getErrors();
      if (errors.size() == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Timeline entities are successfully put in event " + event
              .getEventType());
        }
      } else {
        for (TimelinePutResponse.TimelinePutError error : errors) {
          LOG.error(
              "Error when publishing entity [" + error.getEntityType() + ","
                  + error.getEntityId() + "], server side error code: "
                  + error.getErrorCode());
        }
      }
    } catch (YarnException | IOException | ClientHandlerException ex) {
      LOG.error("Error putting entity " + tEntity.getEntityId() + " to Timeline"
          + "Server", ex);
    }
  }

  // create JobEntity from HistoryEvent with adding other info, like:
  // jobId, timestamp and entityType.
  private org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
      createJobEntity(HistoryEvent event, long timestamp, JobId jobId,
      String entityType, boolean setCreatedTime) {

    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity entity =
        createBaseEntity(event, timestamp, entityType, setCreatedTime);
    entity.setId(jobId.toString());
    return entity;
  }

  private org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
      createJobEntity(JobId jobId) {
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity entity =
        new org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity();
    entity.setId(jobId.toString());
    entity.setType(MAPREDUCE_JOB_ENTITY_TYPE);
    return entity;
  }

  // create ApplicationEntity with job finished Metrics from HistoryEvent
  private org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
      createAppEntityWithJobMetrics(HistoryEvent event, JobId jobId) {
    ApplicationEntity entity = new ApplicationEntity();
    entity.setId(jobId.getAppId().toString());
    entity.setMetrics(event.getTimelineMetrics());
    return entity;
  }

  // create BaseEntity from HistoryEvent with adding other info, like:
  // timestamp and entityType.
  private org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
      createBaseEntity(HistoryEvent event, long timestamp, String entityType,
      boolean setCreatedTime) {
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent tEvent =
        event.toTimelineEvent();
    tEvent.setTimestamp(timestamp);

    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity entity =
        new org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity();
    entity.addEvent(tEvent);
    entity.setType(entityType);
    if (setCreatedTime) {
      entity.setCreatedTime(timestamp);
    }
    Set<TimelineMetric> timelineMetrics = event.getTimelineMetrics();
    if (timelineMetrics != null) {
      entity.setMetrics(timelineMetrics);
    }
    return entity;
  }

  // create TaskEntity from HistoryEvent with adding other info, like:
  // taskId, jobId, timestamp, entityType and relatedJobEntity.
  private org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
      createTaskEntity(HistoryEvent event, long timestamp, String taskId,
      String entityType, String relatedJobEntity, JobId jobId,
      boolean setCreatedTime) {
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity entity =
        createBaseEntity(event, timestamp, entityType, setCreatedTime);
    entity.setId(taskId);
    if (event.getEventType() == EventType.TASK_STARTED) {
      entity.addInfo("TASK_TYPE",
          ((TaskStartedEvent)event).getTaskType().toString());
    }
    entity.addIsRelatedToEntity(relatedJobEntity, jobId.toString());
    return entity;
  }

  // create TaskAttemptEntity from HistoryEvent with adding other info, like:
  // timestamp, taskAttemptId, entityType, relatedTaskEntity and taskId.
  private org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
      createTaskAttemptEntity(HistoryEvent event, long timestamp,
      String taskAttemptId, String entityType, String relatedTaskEntity,
      String taskId, boolean setCreatedTime) {
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity entity =
        createBaseEntity(event, timestamp, entityType, setCreatedTime);
    entity.setId(taskAttemptId);
    entity.addIsRelatedToEntity(relatedTaskEntity, taskId);
    return entity;
  }

  private void publishConfigsOnJobSubmittedEvent(JobSubmittedEvent event,
      JobId jobId) {
    if (event.getJobConf() == null) {
      return;
    }
    // Publish job configurations both as job and app entity.
    // Configs are split into multiple entities if they exceed 100kb in size.
    org.apache.hadoop.yarn.api.records.timelineservice.
        TimelineEntity jobEntityForConfigs = createJobEntity(jobId);
    ApplicationEntity appEntityForConfigs = new ApplicationEntity();
    String appId = jobId.getAppId().toString();
    appEntityForConfigs.setId(appId);
    try {
      int configSize = 0;
      for (Map.Entry<String, String> entry : event.getJobConf()) {
        int size = entry.getKey().length() + entry.getValue().length();
        configSize += size;
        if (configSize > JobHistoryEventUtils.ATS_CONFIG_PUBLISH_SIZE_BYTES) {
          if (jobEntityForConfigs.getConfigs().size() > 0) {
            timelineClient.putEntities(jobEntityForConfigs);
            timelineClient.putEntities(appEntityForConfigs);
            jobEntityForConfigs = createJobEntity(jobId);
            appEntityForConfigs = new ApplicationEntity();
            appEntityForConfigs.setId(appId);
          }
          configSize = size;
        }
        jobEntityForConfigs.addConfig(entry.getKey(), entry.getValue());
        appEntityForConfigs.addConfig(entry.getKey(), entry.getValue());
      }
      if (configSize > 0) {
        timelineClient.putEntities(jobEntityForConfigs);
        timelineClient.putEntities(appEntityForConfigs);
      }
    } catch (IOException | YarnException e) {
      LOG.error("Exception while publishing configs on JOB_SUBMITTED Event " +
          " for the job : " + jobId, e);
    }
  }

  private void processEventForNewTimelineService(HistoryEvent event,
      JobId jobId, long timestamp) {
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity tEntity =
        null;
    String taskId = null;
    String taskAttemptId = null;
    boolean setCreatedTime = false;

    switch (event.getEventType()) {
    // Handle job events
    case JOB_SUBMITTED:
      setCreatedTime = true;
      break;
    case JOB_STATUS_CHANGED:
    case JOB_INFO_CHANGED:
    case JOB_INITED:
    case JOB_PRIORITY_CHANGED:
    case JOB_QUEUE_CHANGED:
    case JOB_FAILED:
    case JOB_KILLED:
    case JOB_ERROR:
    case JOB_FINISHED:
    case AM_STARTED:
    case NORMALIZED_RESOURCE:
      break;
    // Handle task events
    case TASK_STARTED:
      setCreatedTime = true;
      taskId = ((TaskStartedEvent)event).getTaskId().toString();
      break;
    case TASK_FAILED:
      taskId = ((TaskFailedEvent)event).getTaskId().toString();
      break;
    case TASK_UPDATED:
      taskId = ((TaskUpdatedEvent)event).getTaskId().toString();
      break;
    case TASK_FINISHED:
      taskId = ((TaskFinishedEvent)event).getTaskId().toString();
      break;
    case MAP_ATTEMPT_STARTED:
    case REDUCE_ATTEMPT_STARTED:
      setCreatedTime = true;
      taskId = ((TaskAttemptStartedEvent)event).getTaskId().toString();
      taskAttemptId = ((TaskAttemptStartedEvent)event).
          getTaskAttemptId().toString();
      break;
    case CLEANUP_ATTEMPT_STARTED:
    case SETUP_ATTEMPT_STARTED:
      taskId = ((TaskAttemptStartedEvent)event).getTaskId().toString();
      taskAttemptId = ((TaskAttemptStartedEvent)event).
          getTaskAttemptId().toString();
      break;
    case MAP_ATTEMPT_FAILED:
    case CLEANUP_ATTEMPT_FAILED:
    case REDUCE_ATTEMPT_FAILED:
    case SETUP_ATTEMPT_FAILED:
    case MAP_ATTEMPT_KILLED:
    case CLEANUP_ATTEMPT_KILLED:
    case REDUCE_ATTEMPT_KILLED:
    case SETUP_ATTEMPT_KILLED:
      taskId = ((TaskAttemptUnsuccessfulCompletionEvent)event).
          getTaskId().toString();
      taskAttemptId = ((TaskAttemptUnsuccessfulCompletionEvent)event).
          getTaskAttemptId().toString();
      break;
    case MAP_ATTEMPT_FINISHED:
      taskId = ((MapAttemptFinishedEvent)event).getTaskId().toString();
      taskAttemptId = ((MapAttemptFinishedEvent)event).
          getAttemptId().toString();
      break;
    case REDUCE_ATTEMPT_FINISHED:
      taskId = ((ReduceAttemptFinishedEvent)event).getTaskId().toString();
      taskAttemptId = ((ReduceAttemptFinishedEvent)event).
          getAttemptId().toString();
      break;
    case SETUP_ATTEMPT_FINISHED:
    case CLEANUP_ATTEMPT_FINISHED:
      taskId = ((TaskAttemptFinishedEvent)event).getTaskId().toString();
      taskAttemptId = ((TaskAttemptFinishedEvent)event).
          getAttemptId().toString();
      break;
    default:
      LOG.warn("EventType: " + event.getEventType() + " cannot be recognized" +
          " and handled by timeline service.");
      return;
    }

    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
        appEntityWithJobMetrics = null;
    if (taskId == null) {
      // JobEntity
      tEntity = createJobEntity(event, timestamp, jobId,
          MAPREDUCE_JOB_ENTITY_TYPE, setCreatedTime);
      if (event.getEventType() == EventType.JOB_FINISHED
          && event.getTimelineMetrics() != null) {
        appEntityWithJobMetrics = createAppEntityWithJobMetrics(event, jobId);
      }
    } else {
      if (taskAttemptId == null) {
        // TaskEntity
        tEntity = createTaskEntity(event, timestamp, taskId,
            MAPREDUCE_TASK_ENTITY_TYPE, MAPREDUCE_JOB_ENTITY_TYPE,
            jobId, setCreatedTime);
      } else {
        // TaskAttemptEntity
        tEntity = createTaskAttemptEntity(event, timestamp, taskAttemptId,
            MAPREDUCE_TASK_ATTEMPT_ENTITY_TYPE, MAPREDUCE_TASK_ENTITY_TYPE,
            taskId, setCreatedTime);
      }
    }
    try {
      if (appEntityWithJobMetrics == null) {
        timelineClient.putEntitiesAsync(tEntity);
      } else {
        timelineClient.putEntities(tEntity, appEntityWithJobMetrics);
      }
    } catch (IOException | YarnException e) {
      LOG.error("Failed to process Event " + event.getEventType()
          + " for the job : " + jobId, e);
      return;
    }
    if (event.getEventType() == EventType.JOB_SUBMITTED) {
      // Publish configs after main job submitted event has been posted.
      publishConfigsOnJobSubmittedEvent((JobSubmittedEvent)event, jobId);
    }
  }

  private void setSummarySlotSeconds(JobSummary summary, Counters allCounters) {

    Counter slotMillisMapCounter = allCounters
      .findCounter(JobCounter.SLOTS_MILLIS_MAPS);
    if (slotMillisMapCounter != null) {
      summary.setMapSlotSeconds(slotMillisMapCounter.getValue() / 1000);
    }

    Counter slotMillisReduceCounter = allCounters
      .findCounter(JobCounter.SLOTS_MILLIS_REDUCES);
    if (slotMillisReduceCounter != null) {
      summary.setReduceSlotSeconds(slotMillisReduceCounter.getValue() / 1000);
    }
  }

  protected void closeEventWriter(JobId jobId) throws IOException {
    final MetaInfo mi = fileMap.get(jobId);
    if (mi == null) {
      throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
    }

    if (!mi.isWriterActive()) {
      throw new IOException(
          "Inactive Writer: Likely received multiple JobFinished / " +
          "JobUnsuccessful events for JobId: ["
              + jobId + "]");
    }

    // Close the Writer
    try {
      mi.closeWriter();
    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + jobId);
      throw e;
    }
  }

  protected void processDoneFiles(JobId jobId) throws IOException {

    final MetaInfo mi = fileMap.get(jobId);
    if (mi == null) {
      throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
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
      doneDirFS.setPermission(qualifiedSummaryDoneFile, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_FILE_PERMISSIONS));
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
        int jobNameLimit =
            getConfig().getInt(JHAdminConfig.MR_HS_JOBNAME_LIMIT,
            JHAdminConfig.DEFAULT_MR_HS_JOBNAME_LIMIT);
        String doneJobHistoryFileName =
            getTempFileName(FileNameIndexUtils.getDoneFileName(mi
                .getJobIndexInfo(), jobNameLimit));
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

  private class FlushTimerTask extends TimerTask {
    private MetaInfo metaInfo;
    private IOException ioe = null;
    private volatile boolean shouldRun = true;

    FlushTimerTask(MetaInfo metaInfo) {
      this.metaInfo = metaInfo;
    }

    @Override
    public void run() {
      LOG.debug("In flush timer task");
      synchronized (lock) {
        try {
          if (!metaInfo.isTimerShutDown() && shouldRun)
            metaInfo.flush();
        } catch (IOException e) {
          ioe = e;
        }
      }
    }

    public IOException getException() {
      return ioe;
    }

    public void stop() {
      shouldRun = false;
      this.cancel();
    }
  }

  protected class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;
    JobIndexInfo jobIndexInfo;
    JobSummary jobSummary;
    Timer flushTimer; 
    FlushTimerTask flushTimerTask;
    private boolean isTimerShutDown = false;
    private String forcedJobStateOnShutDown;

    MetaInfo(Path historyFile, Path conf, EventWriter writer, String user,
        String jobName, JobId jobId, String forcedJobStateOnShutDown,
        String queueName) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
      this.jobIndexInfo =
          new JobIndexInfo(-1, -1, user, jobName, jobId, -1, -1, null,
                           queueName);
      this.jobSummary = new JobSummary();
      this.flushTimer = new Timer("FlushTimer", true);
      this.forcedJobStateOnShutDown = forcedJobStateOnShutDown;
    }

    Path getHistoryFile() {
      return historyFile;
    }

    Path getConfFile() {
      return confFile;
    }

    JobIndexInfo getJobIndexInfo() {
      return jobIndexInfo;
    }

    JobSummary getJobSummary() {
      return jobSummary;
    }

    boolean isWriterActive() {
      return writer != null;
    }
    
    boolean isTimerShutDown() {
      return isTimerShutDown;
    }

    String getForcedJobStateOnShutDown() {
      return forcedJobStateOnShutDown;
    }

    @Override
    public String toString() {
      return "Job MetaInfo for "+ jobSummary.getJobId()
             + " history file " + historyFile;
    }

    void closeWriter() throws IOException {
      LOG.debug("Closing Writer");
      synchronized (lock) {
        if (writer != null) {
          writer.close();
        }
        writer = null;
      }
    }

    void writeEvent(HistoryEvent event) throws IOException {
      LOG.debug("Writing event");
      synchronized (lock) {
        if (writer != null) {
          writer.write(event);
          processEventForFlush(event);
          maybeFlush(event);
        }
      }
    }

    void processEventForFlush(HistoryEvent historyEvent) throws IOException {
      if (EnumSet.of(EventType.MAP_ATTEMPT_FINISHED,
          EventType.MAP_ATTEMPT_FAILED, EventType.MAP_ATTEMPT_KILLED,
          EventType.REDUCE_ATTEMPT_FINISHED, EventType.REDUCE_ATTEMPT_FAILED,
          EventType.REDUCE_ATTEMPT_KILLED, EventType.TASK_FINISHED,
          EventType.TASK_FAILED, EventType.JOB_FINISHED, EventType.JOB_FAILED,
          EventType.JOB_KILLED).contains(historyEvent.getEventType())) {
        numUnflushedCompletionEvents++;
        if (!isTimerActive) {
          resetFlushTimer();
          if (!isTimerShutDown) {
            flushTimerTask = new FlushTimerTask(this);
            flushTimer.schedule(flushTimerTask, flushTimeout);
            isTimerActive = true;
          }
        }
      }
    }

    void resetFlushTimer() throws IOException {
      if (flushTimerTask != null) {
        IOException exception = flushTimerTask.getException();
        flushTimerTask.stop();
        if (exception != null) {
          throw exception;
        }
        flushTimerTask = null;
      }
      isTimerActive = false;
    }

    void maybeFlush(HistoryEvent historyEvent) throws IOException {
      if ((eventQueue.size() < minQueueSizeForBatchingFlushes 
          && numUnflushedCompletionEvents > 0)
          || numUnflushedCompletionEvents >= maxUnflushedCompletionEvents 
          || isJobCompletionEvent(historyEvent)) {
        this.flush();
      }
    }

    void flush() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Flushing " + toString());
      }
      synchronized (lock) {
        if (numUnflushedCompletionEvents != 0) { // skipped timer cancel.
          writer.flush();
          numUnflushedCompletionEvents = 0;
          resetFlushTimer();
        }
      }
    }

    void shutDownTimer() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Shutting down timer "+ toString());
      }
      synchronized (lock) {
        isTimerShutDown = true;
        flushTimer.cancel();
        if (flushTimerTask != null && flushTimerTask.getException() != null) {
          throw flushTimerTask.getException();
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
      LOG.info("Copying " + fromPath.toString() + " to " + toPath.toString());
      // TODO temporarily removing the existing dst
      if (doneDirFS.exists(toPath)) {
        doneDirFS.delete(toPath, true);
      }
      boolean copied = FileUtil.copy(stagingDirFS, fromPath, doneDirFS, toPath,
          false, getConfig());

      if (copied)
        LOG.info("Copied to done location: " + toPath);
      else 
        LOG.info("copy failed");
      doneDirFS.setPermission(toPath, new FsPermission(
          JobHistoryUtils.HISTORY_INTERMEDIATE_FILE_PERMISSIONS));
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

  public void setForcejobCompletion(boolean forceJobCompletion) {
    this.forceJobCompletion = forceJobCompletion;
    LOG.info("JobHistoryEventHandler notified that forceJobCompletion is "
      + forceJobCompletion);
  }

  private String createJobStateForJobUnsuccessfulCompletionEvent(
      String forcedJobStateOnShutDown) {
    if (forcedJobStateOnShutDown == null || forcedJobStateOnShutDown
        .isEmpty()) {
      return JobState.KILLED.toString();
    } else if (forcedJobStateOnShutDown.equals(
        JobStateInternal.ERROR.toString()) ||
        forcedJobStateOnShutDown.equals(JobStateInternal.FAILED.toString())) {
      return JobState.FAILED.toString();
    } else if (forcedJobStateOnShutDown.equals(JobStateInternal.SUCCEEDED
        .toString())) {
      return JobState.SUCCEEDED.toString();
    }
    return JobState.KILLED.toString();
  }

  @VisibleForTesting
  boolean getFlushTimerStatus() {
    return isTimerActive;
  }
}
