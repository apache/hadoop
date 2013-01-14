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

package org.apache.hadoop.mapreduce.v2.app.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.ControlledClock;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterTaskAbortEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

/*
 * Recovers the completed tasks from the previous life of Application Master.
 * The completed tasks are deciphered from the history file of the previous life.
 * Recovery service intercepts and replay the events for completed tasks.
 * While recovery is in progress, the scheduling of new tasks are delayed by 
 * buffering the task schedule events.
 * The recovery service controls the clock while recovery is in progress.
 */

//TODO:
//task cleanup for all non completed tasks
public class RecoveryService extends CompositeService implements Recovery {

  private static final Log LOG = LogFactory.getLog(RecoveryService.class);

  private final ApplicationAttemptId applicationAttemptId;
  private final OutputCommitter committer;
  private final boolean newApiCommitter;
  private final Dispatcher dispatcher;
  private final ControlledClock clock;

  private JobInfo jobInfo = null;
  private final Map<TaskId, TaskInfo> completedTasks =
    new HashMap<TaskId, TaskInfo>();

  private final List<TaskEvent> pendingTaskScheduleEvents =
    new ArrayList<TaskEvent>();

  private volatile boolean recoveryMode = false;

  public RecoveryService(ApplicationAttemptId applicationAttemptId, 
      Clock clock, OutputCommitter committer, boolean newApiCommitter) {
    super("RecoveringDispatcher");
    this.applicationAttemptId = applicationAttemptId;
    this.committer = committer;
    this.newApiCommitter = newApiCommitter;
    this.dispatcher = createRecoveryDispatcher();
    this.clock = new ControlledClock(clock);
      addService((Service) dispatcher);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    // parse the history file
    try {
      parse();
    } catch (Exception e) {
      LOG.warn(e);
      LOG.warn("Could not parse the old history file. Aborting recovery. "
          + "Starting afresh.", e);
    }
    if (completedTasks.size() > 0) {
      recoveryMode = true;
      LOG.info("SETTING THE RECOVERY MODE TO TRUE. NO OF COMPLETED TASKS "
          + "TO RECOVER " + completedTasks.size());
      LOG.info("Job launch time " + jobInfo.getLaunchTime());
      clock.setTime(jobInfo.getLaunchTime());
    }
  }

  @Override
  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  @Override
  public Clock getClock() {
    return clock;
  }

  @Override
  public Map<TaskId, TaskInfo> getCompletedTasks() {
    return completedTasks;
  }

  @Override
  public List<AMInfo> getAMInfos() {
    if (jobInfo == null || jobInfo.getAMInfos() == null) {
      return new LinkedList<AMInfo>();
    }
    List<AMInfo> amInfos = new LinkedList<AMInfo>();
    for (org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.AMInfo jhAmInfo : jobInfo
        .getAMInfos()) {
      AMInfo amInfo =
          MRBuilderUtils.newAMInfo(jhAmInfo.getAppAttemptId(),
              jhAmInfo.getStartTime(), jhAmInfo.getContainerId(),
              jhAmInfo.getNodeManagerHost(), jhAmInfo.getNodeManagerPort(),
              jhAmInfo.getNodeManagerHttpPort());

      amInfos.add(amInfo);
    }
    return amInfos;
  }

  private void parse() throws IOException {
    FSDataInputStream in =
        getPreviousJobHistoryFileStream(getConfig(), applicationAttemptId);
    JobHistoryParser parser = new JobHistoryParser(in);
    jobInfo = parser.parse();
    Exception parseException = parser.getParseException();
    if (parseException != null) {
      LOG.info("Got an error parsing job-history file" + 
          ", ignoring incomplete events.", parseException);
    }
    Map<org.apache.hadoop.mapreduce.TaskID, TaskInfo> taskInfos = jobInfo
        .getAllTasks();
    for (TaskInfo taskInfo : taskInfos.values()) {
      if (TaskState.SUCCEEDED.toString().equals(taskInfo.getTaskStatus())) {
        completedTasks
            .put(TypeConverter.toYarn(taskInfo.getTaskId()), taskInfo);
        LOG.info("Read from history task "
            + TypeConverter.toYarn(taskInfo.getTaskId()));
      }
    }
    LOG.info("Read completed tasks from history "
        + completedTasks.size());
  }

  public static FSDataInputStream getPreviousJobHistoryFileStream(
      Configuration conf, ApplicationAttemptId applicationAttemptId)
      throws IOException {
    FSDataInputStream in = null;
    Path historyFile = null;
    String jobId =
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId())
          .toString();
    String jobhistoryDir =
        JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf, jobId);
    Path histDirPath =
        FileContext.getFileContext(conf).makeQualified(new Path(jobhistoryDir));
    FileContext fc = FileContext.getFileContext(histDirPath.toUri(), conf);
    // read the previous history file
    historyFile =
        fc.makeQualified(JobHistoryUtils.getStagingJobHistoryFile(histDirPath,
          jobId, (applicationAttemptId.getAttemptId() - 1)));
    LOG.info("History file is at " + historyFile);
    in = fc.open(historyFile);
    return in;
  }
  
  protected Dispatcher createRecoveryDispatcher() {
    return new RecoveryDispatcher();
  }

  @SuppressWarnings("rawtypes")
  class RecoveryDispatcher extends AsyncDispatcher {
    private final EventHandler actualHandler;
    private final EventHandler handler;

    RecoveryDispatcher() {
      super();
      actualHandler = super.getEventHandler();
      handler = new InterceptingEventHandler(actualHandler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void dispatch(Event event) {
      if (recoveryMode) {
        if (event.getType() == TaskAttemptEventType.TA_CONTAINER_LAUNCHED) {
          TaskAttemptInfo attInfo = getTaskAttemptInfo(((TaskAttemptEvent) event)
              .getTaskAttemptID());
          LOG.info("Recovered Attempt start time " + attInfo.getStartTime());
          clock.setTime(attInfo.getStartTime());

        } else if (event.getType() == TaskAttemptEventType.TA_DONE
            || event.getType() == TaskAttemptEventType.TA_FAILMSG
            || event.getType() == TaskAttemptEventType.TA_KILL) {
          TaskAttemptInfo attInfo = getTaskAttemptInfo(((TaskAttemptEvent) event)
              .getTaskAttemptID());
          LOG.info("Recovered Attempt finish time " + attInfo.getFinishTime());
          clock.setTime(attInfo.getFinishTime());
        }

        else if (event.getType() == TaskEventType.T_ATTEMPT_FAILED
            || event.getType() == TaskEventType.T_ATTEMPT_KILLED
            || event.getType() == TaskEventType.T_ATTEMPT_SUCCEEDED) {
          TaskTAttemptEvent tEvent = (TaskTAttemptEvent) event;
          LOG.info("Recovered Task attempt " + tEvent.getTaskAttemptID());
          TaskInfo taskInfo = completedTasks.get(tEvent.getTaskAttemptID()
              .getTaskId());
          taskInfo.getAllTaskAttempts().remove(
              TypeConverter.fromYarn(tEvent.getTaskAttemptID()));
          // remove the task info from completed tasks if all attempts are
          // recovered
          if (taskInfo.getAllTaskAttempts().size() == 0) {
            completedTasks.remove(tEvent.getTaskAttemptID().getTaskId());
            // checkForRecoveryComplete
            LOG.info("CompletedTasks() " + completedTasks.size());
            if (completedTasks.size() == 0) {
              recoveryMode = false;
              clock.reset();
              LOG.info("Setting the recovery mode to false. " +
                 "Recovery is complete!");

              // send all pending tasks schedule events
              for (TaskEvent tEv : pendingTaskScheduleEvents) {
                actualHandler.handle(tEv);
              }

            }
          }
        }
      }
      realDispatch(event);
    }
    
    public void realDispatch(Event event) {
      super.dispatch(event);
    }

    @Override
    public EventHandler getEventHandler() {
      return handler;
    }
  }

  private TaskAttemptInfo getTaskAttemptInfo(TaskAttemptId id) {
    TaskInfo taskInfo = completedTasks.get(id.getTaskId());
    return taskInfo.getAllTaskAttempts().get(TypeConverter.fromYarn(id));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private class InterceptingEventHandler implements EventHandler {
    EventHandler actualHandler;

    InterceptingEventHandler(EventHandler actualHandler) {
      this.actualHandler = actualHandler;
    }

    @Override
    public void handle(Event event) {
      if (!recoveryMode) {
        // delegate to the dispatcher one
        actualHandler.handle(event);
        return;
      }

      else if (event.getType() == TaskEventType.T_SCHEDULE) {
        TaskEvent taskEvent = (TaskEvent) event;
        // delay the scheduling of new tasks till previous ones are recovered
        if (completedTasks.get(taskEvent.getTaskID()) == null) {
          LOG.debug("Adding to pending task events "
              + taskEvent.getTaskID());
          pendingTaskScheduleEvents.add(taskEvent);
          return;
        }
      }

      else if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
        TaskAttemptId aId = ((ContainerAllocatorEvent) event).getAttemptID();
        TaskAttemptInfo attInfo = getTaskAttemptInfo(aId);
        LOG.debug("CONTAINER_REQ " + aId);
        sendAssignedEvent(aId, attInfo);
        return;
      }

      else if (event.getType() == CommitterEventType.TASK_ABORT) {
        TaskAttemptId aId = ((CommitterTaskAbortEvent) event).getAttemptID();
        LOG.debug("TASK_CLEAN");
        actualHandler.handle(new TaskAttemptEvent(aId,
            TaskAttemptEventType.TA_CLEANUP_DONE));
        return;
      }

      else if (event.getType() == ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH) {
        TaskAttemptId aId = ((ContainerRemoteLaunchEvent) event)
            .getTaskAttemptID();
        TaskAttemptInfo attInfo = getTaskAttemptInfo(aId);
        actualHandler.handle(new TaskAttemptContainerLaunchedEvent(aId,
            attInfo.getShufflePort()));
        // send the status update event
        sendStatusUpdateEvent(aId, attInfo);

        TaskAttemptState state = TaskAttemptState.valueOf(attInfo.getTaskStatus());
        switch (state) {
        case SUCCEEDED:
          //recover the task output
          
          // check the committer type and construct corresponding context
          TaskAttemptContext taskContext = null;
          if(newApiCommitter) {
            taskContext = new TaskAttemptContextImpl(getConfig(),
                attInfo.getAttemptId());
          } else {
            taskContext = new org.apache.hadoop.mapred.TaskAttemptContextImpl(new JobConf(getConfig()),
                TypeConverter.fromYarn(aId));
          }
          
          try { 
            TaskType type = taskContext.getTaskAttemptID().getTaskID().getTaskType();
            int numReducers = taskContext.getConfiguration().getInt(MRJobConfig.NUM_REDUCES, 1); 
            if(type == TaskType.REDUCE || (type == TaskType.MAP && numReducers <= 0)) {
              committer.recoverTask(taskContext);
              LOG.info("Recovered output from task attempt " + attInfo.getAttemptId());
            } else {
              LOG.info("Will not try to recover output for "
                  + taskContext.getTaskAttemptID());
            }
          } catch (IOException e) {
            LOG.error("Caught an exception while trying to recover task "+aId, e);
            actualHandler.handle(new JobDiagnosticsUpdateEvent(
                aId.getTaskId().getJobId(), "Error in recovering task output " + 
                e.getMessage()));
            actualHandler.handle(new JobEvent(aId.getTaskId().getJobId(),
                JobEventType.INTERNAL_ERROR));
          }
          
          // send the done event
          LOG.info("Sending done event to recovered attempt " + aId);
          actualHandler.handle(new TaskAttemptEvent(aId,
              TaskAttemptEventType.TA_DONE));
          break;
        case KILLED:
          LOG.info("Sending kill event to recovered attempt " + aId);
          actualHandler.handle(new TaskAttemptEvent(aId,
              TaskAttemptEventType.TA_KILL));
          break;
        default:
          LOG.info("Sending fail event to recovered attempt " + aId);
          actualHandler.handle(new TaskAttemptEvent(aId,
              TaskAttemptEventType.TA_FAILMSG));
          break;
        }
        return;
      }

      else if (event.getType() == 
        ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP) {
        TaskAttemptId aId = ((ContainerLauncherEvent) event)
          .getTaskAttemptID();
        actualHandler.handle(
           new TaskAttemptEvent(aId,
                TaskAttemptEventType.TA_CONTAINER_CLEANED));
        return;
      }

      // delegate to the actual handler
      actualHandler.handle(event);
    }

    private void sendStatusUpdateEvent(TaskAttemptId yarnAttemptID,
        TaskAttemptInfo attemptInfo) {
      LOG.info("Sending status update event to " + yarnAttemptID);
      TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
      taskAttemptStatus.id = yarnAttemptID;
      taskAttemptStatus.progress = 1.0f;
      taskAttemptStatus.stateString = attemptInfo.getTaskStatus(); 
      // taskAttemptStatus.outputSize = attemptInfo.getOutputSize();
      taskAttemptStatus.phase = Phase.CLEANUP;
      org.apache.hadoop.mapreduce.Counters cntrs = attemptInfo.getCounters();
      if (cntrs == null) {
        taskAttemptStatus.counters = null;
      } else {
        taskAttemptStatus.counters = cntrs;
      }
      actualHandler.handle(new TaskAttemptStatusUpdateEvent(
          taskAttemptStatus.id, taskAttemptStatus));
    }

    private void sendAssignedEvent(TaskAttemptId yarnAttemptID,
        TaskAttemptInfo attemptInfo) {
      LOG.info("Sending assigned event to " + yarnAttemptID);
      ContainerId cId = attemptInfo.getContainerId();

      NodeId nodeId =
          ConverterUtils.toNodeId(attemptInfo.getHostname() + ":"
              + attemptInfo.getPort());
      // Resource/Priority/ApplicationACLs are only needed while launching the
      // container on an NM, these are already completed tasks, so setting them
      // to null
      Container container = BuilderUtils.newContainer(cId, nodeId,
          attemptInfo.getTrackerName() + ":" + attemptInfo.getHttpPort(),
          null, null, null);
      actualHandler.handle(new TaskAttemptContainerAssignedEvent(yarnAttemptID,
          container, null));
    }
  }

}
