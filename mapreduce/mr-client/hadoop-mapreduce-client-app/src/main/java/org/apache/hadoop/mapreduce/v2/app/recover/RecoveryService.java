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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app.taskclean.TaskCleanupEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;

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
//change job output committer to have 
//    - atomic job output promotion
//    - recover output of completed tasks

public class RecoveryService extends CompositeService implements Recovery {

  private static final Log LOG = LogFactory.getLog(RecoveryService.class);

  private final ApplicationId appID;
  private final Dispatcher dispatcher;
  private final ControlledClock clock;
  private final int startCount;

  private JobInfo jobInfo = null;
  private final Map<TaskId, TaskInfo> completedTasks =
    new HashMap<TaskId, TaskInfo>();

  private final List<TaskEvent> pendingTaskScheduleEvents =
    new ArrayList<TaskEvent>();

  private volatile boolean recoveryMode = false;

  public RecoveryService(ApplicationId appID, Clock clock, int startCount) {
    super("RecoveringDispatcher");
    this.appID = appID;
    this.startCount = startCount;
    this.dispatcher = new RecoveryDispatcher();
    this.clock = new ControlledClock(clock);
    if (dispatcher instanceof Service) {
      addService((Service) dispatcher);
    }
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    // parse the history file
    try {
      parse();
      if (completedTasks.size() > 0) {
        recoveryMode = true;
        LOG.info("SETTING THE RECOVERY MODE TO TRUE. NO OF COMPLETED TASKS " + 
            "TO RECOVER " + completedTasks.size());
        LOG.info("Job launch time " + jobInfo.getLaunchTime());
        clock.setTime(jobInfo.getLaunchTime());
      }
    } catch (IOException e) {
      LOG.warn(e);
      LOG.warn("Could not parse the old history file. Aborting recovery. "
          + "Starting afresh.");
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
  public Set<TaskId> getCompletedTasks() {
    return completedTasks.keySet();
  }

  private void parse() throws IOException {
    // TODO: parse history file based on startCount
    String jobName = TypeConverter.fromYarn(appID).toString();
    String jobhistoryDir = JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(getConfig());
    FSDataInputStream in = null;
    Path historyFile = null;
    Path histDirPath = FileContext.getFileContext(getConfig()).makeQualified(
        new Path(jobhistoryDir));
    FileContext fc = FileContext.getFileContext(histDirPath.toUri(),
        getConfig());
    historyFile = fc.makeQualified(JobHistoryUtils.getStagingJobHistoryFile(
        histDirPath, jobName, startCount - 1));          //read the previous history file
    in = fc.open(historyFile);
    JobHistoryParser parser = new JobHistoryParser(in);
    jobInfo = parser.parse();
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

  class RecoveryDispatcher extends AsyncDispatcher {
    private final EventHandler actualHandler;
    private final EventHandler handler;

    RecoveryDispatcher() {
      actualHandler = super.getEventHandler();
      handler = new InterceptingEventHandler(actualHandler);
    }

    @Override
    public void dispatch(Event event) {
      if (recoveryMode) {
        if (event.getType() == TaskAttemptEventType.TA_CONTAINER_LAUNCHED) {
          TaskAttemptInfo attInfo = getTaskAttemptInfo(((TaskAttemptEvent) event)
              .getTaskAttemptID());
          LOG.info("Attempt start time " + attInfo.getStartTime());
          clock.setTime(attInfo.getStartTime());

        } else if (event.getType() == TaskAttemptEventType.TA_DONE
            || event.getType() == TaskAttemptEventType.TA_FAILMSG
            || event.getType() == TaskAttemptEventType.TA_KILL) {
          TaskAttemptInfo attInfo = getTaskAttemptInfo(((TaskAttemptEvent) event)
              .getTaskAttemptID());
          LOG.info("Attempt finish time " + attInfo.getFinishTime());
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

      else if (event.getType() == TaskCleaner.EventType.TASK_CLEAN) {
        TaskAttemptId aId = ((TaskCleanupEvent) event).getAttemptID();
        LOG.debug("TASK_CLEAN");
        actualHandler.handle(new TaskAttemptEvent(aId,
            TaskAttemptEventType.TA_CLEANUP_DONE));
        return;
      }

      else if (event.getType() == ContainerLauncher.EventType.CONTAINER_REMOTE_LAUNCH) {
        TaskAttemptId aId = ((ContainerRemoteLaunchEvent) event)
            .getTaskAttemptID();
        TaskAttemptInfo attInfo = getTaskAttemptInfo(aId);
        actualHandler.handle(new TaskAttemptEvent(aId,
            TaskAttemptEventType.TA_CONTAINER_LAUNCHED));
        // send the status update event
        sendStatusUpdateEvent(aId, attInfo);

        TaskAttemptState state = TaskAttemptState.valueOf(attInfo.getTaskStatus());
        switch (state) {
        case SUCCEEDED:
          // send the done event
          LOG.info("Sending done event to " + aId);
          actualHandler.handle(new TaskAttemptEvent(aId,
              TaskAttemptEventType.TA_DONE));
          break;
        case KILLED:
          LOG.info("Sending kill event to " + aId);
          actualHandler.handle(new TaskAttemptEvent(aId,
              TaskAttemptEventType.TA_KILL));
          break;
        default:
          LOG.info("Sending fail event to " + aId);
          actualHandler.handle(new TaskAttemptEvent(aId,
              TaskAttemptEventType.TA_FAILMSG));
          break;
        }
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
      taskAttemptStatus.diagnosticInfo = "";
      taskAttemptStatus.stateString = attemptInfo.getTaskStatus(); 
      // taskAttemptStatus.outputSize = attemptInfo.getOutputSize();
      taskAttemptStatus.phase = Phase.CLEANUP;
      org.apache.hadoop.mapreduce.Counters cntrs = attemptInfo.getCounters();
      if (cntrs == null) {
        taskAttemptStatus.counters = null;
      } else {
        taskAttemptStatus.counters = TypeConverter.toYarn(attemptInfo
            .getCounters());
      }
      actualHandler.handle(new TaskAttemptStatusUpdateEvent(
          taskAttemptStatus.id, taskAttemptStatus));
    }

    private void sendAssignedEvent(TaskAttemptId yarnAttemptID,
        TaskAttemptInfo attemptInfo) {
      LOG.info("Sending assigned event to " + yarnAttemptID);
      ContainerId cId = RecordFactoryProvider.getRecordFactory(null)
          .newRecordInstance(ContainerId.class);
      Container container = RecordFactoryProvider.getRecordFactory(null)
          .newRecordInstance(Container.class);
      container.setId(cId);
      container.setContainerManagerAddress("localhost");
      container.setContainerToken(null);
      container.setNodeHttpAddress(attemptInfo.getHostname() + ":" + 
          attemptInfo.getHttpPort());
      actualHandler.handle(new TaskAttemptContainerAssignedEvent(yarnAttemptID,
          container));
    }
  }

}
