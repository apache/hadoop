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

package org.apache.hadoop.mapreduce.v2.app2.recover;

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
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.ControlledClock;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventFailRequest;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventKillRequest;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptRemoteStartEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventContainerCompleted;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerTALaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventTAEnded;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorContainerDeAllocateRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerAssignTAEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventLaunched;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventCompleted;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerLaunchRequestEvent;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerState;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerTASucceededEvent;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleanupEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
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
  private final Dispatcher dispatcher;
  private final ControlledClock clock;
  private final AppContext appContext;

  private JobInfo jobInfo = null;
  private final Map<TaskId, TaskInfo> completedTasks =
    new HashMap<TaskId, TaskInfo>();

  private final List<TaskEvent> pendingTaskScheduleEvents =
    new ArrayList<TaskEvent>();
  private Map<ContainerId, ContainerInfo> containerInfo =
      new HashMap<ContainerId, ContainerInfo>();
  private Map<TaskAttemptId, ContainerId> attemptToContainerMap =
      new HashMap<TaskAttemptId, ContainerId>();

  private volatile boolean recoveryMode = false;

  public RecoveryService(AppContext appContext, OutputCommitter committer) {
    super("RecoveringDispatcher");
    this.appContext = appContext;
    this.applicationAttemptId = appContext.getApplicationAttemptId();
    this.committer = committer;
    this.dispatcher = createRecoveryDispatcher();
    this.clock = new ControlledClock(appContext.getClock());
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
    // TODO: parse history file based on startCount
    String jobName = 
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()).toString();
    String jobhistoryDir = JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(getConfig());
    FSDataInputStream in = null;
    Path historyFile = null;
    Path histDirPath = FileContext.getFileContext(getConfig()).makeQualified(
        new Path(jobhistoryDir));
    FileContext fc = FileContext.getFileContext(histDirPath.toUri(),
        getConfig());
    //read the previous history file
    historyFile = fc.makeQualified(JobHistoryUtils.getStagingJobHistoryFile(
        histDirPath, jobName, (applicationAttemptId.getAttemptId() - 1)));  
    LOG.info("History file is at " + historyFile);
    in = fc.open(historyFile);
    JobHistoryParser parser = new JobHistoryParser(in);
    jobInfo = parser.parse();
    Exception parseException = parser.getParseException();
    if (parseException != null) {
      LOG.info("Got an error parsing job-history file " + historyFile + 
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
  
  protected Dispatcher createRecoveryDispatcher() {
    return new RecoveryDispatcher();
  }

  @SuppressWarnings("rawtypes")
  class RecoveryDispatcher extends AsyncDispatcher {
    // Intercept events when they're being drained from the queue - so oreder is considered.
    private final EventHandler actualHandler;
    private final EventHandler handler;

    RecoveryDispatcher() {
      super();
      actualHandler = super.getEventHandler();
      handler = new InterceptingEventHandler(actualHandler);
    }

    
    @Override
    public void dispatch(Event event) {
      if (recoveryMode) {
        if (event.getType() == TaskAttemptEventType.TA_STARTED_REMOTELY) {
          // These events are split between the intercepted handle() call, and
          // just before the dispatch.
          TaskAttemptInfo attInfo = getTaskAttemptInfo(((TaskAttemptEvent) event)
              .getTaskAttemptID());
          LOG.info("Recovered Attempt start time " + attInfo.getStartTime());
          clock.setTime(attInfo.getStartTime());
        } else if (event.getType() == TaskAttemptEventType.TA_DONE
            || event.getType() == TaskAttemptEventType.TA_FAIL_REQUEST
            || event.getType() == TaskAttemptEventType.TA_KILL_REQUEST) {
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
            if (allTasksRecovered()) {
              if (!allContainersStopped()) {
                stopRemainingContainers(actualHandler);
              } else {
                endRecovery(actualHandler);
              }
            }
          }
        } else if (event.getType() == AMSchedulerEventType.S_CONTAINER_COMPLETED) {
          // This is the last event after a container completes. TA_TERMINATED
          // messages to tasks would have gone out, and been processed before
          // this. As a result, TASK_CLEAN generated by TA_TERMINATED would
          // reach the InterceptingEventHandler (and are ignored) before this
          // event type is dispatched.
          // At this point, it's safe to remove this container from the
          // containerInfo map.
          AMSchedulerEventContainerCompleted cEvent = (AMSchedulerEventContainerCompleted)event;
          ContainerId containerId = cEvent.getContainerId();
          LOG.info("In Recovery, Container with id: " + containerId + " completed");
          containerInfo.remove(containerId);

          // Check if recovery is complete.
          if (allTasksRecovered() && allContainersStopped()) {
            endRecovery(actualHandler);
          }
          return; //S_CONTAINER_COMPELTED does not need to reach the scheduler.
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

  protected boolean allContainersStopped() {
    return containerInfo.size() == 0;
  }

  protected boolean allTasksRecovered() {
    return completedTasks.size() == 0;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected void stopRemainingContainers(EventHandler eventHandler) {
    for (ContainerId containerId : containerInfo.keySet()) {
      eventHandler.handle(new AMContainerEvent(containerId,
          AMContainerEventType.C_STOP_REQUEST));
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected void endRecovery(EventHandler eventHandler) {
    recoveryMode = false;
    clock.reset();
    LOG.info("Setting the recovery mode to false. " + "Recovery is complete!");

    // send all pending tasks schedule events
    for (TaskEvent tEv : pendingTaskScheduleEvents) {
      eventHandler.handle(tEv);
    }
  }

  
  @SuppressWarnings({"rawtypes", "unchecked"})
  private class InterceptingEventHandler implements EventHandler {
    //Intercept events before they're put onto the queue.
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

      // Schedule previous finished attempts. Delay new ones till after recovery.
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

      // Intercept TaskAttempt start request.
      else if (event.getType() == AMSchedulerEventType.S_TA_LAUNCH_REQUEST) {
        TaskAttemptId aId = ((AMSchedulerTALaunchRequestEvent) event).getAttemptID();
        TaskAttemptInfo attInfo = getTaskAttemptInfo(aId);
        LOG.debug("TA_LAUNCH_REQUEST " + aId);
        sendAssignedEvent(aId, attInfo, (AMSchedulerTALaunchRequestEvent)event);
        return;
      }

      // Container Launch request. Mock and send back launched.
      else if (event.getType() == NMCommunicatorEventType.CONTAINER_LAUNCH_REQUEST) {
        ContainerId cId = ((NMCommunicatorLaunchRequestEvent) event)
            .getContainerId();
        // Simulate container launch.
        ContainerInfo cInfo = containerInfo.get(cId);
        actualHandler.handle(new AMContainerEventLaunched(cId, cInfo
            .getShufflePort()));

        // Simulate a pull from the TaskAttempt
        actualHandler.handle(new AMContainerEvent(cId,
            AMContainerEventType.C_PULL_TA));

        // Inform the TaskAttempt about the assignment.
        actualHandler.handle(new TaskAttemptRemoteStartEvent(cInfo
            .getNextAttemptId(), cId, null, cInfo.getShufflePort()));
        
        // TaskAttempt doesn't generate any useful event while in RUNNING. Generate events for next state here.
        TaskAttemptId aId = cInfo.getNextAttemptId();
        
        TaskAttemptInfo attInfo = getTaskAttemptInfo(aId);
        // send the status update event
        sendStatusUpdateEvent(aId, attInfo);

        TaskAttemptState state = TaskAttemptState.valueOf(attInfo.getTaskStatus());
        switch (state) {
        case SUCCEEDED:
          //recover the task output
          TaskAttemptContext taskContext = new TaskAttemptContextImpl(getConfig(),
              attInfo.getAttemptId());
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
          // XXX (Post-3902)thh.unregister happens here. Ensure THH handles it
          // correctly in case of recovery. 
          break;
        case KILLED:
          LOG.info("Sending kill event to recovered attempt " + aId);
          actualHandler.handle(new TaskAttemptEventKillRequest(aId, "")); 
          break;
        default:
          LOG.info("Sending fail event to recovered attempt " + aId);
          actualHandler.handle(new TaskAttemptEventFailRequest(aId, ""));
          break;
        }
        return;
      } 
      
      // Handle Events which may be sent to the scheduler.
      else if (event.getType() == AMSchedulerEventType.S_TA_ENDED) {
        // Tell the container to stop.
        AMSchedulerEventTAEnded sEvent = (AMSchedulerEventTAEnded) event;
        ContainerId containerId = attemptToContainerMap.get(sEvent
            .getAttemptID());
        switch (sEvent.getState()) {
        case FAILED: 
        case KILLED:
          actualHandler.handle(new AMContainerEvent(containerId,
              AMContainerEventType.C_STOP_REQUEST));
          return;
          // XXX (Post-3902)chh.unregister happens here. Ensure THH handles it
          // correctly in case of recovery.
        case SUCCEEDED:
          // Inform the container that the task attempt succeeded.
          // Leaving the event in the map - for TA failure after success.
          actualHandler.handle(new AMContainerTASucceededEvent(containerId,
              sEvent.getAttemptID()));
          return;
          // XXX (Post-3902)tal.unregister happens here. Ensure THH handles it
          // correctly in case of recovery.
        default:
            throw new YarnException("Invalid state " + sEvent.getState());
        }
      }
      
      // De-allocate containers used by previous attempts immediately.
      else if (event.getType() == NMCommunicatorEventType.CONTAINER_STOP_REQUEST) {
        // Ignore. Unless we start relying on a successful NM.stopContainer() call.
        NMCommunicatorEvent nEvent = (NMCommunicatorEvent)event;
        ContainerId cId = nEvent.getContainerId();
        ContainerStatus cs = BuilderUtils.newContainerStatus(cId,
            ContainerState.COMPLETE, "", 0);
        actualHandler.handle(new AMContainerEventCompleted(cs));
        return;
      }
      
      // De-allocate containers used by previous attempts immediately.
      else if (event.getType() == RMCommunicatorEventType.CONTAINER_DEALLOCATE) {
        RMCommunicatorContainerDeAllocateRequestEvent dEvent = (RMCommunicatorContainerDeAllocateRequestEvent) event;
        ContainerId cId = dEvent.getContainerId();
        // exitStatus not known, diagnostics not known.
        ContainerStatus cs = BuilderUtils.newContainerStatus(cId,
            ContainerState.COMPLETE, "", 0);
        actualHandler.handle(new AMContainerEventCompleted(cs));
        return;
      }
      
      // Received for FAILED/KILLED tasks after C_COMPLETED.
      else if (event.getType() == TaskCleaner.EventType.TASK_CLEAN) {
        TaskAttemptId aId = ((TaskCleanupEvent) event).getAttemptID();
        LOG.debug("TASK_CLEAN for attemptId: " + aId);
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
        TaskAttemptInfo attemptInfo, AMSchedulerTALaunchRequestEvent event) {
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
      
      // Track shufflePort, attemptId for container - would normally be done by the scheduler.
      ContainerInfo cInfo = containerInfo.get(cId);
      if (cInfo == null) {
        cInfo = new ContainerInfo(attemptInfo.getShufflePort());
        containerInfo.put(cId, cInfo);
      }
      cInfo.setAttemptId(yarnAttemptID);
      attemptToContainerMap.put(yarnAttemptID, cId);
      
      appContext.getAllNodes().nodeSeen(nodeId);
      appContext.getAllContainers().addContainerIfNew(container);
      
      
      // Request container launch for new containers.
      if (appContext.getAllContainers().get(cId).getState() == AMContainerState.ALLOCATED) {
        TaskId taskId = yarnAttemptID.getTaskId();
        AMContainerLaunchRequestEvent lrEvent = new AMContainerLaunchRequestEvent(
            cId, taskId.getJobId(), taskId.getTaskType(), event.getJobToken(),
            event.getCredentials(), false, new JobConf(appContext.getJob(
                taskId.getJobId()).getConf()));
        actualHandler.handle(lrEvent);
      }
      // Assing the task attempt to this container.
      actualHandler.handle(new AMContainerAssignTAEvent(cId, yarnAttemptID,
          event.getRemoteTask()));
    }
    // TODO: Handle container launch request
  }

  private static class ContainerInfo {
    int shufflePort;
    TaskAttemptId nextAttemptId;

    ContainerInfo(int shufflePort) {
      this.shufflePort = shufflePort;
    }

    void setAttemptId(TaskAttemptId attemptId) {
      this.nextAttemptId = attemptId;
    }
    
    int getShufflePort() {
      return shufflePort;
    }
    
    TaskAttemptId getNextAttemptId() {
      return nextAttemptId;
    }
  }

}
