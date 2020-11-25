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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSpeculator extends AbstractService implements
    Speculator {

  private static final long ON_SCHEDULE = Long.MIN_VALUE;
  private static final long ALREADY_SPECULATING = Long.MIN_VALUE + 1;
  private static final long TOO_NEW = Long.MIN_VALUE + 2;
  private static final long PROGRESS_IS_GOOD = Long.MIN_VALUE + 3;
  private static final long NOT_RUNNING = Long.MIN_VALUE + 4;
  private static final long TOO_LATE_TO_SPECULATE = Long.MIN_VALUE + 5;

  private long soonestRetryAfterNoSpeculate;
  private long soonestRetryAfterSpeculate;
  private double proportionRunningTasksSpeculatable;
  private double proportionTotalTasksSpeculatable;
  private int  minimumAllowedSpeculativeTasks;

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultSpeculator.class);

  private final ConcurrentMap<TaskId, Boolean> runningTasks
      = new ConcurrentHashMap<TaskId, Boolean>();

  // Used to track any TaskAttempts that aren't heart-beating for a while, so
  // that we can aggressively speculate instead of waiting for task-timeout.
  private final ConcurrentMap<TaskAttemptId, TaskAttemptHistoryStatistics>
      runningTaskAttemptStatistics = new ConcurrentHashMap<TaskAttemptId,
          TaskAttemptHistoryStatistics>();
  // Regular heartbeat from tasks is every 3 secs. So if we don't get a
  // heartbeat in 9 secs (3 heartbeats), we simulate a heartbeat with no change
  // in progress.
  private static final long MAX_WAITTING_TIME_FOR_HEARTBEAT = 9 * 1000;

  // These are the current needs, not the initial needs.  For each job, these
  //  record the number of attempts that exist and that are actively
  //  waiting for a container [as opposed to running or finished]
  private final ConcurrentMap<JobId, AtomicInteger> mapContainerNeeds
      = new ConcurrentHashMap<JobId, AtomicInteger>();
  private final ConcurrentMap<JobId, AtomicInteger> reduceContainerNeeds
      = new ConcurrentHashMap<JobId, AtomicInteger>();

  private final Set<TaskId> mayHaveSpeculated = new HashSet<TaskId>();

  private final Configuration conf;
  private AppContext context;
  private Thread speculationBackgroundThread = null;
  private volatile boolean stopped = false;
  private TaskRuntimeEstimator estimator;

  private BlockingQueue<Object> scanControl = new LinkedBlockingQueue<Object>();

  private final Clock clock;

  private final EventHandler<Event> eventHandler;

  public DefaultSpeculator(Configuration conf, AppContext context) {
    this(conf, context, context.getClock());
  }

  public DefaultSpeculator(Configuration conf, AppContext context, Clock clock) {
    this(conf, context, getEstimator(conf, context), clock);
  }
  
  static private TaskRuntimeEstimator getEstimator
      (Configuration conf, AppContext context) {
    TaskRuntimeEstimator estimator;
    
    try {
      // "yarn.mapreduce.job.task.runtime.estimator.class"
      Class<? extends TaskRuntimeEstimator> estimatorClass
          = conf.getClass(MRJobConfig.MR_AM_TASK_ESTIMATOR,
                          LegacyTaskRuntimeEstimator.class,
                          TaskRuntimeEstimator.class);

      Constructor<? extends TaskRuntimeEstimator> estimatorConstructor
          = estimatorClass.getConstructor();

      estimator = estimatorConstructor.newInstance();

      estimator.contextualize(conf, context);
    } catch (InstantiationException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (IllegalAccessException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (InvocationTargetException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (NoSuchMethodException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    }
    
  return estimator;
  }

  // This constructor is designed to be called by other constructors.
  //  However, it's public because we do use it in the test cases.
  // Normally we figure out our own estimator.
  public DefaultSpeculator
      (Configuration conf, AppContext context,
       TaskRuntimeEstimator estimator, Clock clock) {
    super(DefaultSpeculator.class.getName());

    this.conf = conf;
    this.context = context;
    this.estimator = estimator;
    this.clock = clock;
    this.eventHandler = context.getEventHandler();
    this.soonestRetryAfterNoSpeculate =
        conf.getLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_NO_SPECULATE,
                MRJobConfig.DEFAULT_SPECULATIVE_RETRY_AFTER_NO_SPECULATE);
    this.soonestRetryAfterSpeculate =
        conf.getLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_SPECULATE,
                MRJobConfig.DEFAULT_SPECULATIVE_RETRY_AFTER_SPECULATE);
    this.proportionRunningTasksSpeculatable =
        conf.getDouble(MRJobConfig.SPECULATIVECAP_RUNNING_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVECAP_RUNNING_TASKS);
    this.proportionTotalTasksSpeculatable =
        conf.getDouble(MRJobConfig.SPECULATIVECAP_TOTAL_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVECAP_TOTAL_TASKS);
    this.minimumAllowedSpeculativeTasks =
        conf.getInt(MRJobConfig.SPECULATIVE_MINIMUM_ALLOWED_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS);
  }

/*   *************************************************************    */

  // This is the task-mongering that creates the two new threads -- one for
  //  processing events from the event queue and one for periodically
  //  looking for speculation opportunities

  @Override
  protected void serviceStart() throws Exception {
    Runnable speculationBackgroundCore
        = new Runnable() {
            @Override
            public void run() {
              while (!stopped && !Thread.currentThread().isInterrupted()) {
                long backgroundRunStartTime = clock.getTime();
                try {
                  int speculations = computeSpeculations();
                  long mininumRecomp
                      = speculations > 0 ? soonestRetryAfterSpeculate
                                         : soonestRetryAfterNoSpeculate;

                  long wait = Math.max(mininumRecomp,
                        clock.getTime() - backgroundRunStartTime);

                  if (speculations > 0) {
                    LOG.info("We launched " + speculations
                        + " speculations.  Sleeping " + wait + " milliseconds.");
                  }

                  Object pollResult
                      = scanControl.poll(wait, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  if (!stopped) {
                    LOG.error("Background thread returning, interrupted", e);
                  }
                  return;
                }
              }
            }
          };
    speculationBackgroundThread = new Thread
        (speculationBackgroundCore, "DefaultSpeculator background processing");
    speculationBackgroundThread.start();

    super.serviceStart();
  }

  @Override
  protected void serviceStop()throws Exception {
      stopped = true;
    // this could be called before background thread is established
    if (speculationBackgroundThread != null) {
      speculationBackgroundThread.interrupt();
    }
    super.serviceStop();
  }

  @Override
  public void handleAttempt(TaskAttemptStatus status) {
    long timestamp = clock.getTime();
    statusUpdate(status, timestamp);
  }

  // This section is not part of the Speculator interface; it's used only for
  //  testing
  public boolean eventQueueEmpty() {
    return scanControl.isEmpty();
  }

  // This interface is intended to be used only for test cases.
  public void scanForSpeculations() {
    LOG.info("We got asked to run a debug speculation scan.");
    // debug
    System.out.println("We got asked to run a debug speculation scan.");
    System.out.println("There are " + scanControl.size()
        + " events stacked already.");
    scanControl.add(new Object());
    Thread.yield();
  }


/*   *************************************************************    */

  // This section contains the code that gets run for a SpeculatorEvent

  private AtomicInteger containerNeed(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    TaskType taskType = taskID.getTaskType();

    ConcurrentMap<JobId, AtomicInteger> relevantMap
        = taskType == TaskType.MAP ? mapContainerNeeds : reduceContainerNeeds;

    AtomicInteger result = relevantMap.get(jobID);

    if (result == null) {
      relevantMap.putIfAbsent(jobID, new AtomicInteger(0));
      result = relevantMap.get(jobID);
    }

    return result;
  }

  private synchronized void processSpeculatorEvent(SpeculatorEvent event) {
    switch (event.getType()) {
      case ATTEMPT_STATUS_UPDATE:
        statusUpdate(event.getReportedStatus(), event.getTimestamp());
        break;

      case TASK_CONTAINER_NEED_UPDATE:
      {
        AtomicInteger need = containerNeed(event.getTaskID());
        need.addAndGet(event.containersNeededChange());
        break;
      }

      case ATTEMPT_START:
      {
        LOG.info("ATTEMPT_START " + event.getTaskID());
        estimator.enrollAttempt
            (event.getReportedStatus(), event.getTimestamp());
        break;
      }
      
      case JOB_CREATE:
      {
        LOG.info("JOB_CREATE " + event.getJobID());
        estimator.contextualize(getConfig(), context);
        break;
      }
    }
  }

  /**
   * Absorbs one TaskAttemptStatus
   *
   * @param reportedStatus the status report that we got from a task attempt
   *        that we want to fold into the speculation data for this job
   * @param timestamp the time this status corresponds to.  This matters
   *        because statuses contain progress.
   */
  protected void statusUpdate(TaskAttemptStatus reportedStatus, long timestamp) {

    String stateString = reportedStatus.taskState.toString();

    TaskAttemptId attemptID = reportedStatus.id;
    TaskId taskID = attemptID.getTaskId();
    Job job = context.getJob(taskID.getJobId());

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }

    estimator.updateAttempt(reportedStatus, timestamp);

    if (stateString.equals(TaskAttemptState.RUNNING.name())) {
      runningTasks.putIfAbsent(taskID, Boolean.TRUE);
    } else {
      runningTasks.remove(taskID, Boolean.TRUE);
      if (!stateString.equals(TaskAttemptState.STARTING.name())) {
        runningTaskAttemptStatistics.remove(attemptID);
      }
    }
  }

/*   *************************************************************    */

// This is the code section that runs periodically and adds speculations for
//  those jobs that need them.


  // This can return a few magic values for tasks that shouldn't speculate:
  //  returns ON_SCHEDULE if thresholdRuntime(taskID) says that we should not
  //     considering speculating this task
  //  returns ALREADY_SPECULATING if that is true.  This has priority.
  //  returns TOO_NEW if our companion task hasn't gotten any information
  //  returns PROGRESS_IS_GOOD if the task is sailing through
  //  returns NOT_RUNNING if the task is not running
  //
  // All of these values are negative.  Any value that should be allowed to
  //  speculate is 0 or positive.
  private long speculationValue(TaskId taskID, long now) {
    Job job = context.getJob(taskID.getJobId());
    Task task = job.getTask(taskID);
    Map<TaskAttemptId, TaskAttempt> attempts = task.getAttempts();
    long acceptableRuntime = Long.MIN_VALUE;
    long result = Long.MIN_VALUE;

    if (!mayHaveSpeculated.contains(taskID)) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    TaskAttemptId runningTaskAttemptID = null;

    int numberRunningAttempts = 0;

    for (TaskAttempt taskAttempt : attempts.values()) {
      if (taskAttempt.getState() == TaskAttemptState.RUNNING
          || taskAttempt.getState() == TaskAttemptState.STARTING) {
        if (++numberRunningAttempts > 1) {
          return ALREADY_SPECULATING;
        }
        runningTaskAttemptID = taskAttempt.getID();

        long estimatedRunTime = estimator.estimatedRuntime(runningTaskAttemptID);

        long taskAttemptStartTime
            = estimator.attemptEnrolledTime(runningTaskAttemptID);
        if (taskAttemptStartTime > now) {
          // This background process ran before we could process the task
          //  attempt status change that chronicles the attempt start
          return TOO_NEW;
        }

        long estimatedEndTime = estimatedRunTime + taskAttemptStartTime;

        long estimatedReplacementEndTime
            = now + estimator.estimatedNewAttemptRuntime(taskID);

        float progress = taskAttempt.getProgress();
        TaskAttemptHistoryStatistics data =
            runningTaskAttemptStatistics.get(runningTaskAttemptID);
        if (data == null) {
          runningTaskAttemptStatistics.put(runningTaskAttemptID,
            new TaskAttemptHistoryStatistics(estimatedRunTime, progress, now));
        } else {
          if (estimatedRunTime == data.getEstimatedRunTime()
              && progress == data.getProgress()) {
            // Previous stats are same as same stats
            if (data.notHeartbeatedInAWhile(now)
                || estimator.hasStagnatedProgress(runningTaskAttemptID, now)) {
              // Stats have stagnated for a while, simulate heart-beat.
              TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
              taskAttemptStatus.id = runningTaskAttemptID;
              taskAttemptStatus.progress = progress;
              taskAttemptStatus.taskState = taskAttempt.getState();
              // Now simulate the heart-beat
              handleAttempt(taskAttemptStatus);
            }
          } else {
            // Stats have changed - update our data structure
            data.setEstimatedRunTime(estimatedRunTime);
            data.setProgress(progress);
            data.resetHeartBeatTime(now);
          }
        }

        if (estimatedEndTime < now) {
          return PROGRESS_IS_GOOD;
        }

        if (estimatedReplacementEndTime >= estimatedEndTime) {
          return TOO_LATE_TO_SPECULATE;
        }

        result = estimatedEndTime - estimatedReplacementEndTime;
      }
    }

    // If we are here, there's at most one task attempt.
    if (numberRunningAttempts == 0) {
      return NOT_RUNNING;
    }



    if (acceptableRuntime == Long.MIN_VALUE) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    return result;
  }

  //Add attempt to a given Task.
  protected void addSpeculativeAttempt(TaskId taskID) {
    LOG.info
        ("DefaultSpeculator.addSpeculativeAttempt -- we are speculating " + taskID);
    eventHandler.handle(new TaskEvent(taskID, TaskEventType.T_ADD_SPEC_ATTEMPT));
    mayHaveSpeculated.add(taskID);
  }

  @Override
  public void handle(SpeculatorEvent event) {
    processSpeculatorEvent(event);
  }


  private int maybeScheduleAMapSpeculation() {
    return maybeScheduleASpeculation(TaskType.MAP);
  }

  private int maybeScheduleAReduceSpeculation() {
    return maybeScheduleASpeculation(TaskType.REDUCE);
  }

  private int maybeScheduleASpeculation(TaskType type) {
    int successes = 0;

    long now = clock.getTime();

    ConcurrentMap<JobId, AtomicInteger> containerNeeds
        = type == TaskType.MAP ? mapContainerNeeds : reduceContainerNeeds;

    for (ConcurrentMap.Entry<JobId, AtomicInteger> jobEntry : containerNeeds.entrySet()) {
      // This race conditon is okay.  If we skip a speculation attempt we
      //  should have tried because the event that lowers the number of
      //  containers needed to zero hasn't come through, it will next time.
      // Also, if we miss the fact that the number of containers needed was
      //  zero but increased due to a failure it's not too bad to launch one
      //  container prematurely.
      if (jobEntry.getValue().get() > 0) {
        continue;
      }

      int numberSpeculationsAlready = 0;
      int numberRunningTasks = 0;

      // loop through the tasks of the kind
      Job job = context.getJob(jobEntry.getKey());

      Map<TaskId, Task> tasks = job.getTasks(type);

      int numberAllowedSpeculativeTasks
          = (int) Math.max(minimumAllowedSpeculativeTasks,
              proportionTotalTasksSpeculatable * tasks.size());

      TaskId bestTaskID = null;
      long bestSpeculationValue = -1L;

      // this loop is potentially pricey.
      // TODO track the tasks that are potentially worth looking at
      for (Map.Entry<TaskId, Task> taskEntry : tasks.entrySet()) {
        long mySpeculationValue = speculationValue(taskEntry.getKey(), now);

        if (mySpeculationValue == ALREADY_SPECULATING) {
          ++numberSpeculationsAlready;
        }

        if (mySpeculationValue != NOT_RUNNING) {
          ++numberRunningTasks;
        }

        if (mySpeculationValue > bestSpeculationValue) {
          bestTaskID = taskEntry.getKey();
          bestSpeculationValue = mySpeculationValue;
        }
      }
      numberAllowedSpeculativeTasks
          = (int) Math.max(numberAllowedSpeculativeTasks,
              proportionRunningTasksSpeculatable * numberRunningTasks);

      // If we found a speculation target, fire it off
      if (bestTaskID != null
          && numberAllowedSpeculativeTasks > numberSpeculationsAlready) {
        addSpeculativeAttempt(bestTaskID);
        ++successes;
      }
    }

    return successes;
  }

  private int computeSpeculations() {
    // We'll try to issue one map and one reduce speculation per job per run
    return maybeScheduleAMapSpeculation() + maybeScheduleAReduceSpeculation();
  }

  static class TaskAttemptHistoryStatistics {

    private long estimatedRunTime;
    private float progress;
    private long lastHeartBeatTime;

    public TaskAttemptHistoryStatistics(long estimatedRunTime, float progress,
        long nonProgressStartTime) {
      this.estimatedRunTime = estimatedRunTime;
      this.progress = progress;
      resetHeartBeatTime(nonProgressStartTime);
    }

    public long getEstimatedRunTime() {
      return this.estimatedRunTime;
    }

    public float getProgress() {
      return this.progress;
    }

    public void setEstimatedRunTime(long estimatedRunTime) {
      this.estimatedRunTime = estimatedRunTime;
    }

    public void setProgress(float progress) {
      this.progress = progress;
    }

    public boolean notHeartbeatedInAWhile(long now) {
      if (now - lastHeartBeatTime <= MAX_WAITTING_TIME_FOR_HEARTBEAT) {
        return false;
      } else {
        resetHeartBeatTime(now);
        return true;
      }
    }

    public void resetHeartBeatTime(long lastHeartBeatTime) {
      this.lastHeartBeatTime = lastHeartBeatTime;
    }
  }

  @VisibleForTesting
  public long getSoonestRetryAfterNoSpeculate() {
    return soonestRetryAfterNoSpeculate;
  }

  @VisibleForTesting
  public long getSoonestRetryAfterSpeculate() {
    return soonestRetryAfterSpeculate;
  }

  @VisibleForTesting
  public double getProportionRunningTasksSpeculatable() {
    return proportionRunningTasksSpeculatable;
  }

  @VisibleForTesting
  public double getProportionTotalTasksSpeculatable() {
    return proportionTotalTasksSpeculatable;
  }

  @VisibleForTesting
  public int getMinimumAllowedSpeculativeTasks() {
    return minimumAllowedSpeculativeTasks;
  }
}
