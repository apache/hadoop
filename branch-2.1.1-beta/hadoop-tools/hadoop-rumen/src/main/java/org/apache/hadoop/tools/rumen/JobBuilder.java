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
package org.apache.hadoop.tools.rumen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.AMStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobFinished;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInfoChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobPriorityChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobStatusChangedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinished;
import org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinished;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinished;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletion;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailed;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinished;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskUpdatedEvent;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;
import org.apache.hadoop.util.StringUtils;

/**
 * {@link JobBuilder} builds one job. It processes a sequence of
 * {@link HistoryEvent}s.
 */
public class JobBuilder {
  private static final long BYTES_IN_MEG =
      StringUtils.TraditionalBinaryPrefix.string2long("1m");

  private String jobID;

  private boolean finalized = false;

  private ParsedJob result = new ParsedJob();

  private Map<String, ParsedTask> mapTasks = new HashMap<String, ParsedTask>();
  private Map<String, ParsedTask> reduceTasks =
      new HashMap<String, ParsedTask>();
  private Map<String, ParsedTask> otherTasks =
      new HashMap<String, ParsedTask>();

  private Map<String, ParsedTaskAttempt> attempts =
      new HashMap<String, ParsedTaskAttempt>();

  private Map<ParsedHost, ParsedHost> allHosts =
      new HashMap<ParsedHost, ParsedHost>();

  private org.apache.hadoop.mapreduce.jobhistory.JhCounters EMPTY_COUNTERS =
      new org.apache.hadoop.mapreduce.jobhistory.JhCounters();

  /**
   * The number of splits a task can have, before we ignore them all.
   */
  private final static int MAXIMUM_PREFERRED_LOCATIONS = 25;

  private int[] attemptTimesPercentiles = null;

  // Use this to search within the java options to get heap sizes.
  // The heap size number is in Capturing Group 1.
  // The heap size order-of-magnitude suffix is in Capturing Group 2
  private static final Pattern heapPattern =
      Pattern.compile("-Xmx([0-9]+[kKmMgGtT])");

  private Properties jobConfigurationParameters = null;

  public JobBuilder(String jobID) {
    this.jobID = jobID;
  }

  public String getJobID() {
    return jobID;
  }

  {
    if (attemptTimesPercentiles == null) {
      attemptTimesPercentiles = new int[19];

      for (int i = 0; i < 19; ++i) {
        attemptTimesPercentiles[i] = (i + 1) * 5;
      }
    }
  }

  /**
   * Process one {@link HistoryEvent}
   * 
   * @param event
   *          The {@link HistoryEvent} to be processed.
   */
  public void process(HistoryEvent event) {
    if (finalized) {
      throw new IllegalStateException(
          "JobBuilder.process(HistoryEvent event) called after ParsedJob built");
    }

    // these are in lexicographical order by class name.
    if (event instanceof AMStartedEvent) {
      // ignore this event as Rumen currently doesnt need this event
      //TODO Enhance Rumen to process this event and capture restarts
      return;
    } else if (event instanceof JobFinishedEvent) {
      processJobFinishedEvent((JobFinishedEvent) event);
    } else if (event instanceof JobInfoChangeEvent) {
      processJobInfoChangeEvent((JobInfoChangeEvent) event);
    } else if (event instanceof JobInitedEvent) {
      processJobInitedEvent((JobInitedEvent) event);
    } else if (event instanceof JobPriorityChangeEvent) {
      processJobPriorityChangeEvent((JobPriorityChangeEvent) event);
    } else if (event instanceof JobStatusChangedEvent) {
      processJobStatusChangedEvent((JobStatusChangedEvent) event);
    } else if (event instanceof JobSubmittedEvent) {
      processJobSubmittedEvent((JobSubmittedEvent) event);
    } else if (event instanceof JobUnsuccessfulCompletionEvent) {
      processJobUnsuccessfulCompletionEvent((JobUnsuccessfulCompletionEvent) event);
    } else if (event instanceof MapAttemptFinishedEvent) {
      processMapAttemptFinishedEvent((MapAttemptFinishedEvent) event);
    } else if (event instanceof ReduceAttemptFinishedEvent) {
      processReduceAttemptFinishedEvent((ReduceAttemptFinishedEvent) event);
    } else if (event instanceof TaskAttemptFinishedEvent) {
      processTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) event);
    } else if (event instanceof TaskAttemptStartedEvent) {
      processTaskAttemptStartedEvent((TaskAttemptStartedEvent) event);
    } else if (event instanceof TaskAttemptUnsuccessfulCompletionEvent) {
      processTaskAttemptUnsuccessfulCompletionEvent((TaskAttemptUnsuccessfulCompletionEvent) event);
    } else if (event instanceof TaskFailedEvent) {
      processTaskFailedEvent((TaskFailedEvent) event);
    } else if (event instanceof TaskFinishedEvent) {
      processTaskFinishedEvent((TaskFinishedEvent) event);
    } else if (event instanceof TaskStartedEvent) {
      processTaskStartedEvent((TaskStartedEvent) event);
    } else if (event instanceof TaskUpdatedEvent) {
      processTaskUpdatedEvent((TaskUpdatedEvent) event);
    } else
      throw new IllegalArgumentException(
          "JobBuilder.process(HistoryEvent): unknown event type");
  }

  static String extract(Properties conf, String[] names, String defaultValue) {
    for (String name : names) {
      String result = conf.getProperty(name);

      if (result != null) {
        return result;
      }
    }

    return defaultValue;
  }

  private Integer extractMegabytes(Properties conf, String[] names) {
    String javaOptions = extract(conf, names, null);

    if (javaOptions == null) {
      return null;
    }

    Matcher matcher = heapPattern.matcher(javaOptions);

    Integer heapMegabytes = null;

    while (matcher.find()) {
      String heapSize = matcher.group(1);
      heapMegabytes =
          ((int) (StringUtils.TraditionalBinaryPrefix.string2long(heapSize) / BYTES_IN_MEG));
    }

    return heapMegabytes;
  }

  private void maybeSetHeapMegabytes(Integer megabytes) {
    if (megabytes != null) {
      result.setHeapMegabytes(megabytes);
    }
  }

  private void maybeSetJobMapMB(Integer megabytes) {
    if (megabytes != null) {
      result.setJobMapMB(megabytes);
    }
  }

  private void maybeSetJobReduceMB(Integer megabytes) {
    if (megabytes != null) {
      result.setJobReduceMB(megabytes);
    }
  }

  /**
   * Process a collection of JobConf {@link Properties}. We do not restrict it
   * to be called once. It is okay to process a conf before, during or after the
   * events.
   * 
   * @param conf
   *          The job conf properties to be added.
   */
  public void process(Properties conf) {
    if (finalized) {
      throw new IllegalStateException(
          "JobBuilder.process(Properties conf) called after ParsedJob built");
    }

    //TODO remove this once the deprecate APIs in LoggedJob are removed
    String queue = extract(conf, JobConfPropertyNames.QUEUE_NAMES
                           .getCandidates(), null);
    // set the queue name if existing
    if (queue != null) {
      result.setQueue(queue);
    }
    result.setJobName(extract(conf, JobConfPropertyNames.JOB_NAMES
        .getCandidates(), null));

    maybeSetHeapMegabytes(extractMegabytes(conf,
        JobConfPropertyNames.TASK_JAVA_OPTS_S.getCandidates()));
    maybeSetJobMapMB(extractMegabytes(conf,
        JobConfPropertyNames.MAP_JAVA_OPTS_S.getCandidates()));
    maybeSetJobReduceMB(extractMegabytes(conf,
        JobConfPropertyNames.REDUCE_JAVA_OPTS_S.getCandidates()));
        
    this.jobConfigurationParameters = conf;
  }

  /**
   * Request the builder to build the final object. Once called, the
   * {@link JobBuilder} would accept no more events or job-conf properties.
   * 
   * @return Parsed {@link ParsedJob} object.
   */
  public ParsedJob build() {
    // The main job here is to build CDFs and manage the conf
    finalized = true;

    // set the conf
    if (jobConfigurationParameters != null) {
      result.setJobProperties(jobConfigurationParameters);
    }
    
    // initialize all the per-job statistics gathering places
    Histogram[] successfulMapAttemptTimes =
        new Histogram[ParsedHost.numberOfDistances() + 1];
    for (int i = 0; i < successfulMapAttemptTimes.length; ++i) {
      successfulMapAttemptTimes[i] = new Histogram();
    }

    Histogram successfulReduceAttemptTimes = new Histogram();
    Histogram[] failedMapAttemptTimes =
        new Histogram[ParsedHost.numberOfDistances() + 1];
    for (int i = 0; i < failedMapAttemptTimes.length; ++i) {
      failedMapAttemptTimes[i] = new Histogram();
    }
    Histogram failedReduceAttemptTimes = new Histogram();

    Histogram successfulNthMapperAttempts = new Histogram();
    // Histogram successfulNthReducerAttempts = new Histogram();
    // Histogram mapperLocality = new Histogram();

    for (LoggedTask task : result.getMapTasks()) {
      for (LoggedTaskAttempt attempt : task.getAttempts()) {
        int distance = successfulMapAttemptTimes.length - 1;
        Long runtime = null;

        if (attempt.getFinishTime() > 0 && attempt.getStartTime() > 0) {
          runtime = attempt.getFinishTime() - attempt.getStartTime();

          if (attempt.getResult() == Values.SUCCESS) {
            LoggedLocation host = attempt.getLocation();

            List<LoggedLocation> locs = task.getPreferredLocations();

            if (host != null && locs != null) {
              for (LoggedLocation loc : locs) {
                ParsedHost preferedLoc = new ParsedHost(loc);

                distance =
                    Math.min(distance, preferedLoc
                        .distance(new ParsedHost(host)));
              }

              // mapperLocality.enter(distance);
            }

            if (attempt.getStartTime() > 0 && attempt.getFinishTime() > 0) {
              if (runtime != null) {
                successfulMapAttemptTimes[distance].enter(runtime);
              }
            }

            TaskAttemptID attemptID = attempt.getAttemptID();

            if (attemptID != null) {
              successfulNthMapperAttempts.enter(attemptID.getId());
            }
          } else {
            if (attempt.getResult() == Pre21JobHistoryConstants.Values.FAILED) {
              if (runtime != null) {
                failedMapAttemptTimes[distance].enter(runtime);
              }
            }
          }
        }
      }
    }

    for (LoggedTask task : result.getReduceTasks()) {
      for (LoggedTaskAttempt attempt : task.getAttempts()) {
        Long runtime = attempt.getFinishTime() - attempt.getStartTime();

        if (attempt.getFinishTime() > 0 && attempt.getStartTime() > 0) {
          runtime = attempt.getFinishTime() - attempt.getStartTime();
        }
        if (attempt.getResult() == Values.SUCCESS) {
          if (runtime != null) {
            successfulReduceAttemptTimes.enter(runtime);
          }
        } else if (attempt.getResult() == Pre21JobHistoryConstants.Values.FAILED) {
          failedReduceAttemptTimes.enter(runtime);
        }
      }
    }

    result.setFailedMapAttemptCDFs(mapCDFArrayList(failedMapAttemptTimes));

    LoggedDiscreteCDF failedReduce = new LoggedDiscreteCDF();
    failedReduce.setCDF(failedReduceAttemptTimes, attemptTimesPercentiles, 100);
    result.setFailedReduceAttemptCDF(failedReduce);

    result
        .setSuccessfulMapAttemptCDFs(mapCDFArrayList(successfulMapAttemptTimes));

    LoggedDiscreteCDF succReduce = new LoggedDiscreteCDF();
    succReduce.setCDF(successfulReduceAttemptTimes, attemptTimesPercentiles,
        100);
    result.setSuccessfulReduceAttemptCDF(succReduce);

    long totalSuccessfulAttempts = 0L;
    long maxTriesToSucceed = 0L;

    for (Map.Entry<Long, Long> ent : successfulNthMapperAttempts) {
      totalSuccessfulAttempts += ent.getValue();
      maxTriesToSucceed = Math.max(maxTriesToSucceed, ent.getKey());
    }

    if (totalSuccessfulAttempts > 0L) {
      double[] successAfterI = new double[(int) maxTriesToSucceed + 1];
      for (int i = 0; i < successAfterI.length; ++i) {
        successAfterI[i] = 0.0D;
      }

      for (Map.Entry<Long, Long> ent : successfulNthMapperAttempts) {
        successAfterI[ent.getKey().intValue()] =
            ((double) ent.getValue()) / totalSuccessfulAttempts;
      }
      result.setMapperTriesToSucceed(successAfterI);
    } else {
      result.setMapperTriesToSucceed(null);
    }

    return result;
  }

  private ArrayList<LoggedDiscreteCDF> mapCDFArrayList(Histogram[] data) {
    ArrayList<LoggedDiscreteCDF> result = new ArrayList<LoggedDiscreteCDF>();

    for (Histogram hist : data) {
      LoggedDiscreteCDF discCDF = new LoggedDiscreteCDF();
      discCDF.setCDF(hist, attemptTimesPercentiles, 100);
      result.add(discCDF);
    }

    return result;
  }

  private static Values getPre21Value(String name) {
    if (name.equalsIgnoreCase("JOB_CLEANUP")) {
      return Values.CLEANUP;
    }
    if (name.equalsIgnoreCase("JOB_SETUP")) {
      return Values.SETUP;
    }

    // Note that pre-21, the task state of a successful task was logged as 
    // SUCCESS while from 21 onwards, its logged as SUCCEEDED.
    if (name.equalsIgnoreCase(TaskStatus.State.SUCCEEDED.toString())) {
      return Values.SUCCESS;
    }
    
    return Values.valueOf(name.toUpperCase());
  }

  private void processTaskUpdatedEvent(TaskUpdatedEvent event) {
    ParsedTask task = getTask(event.getTaskId().toString());
    if (task == null) {
      return;
    }
    task.setFinishTime(event.getFinishTime());
  }

  private void processTaskStartedEvent(TaskStartedEvent event) {
    ParsedTask task =
        getOrMakeTask(event.getTaskType(), event.getTaskId().toString(), true);
    task.setStartTime(event.getStartTime());
    task.setPreferredLocations(preferredLocationForSplits(event
        .getSplitLocations()));
  }

  private void processTaskFinishedEvent(TaskFinishedEvent event) {
    ParsedTask task =
        getOrMakeTask(event.getTaskType(), event.getTaskId().toString(), false);
    if (task == null) {
      return;
    }
    task.setFinishTime(event.getFinishTime());
    task.setTaskStatus(getPre21Value(event.getTaskStatus()));
    task.incorporateCounters(((TaskFinished) event.getDatum()).counters);
  }

  private void processTaskFailedEvent(TaskFailedEvent event) {
    ParsedTask task =
        getOrMakeTask(event.getTaskType(), event.getTaskId().toString(), false);
    if (task == null) {
      return;
    }
    task.setFinishTime(event.getFinishTime());
    task.setTaskStatus(getPre21Value(event.getTaskStatus()));
    TaskFailed t = (TaskFailed)(event.getDatum());
    task.putDiagnosticInfo(t.error.toString());
    task.putFailedDueToAttemptId(t.failedDueToAttempt.toString());
    org.apache.hadoop.mapreduce.jobhistory.JhCounters counters =
        ((TaskFailed) event.getDatum()).counters;
    task.incorporateCounters(
        counters == null ? EMPTY_COUNTERS : counters);
  }

  private void processTaskAttemptUnsuccessfulCompletionEvent(
      TaskAttemptUnsuccessfulCompletionEvent event) {
    ParsedTaskAttempt attempt =
        getOrMakeTaskAttempt(event.getTaskType(), event.getTaskId().toString(),
            event.getTaskAttemptId().toString());

    if (attempt == null) {
      return;
    }

    attempt.setResult(getPre21Value(event.getTaskStatus()));
    attempt.setHostName(event.getHostname(), event.getRackName());
    ParsedHost pHost = 
      getAndRecordParsedHost(event.getRackName(), event.getHostname());
    if (pHost != null) {
      attempt.setLocation(pHost.makeLoggedLocation());
    }

    attempt.setFinishTime(event.getFinishTime());
    org.apache.hadoop.mapreduce.jobhistory.JhCounters counters =
        ((TaskAttemptUnsuccessfulCompletion) event.getDatum()).counters;
    attempt.incorporateCounters(
        counters == null ? EMPTY_COUNTERS : counters);
    attempt.arraySetClockSplits(event.getClockSplits());
    attempt.arraySetCpuUsages(event.getCpuUsages());
    attempt.arraySetVMemKbytes(event.getVMemKbytes());
    attempt.arraySetPhysMemKbytes(event.getPhysMemKbytes());
    TaskAttemptUnsuccessfulCompletion t =
        (TaskAttemptUnsuccessfulCompletion) (event.getDatum());
    attempt.putDiagnosticInfo(t.error.toString());
  }

  private void processTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    ParsedTaskAttempt attempt =
        getOrMakeTaskAttempt(event.getTaskType(), event.getTaskId().toString(),
            event.getTaskAttemptId().toString());
    if (attempt == null) {
      return;
    }
    attempt.setStartTime(event.getStartTime());
    attempt.putTrackerName(event.getTrackerName());
    attempt.putHttpPort(event.getHttpPort());
    attempt.putShufflePort(event.getShufflePort());
  }

  private void processTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    ParsedTaskAttempt attempt =
        getOrMakeTaskAttempt(event.getTaskType(), event.getTaskId().toString(),
            event.getAttemptId().toString());
    if (attempt == null) {
      return;
    }
    attempt.setResult(getPre21Value(event.getTaskStatus()));
    ParsedHost pHost = getAndRecordParsedHost(event.getRackName(), event.getHostname());
    if (pHost != null) {
      attempt.setLocation(pHost.makeLoggedLocation());
    }
    attempt.setFinishTime(event.getFinishTime());
    attempt
        .incorporateCounters(((TaskAttemptFinished) event.getDatum()).counters);
  }

  private void processReduceAttemptFinishedEvent(
      ReduceAttemptFinishedEvent event) {
    ParsedTaskAttempt attempt =
        getOrMakeTaskAttempt(event.getTaskType(), event.getTaskId().toString(),
            event.getAttemptId().toString());
    if (attempt == null) {
      return;
    }
    attempt.setResult(getPre21Value(event.getTaskStatus()));
    attempt.setHostName(event.getHostname(), event.getRackName());
    ParsedHost pHost = 
      getAndRecordParsedHost(event.getRackName(), event.getHostname());
    if (pHost != null) {
      attempt.setLocation(pHost.makeLoggedLocation());
    }

    // XXX There may be redundant location info available in the event.
    // We might consider extracting it from this event. Currently this
    // is redundant, but making this will add future-proofing.
    attempt.setFinishTime(event.getFinishTime());
    attempt.setShuffleFinished(event.getShuffleFinishTime());
    attempt.setSortFinished(event.getSortFinishTime());
    attempt
        .incorporateCounters(((ReduceAttemptFinished) event.getDatum()).counters);
    attempt.arraySetClockSplits(event.getClockSplits());
    attempt.arraySetCpuUsages(event.getCpuUsages());
    attempt.arraySetVMemKbytes(event.getVMemKbytes());
    attempt.arraySetPhysMemKbytes(event.getPhysMemKbytes());
  }

  private void processMapAttemptFinishedEvent(MapAttemptFinishedEvent event) {
    ParsedTaskAttempt attempt =
        getOrMakeTaskAttempt(event.getTaskType(), event.getTaskId().toString(),
            event.getAttemptId().toString());
    if (attempt == null) {
      return;
    }
    attempt.setResult(getPre21Value(event.getTaskStatus()));
    attempt.setHostName(event.getHostname(), event.getRackName());

    ParsedHost pHost = 
      getAndRecordParsedHost(event.getRackName(), event.getHostname());
    if (pHost != null) {
      attempt.setLocation(pHost.makeLoggedLocation());
    }
    
    // XXX There may be redundant location info available in the event.
    // We might consider extracting it from this event. Currently this
    // is redundant, but making this will add future-proofing.
    attempt.setFinishTime(event.getFinishTime());
    attempt
      .incorporateCounters(((MapAttemptFinished) event.getDatum()).counters);
    attempt.arraySetClockSplits(event.getClockSplits());
    attempt.arraySetCpuUsages(event.getCpuUsages());
    attempt.arraySetVMemKbytes(event.getVMemKbytes());
    attempt.arraySetPhysMemKbytes(event.getPhysMemKbytes());
  }

  private void processJobUnsuccessfulCompletionEvent(
      JobUnsuccessfulCompletionEvent event) {
    result.setOutcome(Pre21JobHistoryConstants.Values
        .valueOf(event.getStatus()));
    result.setFinishTime(event.getFinishTime());
    // No counters in JobUnsuccessfulCompletionEvent
  }

  private void processJobSubmittedEvent(JobSubmittedEvent event) {
    result.setJobID(event.getJobId().toString());
    result.setJobName(event.getJobName());
    result.setUser(event.getUserName());
    result.setSubmitTime(event.getSubmitTime());
    result.putJobConfPath(event.getJobConfPath());
    result.putJobAcls(event.getJobAcls());

    // set the queue name if existing
    String queue = event.getJobQueueName();
    if (queue != null) {
      result.setQueue(queue);
    }
  }

  private void processJobStatusChangedEvent(JobStatusChangedEvent event) {
    result.setOutcome(Pre21JobHistoryConstants.Values
        .valueOf(event.getStatus()));
  }

  private void processJobPriorityChangeEvent(JobPriorityChangeEvent event) {
    result.setPriority(LoggedJob.JobPriority.valueOf(event.getPriority()
        .toString()));
  }

  private void processJobInitedEvent(JobInitedEvent event) {
    result.setLaunchTime(event.getLaunchTime());
    result.setTotalMaps(event.getTotalMaps());
    result.setTotalReduces(event.getTotalReduces());
  }

  private void processJobInfoChangeEvent(JobInfoChangeEvent event) {
    result.setLaunchTime(event.getLaunchTime());
  }

  private void processJobFinishedEvent(JobFinishedEvent event) {
    result.setFinishTime(event.getFinishTime());
    result.setJobID(jobID);
    result.setOutcome(Values.SUCCESS);

    JobFinished job = (JobFinished)event.getDatum();
    Map<String, Long> countersMap =
        JobHistoryUtils.extractCounters(job.totalCounters);
    result.putTotalCounters(countersMap);
    countersMap = JobHistoryUtils.extractCounters(job.mapCounters);
    result.putMapCounters(countersMap);
    countersMap = JobHistoryUtils.extractCounters(job.reduceCounters);
    result.putReduceCounters(countersMap);
  }

  private ParsedTask getTask(String taskIDname) {
    ParsedTask result = mapTasks.get(taskIDname);

    if (result != null) {
      return result;
    }

    result = reduceTasks.get(taskIDname);

    if (result != null) {
      return result;
    }

    return otherTasks.get(taskIDname);
  }

  /**
   * @param type
   *          the task type
   * @param taskIDname
   *          the task ID name, as a string
   * @param allowCreate
   *          if true, we can create a task.
   * @return
   */
  private ParsedTask getOrMakeTask(TaskType type, String taskIDname,
      boolean allowCreate) {
    Map<String, ParsedTask> taskMap = otherTasks;
    List<LoggedTask> tasks = this.result.getOtherTasks();

    switch (type) {
    case MAP:
      taskMap = mapTasks;
      tasks = this.result.getMapTasks();
      break;

    case REDUCE:
      taskMap = reduceTasks;
      tasks = this.result.getReduceTasks();
      break;

    default:
      // no code
    }

    ParsedTask result = taskMap.get(taskIDname);

    if (result == null && allowCreate) {
      result = new ParsedTask();
      result.setTaskType(getPre21Value(type.toString()));
      result.setTaskID(taskIDname);
      taskMap.put(taskIDname, result);
      tasks.add(result);
    }

    return result;
  }

  private ParsedTaskAttempt getOrMakeTaskAttempt(TaskType type,
      String taskIDName, String taskAttemptName) {
    ParsedTask task = getOrMakeTask(type, taskIDName, false);
    ParsedTaskAttempt result = attempts.get(taskAttemptName);

    if (result == null && task != null) {
      result = new ParsedTaskAttempt();
      result.setAttemptID(taskAttemptName);
      attempts.put(taskAttemptName, result);
      task.getAttempts().add(result);
    }

    return result;
  }

  private ParsedHost getAndRecordParsedHost(String hostName) {
    return getAndRecordParsedHost(null, hostName);
  }
  
  private ParsedHost getAndRecordParsedHost(String rackName, String hostName) {
    ParsedHost result = null;
    if (rackName == null) {
      // for old (pre-23) job history files where hostname was represented as
      // /rackname/hostname
      result = ParsedHost.parse(hostName);
    } else {
      // for new (post-23) job history files
      result = new ParsedHost(rackName, hostName);
    }

    if (result != null) {
      ParsedHost canonicalResult = allHosts.get(result);

      if (canonicalResult != null) {
        return canonicalResult;
      }

      allHosts.put(result, result);

      return result;
    }

    return null;
  }

  private ArrayList<LoggedLocation> preferredLocationForSplits(String splits) {
    if (splits != null) {
      ArrayList<LoggedLocation> locations = null;

      StringTokenizer tok = new StringTokenizer(splits, ",", false);

      if (tok.countTokens() <= MAXIMUM_PREFERRED_LOCATIONS) {
        locations = new ArrayList<LoggedLocation>();

        while (tok.hasMoreTokens()) {
          String nextSplit = tok.nextToken();

          ParsedHost node = getAndRecordParsedHost(nextSplit);

          if (locations != null && node != null) {
            locations.add(node.makeLoggedLocation());
          }
        }

        return locations;
      }
    }

    return null;
  }
}
