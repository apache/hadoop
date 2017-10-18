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
package org.apache.hadoop.yarn.sls.synthetic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.*;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.mapreduce.MRJobConfig.QUEUE_NAME;

/**
 * Generates random task data for a synthetic job.
 */
public class SynthJob implements JobStory {

  @SuppressWarnings("StaticVariableName")
  private static Log LOG = LogFactory.getLog(SynthJob.class);

  private final Configuration conf;
  private final int id;

  @SuppressWarnings("ConstantName")
  private static final AtomicInteger sequence = new AtomicInteger(0);
  private final String name;
  private final String queueName;
  private final SynthJobClass jobClass;

  // job timing
  private final long submitTime;
  private final long duration;
  private final long deadline;

  private final int numMapTasks;
  private final int numRedTasks;
  private final long mapMaxMemory;
  private final long reduceMaxMemory;
  private final long mapMaxVcores;
  private final long reduceMaxVcores;
  private final long[] mapRuntime;
  private final float[] reduceRuntime;
  private long totMapRuntime;
  private long totRedRuntime;

  public SynthJob(JDKRandomGenerator rand, Configuration conf,
      SynthJobClass jobClass, long actualSubmissionTime) {

    this.conf = conf;
    this.jobClass = jobClass;

    this.duration = MILLISECONDS.convert(jobClass.getDur(), SECONDS);
    this.numMapTasks = jobClass.getMtasks();
    this.numRedTasks = jobClass.getRtasks();

    // sample memory distributions, correct for sub-minAlloc sizes
    long tempMapMaxMemory = jobClass.getMapMaxMemory();
    this.mapMaxMemory = tempMapMaxMemory < MRJobConfig.DEFAULT_MAP_MEMORY_MB
        ? MRJobConfig.DEFAULT_MAP_MEMORY_MB : tempMapMaxMemory;
    long tempReduceMaxMemory = jobClass.getReduceMaxMemory();
    this.reduceMaxMemory =
            tempReduceMaxMemory < MRJobConfig.DEFAULT_REDUCE_MEMORY_MB
            ? MRJobConfig.DEFAULT_REDUCE_MEMORY_MB : tempReduceMaxMemory;

    // sample vcores distributions, correct for sub-minAlloc sizes
    long tempMapMaxVCores = jobClass.getMapMaxVcores();
    this.mapMaxVcores = tempMapMaxVCores < MRJobConfig.DEFAULT_MAP_CPU_VCORES
        ? MRJobConfig.DEFAULT_MAP_CPU_VCORES : tempMapMaxVCores;
    long tempReduceMaxVcores = jobClass.getReduceMaxVcores();
    this.reduceMaxVcores =
        tempReduceMaxVcores < MRJobConfig.DEFAULT_REDUCE_CPU_VCORES
            ? MRJobConfig.DEFAULT_REDUCE_CPU_VCORES : tempReduceMaxVcores;

    if (numMapTasks > 0) {
      conf.setLong(MRJobConfig.MAP_MEMORY_MB, this.mapMaxMemory);
      conf.set(MRJobConfig.MAP_JAVA_OPTS,
          "-Xmx" + (this.mapMaxMemory - 100) + "m");
    }

    if (numRedTasks > 0) {
      conf.setLong(MRJobConfig.REDUCE_MEMORY_MB, this.reduceMaxMemory);
      conf.set(MRJobConfig.REDUCE_JAVA_OPTS,
          "-Xmx" + (this.reduceMaxMemory - 100) + "m");
    }

    boolean hasDeadline =
        (rand.nextDouble() <= jobClass.jobClass.chance_of_reservation);

    LogNormalDistribution deadlineFactor =
        SynthUtils.getLogNormalDist(rand, jobClass.jobClass.deadline_factor_avg,
            jobClass.jobClass.deadline_factor_stddev);

    double deadlineFactorSample =
        (deadlineFactor != null) ? deadlineFactor.sample() : -1;

    this.queueName = jobClass.workload.getQueueName();

    this.submitTime = MILLISECONDS.convert(actualSubmissionTime, SECONDS);

    this.deadline =
        hasDeadline ? MILLISECONDS.convert(actualSubmissionTime, SECONDS)
            + (long) Math.ceil(deadlineFactorSample * duration) : -1;

    conf.set(QUEUE_NAME, queueName);

    // name and initialize job randomness
    final long seed = rand.nextLong();
    rand.setSeed(seed);
    id = sequence.getAndIncrement();

    name = String.format(jobClass.getClassName() + "_%06d", id);
    LOG.debug(name + " (" + seed + ")");

    LOG.info("JOB TIMING`: job: " + name + " submission:" + submitTime
        + " deadline:" + deadline + " duration:" + duration
        + " deadline-submission: " + (deadline - submitTime));

    // generate map and reduce runtimes
    mapRuntime = new long[numMapTasks];
    for (int i = 0; i < numMapTasks; i++) {
      mapRuntime[i] = jobClass.getMapTimeSample();
      totMapRuntime += mapRuntime[i];
    }
    reduceRuntime = new float[numRedTasks];
    for (int i = 0; i < numRedTasks; i++) {
      reduceRuntime[i] = jobClass.getReduceTimeSample();
      totRedRuntime += (long) Math.ceil(reduceRuntime[i]);
    }
  }

  public boolean hasDeadline() {
    return deadline > 0;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getUser() {
    return jobClass.getUserName();
  }

  @Override
  public JobID getJobID() {
    return new JobID("job_mock_" + name, id);
  }

  @Override
  public Values getOutcome() {
    return Values.SUCCESS;
  }

  @Override
  public long getSubmissionTime() {
    return submitTime;
  }

  @Override
  public int getNumberMaps() {
    return numMapTasks;
  }

  @Override
  public int getNumberReduces() {
    return numRedTasks;
  }

  @Override
  public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
    switch (taskType) {
    case MAP:
      return new TaskInfo(-1, -1, -1, -1, mapMaxMemory, mapMaxVcores);
    case REDUCE:
      return new TaskInfo(-1, -1, -1, -1, reduceMaxMemory, reduceMaxVcores);
    default:
      throw new IllegalArgumentException("Not interested");
    }
  }

  @Override
  public InputSplit[] getInputSplits() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, int taskNumber,
      int taskAttemptNumber) {
    switch (taskType) {
    case MAP:
      return new MapTaskAttemptInfo(State.SUCCEEDED,
          getTaskInfo(taskType, taskNumber), mapRuntime[taskNumber], null);

    case REDUCE:
      // We assume uniform split between pull/sort/reduce
      // aligned with naive progress reporting assumptions
      return new ReduceTaskAttemptInfo(State.SUCCEEDED,
          getTaskInfo(taskType, taskNumber),
          (long) Math.round((reduceRuntime[taskNumber] / 3)),
          (long) Math.round((reduceRuntime[taskNumber] / 3)),
          (long) Math.round((reduceRuntime[taskNumber] / 3)), null);

    default:
      break;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
      int taskAttemptNumber, int locality) {
    throw new UnsupportedOperationException();
  }

  @Override
  public org.apache.hadoop.mapred.JobConf getJobConf() {
    return new JobConf(conf);
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public String toString() {
    return "SynthJob [\n" + "  workload=" + jobClass.getWorkload().getId()
        + "\n" + "  jobClass="
        + jobClass.getWorkload().getClassList().indexOf(jobClass) + "\n"
        + "  conf=" + conf + ",\n" + "  id=" + id + ",\n" + "  name=" + name
        + ",\n" + "  mapRuntime=" + Arrays.toString(mapRuntime) + ",\n"
        + "  reduceRuntime=" + Arrays.toString(reduceRuntime) + ",\n"
        + "  submitTime=" + submitTime + ",\n" + "  numMapTasks=" + numMapTasks
        + ",\n" + "  numRedTasks=" + numRedTasks + ",\n" + "  mapMaxMemory="
        + mapMaxMemory + ",\n" + "  reduceMaxMemory=" + reduceMaxMemory + ",\n"
        + "  queueName=" + queueName + "\n" + "]";
  }

  public SynthJobClass getJobClass() {
    return jobClass;
  }

  public long getTotalSlotTime() {
    return totMapRuntime + totRedRuntime;
  }

  public long getDuration() {
    return duration;
  }

  public long getDeadline() {
    return deadline;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SynthJob)) {
      return false;
    }
    SynthJob o = (SynthJob) other;
    return Arrays.equals(mapRuntime, o.mapRuntime)
        && Arrays.equals(reduceRuntime, o.reduceRuntime)
        && submitTime == o.submitTime && numMapTasks == o.numMapTasks
        && numRedTasks == o.numRedTasks && mapMaxMemory == o.mapMaxMemory
        && reduceMaxMemory == o.reduceMaxMemory
        && mapMaxVcores == o.mapMaxVcores
        && reduceMaxVcores == o.reduceMaxVcores && queueName.equals(o.queueName)
        && jobClass.equals(o.jobClass) && totMapRuntime == o.totMapRuntime
        && totRedRuntime == o.totRedRuntime;
  }

  @Override
  public int hashCode() {
    // could have a bad distr; investigate if a relevant use case exists
    return jobClass.hashCode() * (int) submitTime;
  }
}
