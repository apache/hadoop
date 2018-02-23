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
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.MapTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.ReduceTaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;
import org.apache.hadoop.yarn.sls.appmaster.MRAMSimulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private static final long MIN_MEMORY = 1024;
  private static final long MIN_VCORES = 1;

  private final Configuration conf;
  private final int id;

  @SuppressWarnings("ConstantName")
  private static final AtomicInteger sequence = new AtomicInteger(0);
  private final String name;
  private final String queueName;
  private final SynthTraceJobProducer.JobDefinition jobDef;

  private String type;

  // job timing
  private final long submitTime;
  private final long duration;
  private final long deadline;

  private Map<String, String> params;

  private long totalSlotTime = 0;

  // task information
  private List<SynthTask> tasks = new ArrayList<>();
  private Map<String, List<SynthTask>> taskByType = new HashMap<>();
  private Map<String, Integer> taskCounts = new HashMap<>();
  private Map<String, Long> taskMemory = new HashMap<>();
  private Map<String, Long> taskVcores = new HashMap<>();

  /**
   * Nested class used to represent a task instance in a job. Each task
   * corresponds to one container allocation for the job.
   */
  public static final class SynthTask{
    private String type;
    private long time;
    private long maxMemory;
    private long maxVcores;
    private int priority;

    private SynthTask(String type, long time, long maxMemory, long maxVcores,
        int priority){
      this.type = type;
      this.time = time;
      this.maxMemory = maxMemory;
      this.maxVcores = maxVcores;
      this.priority = priority;
    }

    public String getType(){
      return type;
    }

    public long getTime(){
      return time;
    }

    public long getMemory(){
      return maxMemory;
    }

    public long getVcores(){
      return maxVcores;
    }

    public int getPriority(){
      return priority;
    }

    @Override
    public String toString(){
      return String.format("[task]\ttype: %1$-10s\ttime: %2$3s\tmemory: "
              + "%3$4s\tvcores: %4$2s%n", getType(), getTime(), getMemory(),
          getVcores());
    }
  }


  protected SynthJob(JDKRandomGenerator rand, Configuration conf,
      SynthTraceJobProducer.JobDefinition jobDef,
      String queue, long actualSubmissionTime) {

    this.conf = conf;
    this.jobDef = jobDef;

    this.queueName = queue;

    this.duration = MILLISECONDS.convert(jobDef.duration.getInt(),
        SECONDS);

    boolean hasDeadline =
        (rand.nextDouble() <= jobDef.reservation.getDouble());

    double deadlineFactorSample = jobDef.deadline_factor.getDouble();

    this.type = jobDef.type;

    this.submitTime = MILLISECONDS.convert(actualSubmissionTime, SECONDS);

    this.deadline =
        hasDeadline ? MILLISECONDS.convert(actualSubmissionTime, SECONDS)
            + (long) Math.ceil(deadlineFactorSample * duration) : -1;

    this.params = jobDef.params;

    conf.set(QUEUE_NAME, queueName);

    // name and initialize job randomness
    final long seed = rand.nextLong();
    rand.setSeed(seed);
    id = sequence.getAndIncrement();

    name = String.format(jobDef.class_name + "_%06d", id);
    LOG.debug(name + " (" + seed + ")");

    LOG.info("JOB TIMING`: job: " + name + " submission:" + submitTime
        + " deadline:" + deadline + " duration:" + duration
        + " deadline-submission: " + (deadline - submitTime));

    // Expand tasks
    for(SynthTraceJobProducer.TaskDefinition task : jobDef.tasks){
      int num = task.count.getInt();
      String taskType = task.type;
      long memory = task.max_memory.getLong();
      memory = memory < MIN_MEMORY ? MIN_MEMORY: memory;
      long vcores = task.max_vcores.getLong();
      vcores = vcores < MIN_VCORES ? MIN_VCORES  : vcores;
      int priority = task.priority;

      // Save task information by type
      taskByType.put(taskType, new ArrayList<>());
      taskCounts.put(taskType, num);
      taskMemory.put(taskType, memory);
      taskVcores.put(taskType, vcores);

      for(int i = 0; i < num; ++i){
        long time = task.time.getLong();
        totalSlotTime += time;
        SynthTask t = new SynthTask(taskType, time, memory, vcores,
            priority);
        tasks.add(t);
        taskByType.get(taskType).add(t);
      }
    }

  }

  public String getType(){
    return type;
  }

  public List<SynthTask> getTasks(){
    return tasks;
  }

  public boolean hasDeadline() {
    return deadline > 0;
  }

  public String getName() {
    return name;
  }

  public String getUser() {
    return jobDef.user_name;
  }

  public JobID getJobID() {
    return new JobID("job_mock_" + name, id);
  }

  public long getSubmissionTime() {
    return submitTime;
  }

  public String getQueueName() {
    return queueName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String res = "\nSynthJob [" + jobDef.class_name + "]: \n"
        + "\tname: " + getName() + "\n"
        + "\ttype: " + getType() + "\n"
        + "\tid: " + id + "\n"
        + "\tqueue: " + getQueueName() + "\n"
        + "\tsubmission: " + getSubmissionTime() + "\n"
        + "\tduration: " + getDuration() + "\n"
        + "\tdeadline: " + getDeadline() + "\n";
    sb.append(res);
    int taskno = 0;
    for(SynthJob.SynthTask t : getTasks()){
      sb.append("\t");
      sb.append(taskno);
      sb.append(": \t");
      sb.append(t.toString());
      taskno++;
    }
    return sb.toString();
  }

  public long getTotalSlotTime() {
    return totalSlotTime;
  }

  public long getDuration() {
    return duration;
  }

  public long getDeadline() {
    return deadline;
  }

  public Map<String, String> getParams() {
    return params;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SynthJob)) {
      return false;
    }
    SynthJob o = (SynthJob) other;
    return tasks.equals(o.tasks)
        && submitTime == o.submitTime
        && type.equals(o.type)
        && queueName.equals(o.queueName)
        && jobDef.class_name.equals(o.jobDef.class_name);
  }

  @Override
  public int hashCode() {
    return jobDef.class_name.hashCode()
        * (int) submitTime * (int) duration;
  }


  @Override
  public JobConf getJobConf() {
    return new JobConf(conf);
  }

  @Override
  public int getNumberMaps() {
    return taskCounts.get(MRAMSimulator.MAP_TYPE);
  }

  @Override
  public int getNumberReduces() {
    return taskCounts.get(MRAMSimulator.REDUCE_TYPE);
  }

  @Override
  public InputSplit[] getInputSplits() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
    switch(taskType){
    case MAP:
      return new TaskInfo(-1, -1, -1, -1,
          taskMemory.get(MRAMSimulator.MAP_TYPE),
          taskVcores.get(MRAMSimulator.MAP_TYPE));
    case REDUCE:
      return new TaskInfo(-1, -1, -1, -1,
          taskMemory.get(MRAMSimulator.REDUCE_TYPE),
          taskVcores.get(MRAMSimulator.REDUCE_TYPE));
    default:
      break;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, int taskNumber,
      int taskAttemptNumber) {
    switch (taskType) {
    case MAP:
      return new MapTaskAttemptInfo(State.SUCCEEDED,
          getTaskInfo(taskType, taskNumber),
          taskByType.get(MRAMSimulator.MAP_TYPE).get(taskNumber).time,
          null);
    case REDUCE:
      // We assume uniform split between pull/sort/reduce
      // aligned with naive progress reporting assumptions
      return new ReduceTaskAttemptInfo(State.SUCCEEDED,
          getTaskInfo(taskType, taskNumber),
          taskByType.get(MRAMSimulator.MAP_TYPE)
              .get(taskNumber).time / 3,
          taskByType.get(MRAMSimulator.MAP_TYPE)
              .get(taskNumber).time / 3,
          taskByType.get(MRAMSimulator.MAP_TYPE)
              .get(taskNumber).time / 3, null);
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
  public Values getOutcome() {
    return Values.SUCCESS;
  }
}
