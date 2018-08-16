/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.sls.synthetic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.sls.appmaster.MRAMSimulator;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.codehaus.jackson.JsonParser.Feature.INTERN_FIELD_NAMES;
import static org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * This is a JobStoryProducer that operates from distribution of different
 * workloads. The .json input file is used to determine how many weight, which
 * size, number of maps/reducers and their duration, as well as the temporal
 * distributed of submissions. For each parameter we control avg and stdev, and
 * generate values via normal or log-normal distributions.
 */
public class SynthTraceJobProducer implements JobStoryProducer {

  @SuppressWarnings("StaticVariableName")
  private static final Logger LOG = LoggerFactory.getLogger(SynthTraceJobProducer.class);

  private final Configuration conf;
  private final AtomicInteger numJobs;
  private final Trace trace;
  private final long seed;

  private int totalWeight;

  private final Queue<StoryParams> listStoryParams;

  private final JDKRandomGenerator rand;

  public static final String SLS_SYNTHETIC_TRACE_FILE =
      "sls.synthetic" + ".trace_file";

  private final static int DEFAULT_MAPPER_PRIORITY = 20;
  private final static int DEFAULT_REDUCER_PRIORITY = 10;

  public SynthTraceJobProducer(Configuration conf) throws IOException {
    this(conf, new Path(conf.get(SLS_SYNTHETIC_TRACE_FILE)));
  }

  public SynthTraceJobProducer(Configuration conf, Path path)
      throws IOException {

    LOG.info("SynthTraceJobProducer");

    this.conf = conf;
    this.rand = new JDKRandomGenerator();

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(INTERN_FIELD_NAMES, true);
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    FileSystem ifs = path.getFileSystem(conf);
    FSDataInputStream fileIn = ifs.open(path);

    // Initialize the random generator and the seed
    this.trace = mapper.readValue(fileIn, Trace.class);
    this.seed = trace.rand_seed;
    this.rand.setSeed(seed);
    // Initialize the trace
    this.trace.init(rand);

    this.numJobs = new AtomicInteger(trace.num_jobs);

    for (Double w : trace.workload_weights) {
      totalWeight += w;
    }

    // Initialize our story parameters
    listStoryParams = createStory();

    LOG.info("Generated " + listStoryParams.size() + " deadlines for "
        + this.numJobs.get() + " jobs");
  }

  // StoryParams hold the minimum amount of information needed to completely
  // specify a job run: job definition, start time, and queue.
  // This allows us to create "jobs" and then order them according to start time
  static class StoryParams {
    // Time the job gets submitted to
    private long actualSubmissionTime;
    // The queue the job gets submitted to
    private String queue;
    // Definition to construct the job from
    private JobDefinition jobDef;

    StoryParams(long actualSubmissionTime, String queue, JobDefinition jobDef) {
      this.actualSubmissionTime = actualSubmissionTime;
      this.queue = queue;
      this.jobDef = jobDef;
    }
  }


  private Queue<StoryParams> createStory() {
    // create priority queue to keep start-time sorted
    Queue<StoryParams> storyQueue =
        new PriorityQueue<>(this.numJobs.get(), new Comparator<StoryParams>() {
          @Override
          public int compare(StoryParams o1, StoryParams o2) {
            return Math
                .toIntExact(o1.actualSubmissionTime - o2.actualSubmissionTime);
          }
        });
    for (int i = 0; i < numJobs.get(); i++) {
      // Generate a workload
      Workload wl = trace.generateWorkload();
      // Save all the parameters needed to completely define a job
      long actualSubmissionTime = wl.generateSubmissionTime();
      String queue = wl.queue_name;
      JobDefinition job = wl.generateJobDefinition();
      storyQueue.add(new StoryParams(actualSubmissionTime, queue, job));
    }
    return storyQueue;
  }

  @Override
  public JobStory getNextJob() throws IOException {
    if (numJobs.decrementAndGet() < 0) {
      return null;
    }
    StoryParams storyParams = listStoryParams.poll();
    return new SynthJob(rand, conf, storyParams.jobDef, storyParams.queue,
        storyParams.actualSubmissionTime);
  }

  @Override
  public void close(){
  }

  @Override
  public String toString() {
    return "SynthTraceJobProducer [ conf=" + conf + ", numJobs=" + numJobs
        + ", r=" + rand + ", totalWeight="
        + totalWeight + ", workloads=" + trace.workloads + "]";
  }

  public int getNumJobs() {
    return trace.num_jobs;
  }

  // Helper to parse and maintain backwards compatibility with
  // syn json formats
  private static void validateJobDef(JobDefinition jobDef){
    if(jobDef.tasks == null) {
      LOG.info("Detected old JobDefinition format. Converting.");
      try {
        jobDef.tasks = new ArrayList<>();
        jobDef.type = "mapreduce";
        jobDef.deadline_factor = new Sample(jobDef.deadline_factor_avg,
            jobDef.deadline_factor_stddev);
        jobDef.duration = new Sample(jobDef.dur_avg,
            jobDef.dur_stddev);
        jobDef.reservation = new Sample(jobDef.chance_of_reservation);

        TaskDefinition map = new TaskDefinition();
        map.type = MRAMSimulator.MAP_TYPE;
        map.count = new Sample(jobDef.mtasks_avg, jobDef.mtasks_stddev);
        map.time = new Sample(jobDef.mtime_avg, jobDef.mtime_stddev);
        map.max_memory = new Sample((double) jobDef.map_max_memory_avg,
            jobDef.map_max_memory_stddev);
        map.max_vcores = new Sample((double) jobDef.map_max_vcores_avg,
            jobDef.map_max_vcores_stddev);
        map.priority = DEFAULT_MAPPER_PRIORITY;
        map.executionType = jobDef.map_execution_type;

        jobDef.tasks.add(map);
        TaskDefinition reduce = new TaskDefinition();
        reduce.type = MRAMSimulator.REDUCE_TYPE;
        reduce.count = new Sample(jobDef.rtasks_avg, jobDef.rtasks_stddev);
        reduce.time = new Sample(jobDef.rtime_avg, jobDef.rtime_stddev);
        reduce.max_memory = new Sample((double) jobDef.reduce_max_memory_avg,
            jobDef.reduce_max_memory_stddev);
        reduce.max_vcores = new Sample((double) jobDef.reduce_max_vcores_avg,
            jobDef.reduce_max_vcores_stddev);
        reduce.priority = DEFAULT_REDUCER_PRIORITY;
        reduce.executionType = jobDef.reduce_execution_type;

        jobDef.tasks.add(reduce);
      } catch (JsonMappingException e) {
        LOG.warn("Error converting old JobDefinition format", e);
      }
    }
  }

  public long getSeed() {
    return seed;
  }

  public int getNodesPerRack() {
    return trace.nodes_per_rack < 1 ? 1: trace.nodes_per_rack;
  }

  public int getNumNodes() {
    return trace.num_nodes;
  }

  /**
   * Class used to parse a trace configuration file.
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  @XmlRootElement
  public static class Trace {
    @JsonProperty("description")
    String description;
    @JsonProperty("num_nodes")
    int num_nodes;
    @JsonProperty("nodes_per_rack")
    int nodes_per_rack;
    @JsonProperty("num_jobs")
    int num_jobs;

    // in sec (selects a portion of time_distribution
    @JsonProperty("rand_seed")
    long rand_seed;
    @JsonProperty("workloads")
    List<Workload> workloads;

    List<Double> workload_weights;
    JDKRandomGenerator rand;

    public void init(JDKRandomGenerator random){
      this.rand = random;
      // Pass rand forward
      for(Workload w : workloads){
        w.init(rand);
      }
      // Initialize workload weights
      workload_weights = new ArrayList<>();
      for(Workload w : workloads){
        workload_weights.add(w.workload_weight);
      }
    }

    Workload generateWorkload(){
      return workloads.get(SynthUtils.getWeighted(workload_weights, rand));
    }
  }

  /**
   * Class used to parse a workload from file.
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  public static class Workload {
    @JsonProperty("workload_name")
    String workload_name;
    // used to change probability this workload is picked for each job
    @JsonProperty("workload_weight")
    double workload_weight;
    @JsonProperty("queue_name")
    String queue_name;
    @JsonProperty("job_classes")
    List<JobDefinition> job_classes;
    @JsonProperty("time_distribution")
    List<TimeSample> time_distribution;

    JDKRandomGenerator rand;

    List<Double> job_weights;
    List<Double> time_weights;

    public void init(JDKRandomGenerator random){
      this.rand = random;
      // Validate and pass rand forward
      for(JobDefinition def : job_classes){
        validateJobDef(def);
        def.init(rand);
      }

      // Initialize job weights
      job_weights = new ArrayList<>();
      job_weights = new ArrayList<>();
      for(JobDefinition j : job_classes){
        job_weights.add(j.class_weight);
      }

      // Initialize time weights
      time_weights = new ArrayList<>();
      for(TimeSample ts : time_distribution){
        time_weights.add(ts.weight);
      }
    }

    public long generateSubmissionTime(){
      int index = SynthUtils.getWeighted(time_weights, rand);
      // Retrieve the lower and upper bounds for this time "bucket"
      int start = time_distribution.get(index).time;
      // Get the beginning of the next time sample (if it exists)
      index = (index+1)<time_distribution.size() ? index+1 : index;
      int end = time_distribution.get(index).time;
      int range = end-start;
      // Within this time "bucket", uniformly pick a time if our
      // range is non-zero, otherwise just use the start time of the bucket
      return start + (range>0 ? rand.nextInt(range) : 0);
    }

    public JobDefinition generateJobDefinition(){
      return job_classes.get(SynthUtils.getWeighted(job_weights, rand));
    }

    @Override
    public String toString(){
      return "\nWorkload " + workload_name + ", weight: " + workload_weight
          + ", queue: " + queue_name + " "
          + job_classes.toString().replace("\n", "\n\t");
    }
  }

  /**
   * Class used to parse a job class from file.
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  public static class JobDefinition {

    @JsonProperty("class_name")
    String class_name;
    @JsonProperty("user_name")
    String user_name;

    // used to change probability this class is chosen
    @JsonProperty("class_weight")
    double class_weight;

    // am type to launch
    @JsonProperty("type")
    String type;
    @JsonProperty("deadline_factor")
    Sample deadline_factor;
    @JsonProperty("duration")
    Sample duration;
    @JsonProperty("reservation")
    Sample reservation;

    @JsonProperty("tasks")
    List<TaskDefinition> tasks;

    @JsonProperty("params")
    Map<String, String> params;

    // Old JSON fields for backwards compatibility
    // reservation related params
    @JsonProperty("chance_of_reservation")
    double chance_of_reservation;
    @JsonProperty("deadline_factor_avg")
    double deadline_factor_avg;
    @JsonProperty("deadline_factor_stddev")
    double deadline_factor_stddev;

    // durations in sec
    @JsonProperty("dur_avg")
    double dur_avg;
    @JsonProperty("dur_stddev")
    double dur_stddev;
    @JsonProperty("mtime_avg")
    double mtime_avg;
    @JsonProperty("mtime_stddev")
    double mtime_stddev;
    @JsonProperty("rtime_avg")
    double rtime_avg;
    @JsonProperty("rtime_stddev")
    double rtime_stddev;

    // number of tasks
    @JsonProperty("mtasks_avg")
    double mtasks_avg;
    @JsonProperty("mtasks_stddev")
    double mtasks_stddev;
    @JsonProperty("rtasks_avg")
    double rtasks_avg;
    @JsonProperty("rtasks_stddev")
    double rtasks_stddev;

    // memory in MB
    @JsonProperty("map_max_memory_avg")
    long map_max_memory_avg;
    @JsonProperty("map_max_memory_stddev")
    double map_max_memory_stddev;
    @JsonProperty("reduce_max_memory_avg")
    long reduce_max_memory_avg;
    @JsonProperty("reduce_max_memory_stddev")
    double reduce_max_memory_stddev;

    // vcores
    @JsonProperty("map_max_vcores_avg")
    long map_max_vcores_avg;
    @JsonProperty("map_max_vcores_stddev")
    double map_max_vcores_stddev;
    @JsonProperty("reduce_max_vcores_avg")
    long reduce_max_vcores_avg;
    @JsonProperty("reduce_max_vcores_stddev")
    double reduce_max_vcores_stddev;

    //container execution type
    @JsonProperty("map_execution_type")
    String map_execution_type = ExecutionType.GUARANTEED.name();
    @JsonProperty("reduce_execution_type")
    String reduce_execution_type = ExecutionType.GUARANTEED.name();

    public void init(JDKRandomGenerator rand){
      deadline_factor.init(rand);
      duration.init(rand);
      reservation.init(rand);

      for(TaskDefinition t : tasks){
        t.count.init(rand);
        t.time.init(rand);
        t.max_memory.init(rand);
        t.max_vcores.init(rand);
      }
    }

    @Override
    public String toString(){
      return "\nJobDefinition " + class_name + ", weight: " + class_weight
          + ", type: " + type + " "
          + tasks.toString().replace("\n", "\n\t");
    }
  }

  /**
   * A task representing a type of container - e.g. "map" in mapreduce
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  public static class TaskDefinition {

    @JsonProperty("type")
    String type;
    @JsonProperty("count")
    Sample count;
    @JsonProperty("time")
    Sample time;
    @JsonProperty("max_memory")
    Sample max_memory;
    @JsonProperty("max_vcores")
    Sample max_vcores;
    @JsonProperty("priority")
    int priority;
    @JsonProperty("execution_type")
    String executionType = ExecutionType.GUARANTEED.name();

    @Override
    public String toString(){
      return "\nTaskDefinition " + type
          + " Count[" + count + "] Time[" + time + "] Memory[" + max_memory
          + "] Vcores[" + max_vcores + "] Priority[" + priority
          + "] ExecutionType[" + executionType + "]";
    }
  }

  /**
   * Class used to parse value sample information.
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  public static class Sample {
    private static final Dist DEFAULT_DIST = Dist.LOGNORM;

    private final double val;
    private final double std;
    private final Dist dist;
    private AbstractRealDistribution dist_instance;
    private final List<String> discrete;
    private final List<Double> weights;
    private final Mode mode;

    private JDKRandomGenerator rand;

    private enum Mode{
      CONST,
      DIST,
      DISC
    }

    private enum Dist{
      LOGNORM,
      NORM
    }

    public Sample(Double val) throws JsonMappingException{
      this(val, null);
    }

    public Sample(Double val, Double std) throws JsonMappingException{
      this(val, std, null, null, null);
    }

    @JsonCreator
    public Sample(@JsonProperty("val") Double val,
        @JsonProperty("std") Double std, @JsonProperty("dist") String dist,
        @JsonProperty("discrete") List<String> discrete,
        @JsonProperty("weights") List<Double> weights)
        throws JsonMappingException{
      // Different Modes
      // - Constant: val must be specified, all else null. Sampling will
      // return val.
      // - Distribution: val, std specified, dist optional (defaults to
      // LogNormal). Sampling will sample from the appropriate distribution
      // - Discrete: discrete must be set to a list of strings or numbers,
      // weights optional (defaults to uniform)

      if(val!=null){
        if(std==null){
          // Constant
          if(dist!=null || discrete!=null || weights!=null){
            throw new JsonMappingException("Instantiation of " + Sample.class
                + " failed");
          }
          mode = Mode.CONST;
          this.val = val;
          this.std = 0;
          this.dist = null;
          this.discrete = null;
          this.weights = null;
        } else {
          // Distribution
          if(discrete!=null || weights != null){
            throw new JsonMappingException("Instantiation of " + Sample.class
                + " failed");
          }
          mode = Mode.DIST;
          this.val = val;
          this.std = std;
          this.dist = dist!=null ? Dist.valueOf(dist) : DEFAULT_DIST;
          this.discrete = null;
          this.weights = null;
        }
      } else {
        // Discrete
        if(discrete==null){
          throw new JsonMappingException("Instantiation of " + Sample.class
              + " failed");
        }
        mode = Mode.DISC;
        this.val = 0;
        this.std = 0;
        this.dist = null;
        this.discrete = discrete;
        if(weights == null){
          weights = new ArrayList<>(Collections.nCopies(
              discrete.size(), 1.0));
        }
        if(weights.size() != discrete.size()){
          throw new JsonMappingException("Instantiation of " + Sample.class
              + " failed");
        }
        this.weights = weights;
      }
    }

    public void init(JDKRandomGenerator random){
      if(this.rand != null){
        throw new YarnRuntimeException("init called twice");
      }
      this.rand = random;
      if(mode == Mode.DIST){
        switch(this.dist){
        case LOGNORM:
          this.dist_instance = SynthUtils.getLogNormalDist(rand, val, std);
          return;
        case NORM:
          this.dist_instance = SynthUtils.getNormalDist(rand, val, std);
          return;
        default:
          throw new YarnRuntimeException("Unknown distribution " + dist.name());
        }
      }
    }

    public int getInt(){
      return Math.toIntExact(getLong());
    }

    public long getLong(){
      return Math.round(getDouble());
    }

    public double getDouble(){
      return Double.parseDouble(getString());
    }

    public String getString(){
      if(this.rand == null){
        throw new YarnRuntimeException("getValue called without init");
      }
      switch(mode){
      case CONST:
        return Double.toString(val);
      case DIST:
        return Double.toString(dist_instance.sample());
      case DISC:
        return this.discrete.get(SynthUtils.getWeighted(this.weights, rand));
      default:
        throw new YarnRuntimeException("Unknown sampling mode " + mode.name());
      }
    }

    @Override
    public String toString(){
      switch(mode){
      case CONST:
        return "value: " + Double.toString(val);
      case DIST:
        return "value: " + this.val + " std: " + this.std + " dist: "
            + this.dist.name();
      case DISC:
        return "discrete: " + this.discrete + ", weights: " + this.weights;
      default:
        throw new YarnRuntimeException("Unknown sampling mode " + mode.name());
      }
    }

  }

  /**
   * This is used to define time-varying probability of a job start-time (e.g.,
   * to simulate daily patterns).
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  public static class TimeSample {
    // in sec
    @JsonProperty("time")
    int time;
    @JsonProperty("weight")
    double weight;
  }
}
