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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.codehaus.jackson.JsonParser.Feature.INTERN_FIELD_NAMES;
import static org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * This is a JobStoryProducer that operates from distribution of different
 * workloads. The .json input file is used to determine how many jobs, which
 * size, number of maps/reducers and their duration, as well as the temporal
 * distributed of submissions. For each parameter we control avg and stdev, and
 * generate values via normal or log-normal distributions.
 */
public class SynthTraceJobProducer implements JobStoryProducer {

  @SuppressWarnings("StaticVariableName")
  private static final Log LOG = LogFactory.getLog(SynthTraceJobProducer.class);

  private final Configuration conf;
  private final AtomicInteger numJobs;
  private final Trace trace;
  private final long seed;

  private int totalWeight;
  private final List<Double> weightList;
  private final Map<Integer, SynthWorkload> workloads;

  private final Queue<StoryParams> listStoryParams;

  private final JDKRandomGenerator rand;

  public static final String SLS_SYNTHETIC_TRACE_FILE =
      "sls.synthetic" + ".trace_file";

  public SynthTraceJobProducer(Configuration conf) throws IOException {
    this(conf, new Path(conf.get(SLS_SYNTHETIC_TRACE_FILE)));
  }

  public SynthTraceJobProducer(Configuration conf, Path path)
      throws IOException {

    LOG.info("SynthTraceJobProducer");

    this.conf = conf;
    this.rand = new JDKRandomGenerator();
    workloads = new HashMap<Integer, SynthWorkload>();
    weightList = new ArrayList<Double>();

    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(INTERN_FIELD_NAMES, true);
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    FileSystem ifs = path.getFileSystem(conf);
    FSDataInputStream fileIn = ifs.open(path);

    this.trace = mapper.readValue(fileIn, Trace.class);
    seed = trace.rand_seed;
    rand.setSeed(seed);

    this.numJobs = new AtomicInteger(trace.num_jobs);

    for (int workloadId = 0; workloadId < trace.workloads
        .size(); workloadId++) {
      SynthWorkload workload = new SynthWorkload(workloadId, trace);
      for (int classId =
          0; classId < trace.workloads.get(workloadId).job_classes
              .size(); classId++) {
        SynthJobClass cls = new SynthJobClass(rand, trace, workload, classId);
        workload.add(cls);
      }
      workloads.put(workloadId, workload);
    }

    for (int i = 0; i < workloads.size(); i++) {
      double w = workloads.get(i).getWorkloadWeight();
      totalWeight += w;
      weightList.add(w);
    }

    // create priority queue to keep start-time sorted
    listStoryParams =
        new PriorityQueue<>(10, new Comparator<StoryParams>() {
          @Override
          public int compare(StoryParams o1, StoryParams o2) {
            long value = o2.actualSubmissionTime - o1.actualSubmissionTime;
            if ((int)value != value) {
              throw new ArithmeticException("integer overflow");
            }
            return (int)value;
          }
        });

    // initialize it
    createStoryParams();
    LOG.info("Generated " + listStoryParams.size() + " deadlines for "
        + this.numJobs.get() + " jobs ");
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
    List<JobClass> job_classes;
    @JsonProperty("time_distribution")
    List<TimeSample> time_distribution;
  }

  /**
   * Class used to parse a job class from file.
   */
  @SuppressWarnings({ "membername", "checkstyle:visibilitymodifier" })
  public static class JobClass {

    @JsonProperty("class_name")
    String class_name;
    @JsonProperty("user_name")
    String user_name;

    // used to change probability this class is chosen
    @JsonProperty("class_weight")
    double class_weight;

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
    double jobs;
  }

  static class StoryParams {
    private SynthJobClass pickedJobClass;
    private long actualSubmissionTime;

    StoryParams(SynthJobClass pickedJobClass, long actualSubmissionTime) {
      this.pickedJobClass = pickedJobClass;
      this.actualSubmissionTime = actualSubmissionTime;
    }
  }


  void createStoryParams() {

    for (int i = 0; i < numJobs.get(); i++) {
      int workload = SynthUtils.getWeighted(weightList, rand);
      SynthWorkload pickedWorkload = workloads.get(workload);
      long jobClass =
          SynthUtils.getWeighted(pickedWorkload.getWeightList(), rand);
      SynthJobClass pickedJobClass =
          pickedWorkload.getClassList().get((int) jobClass);
      long actualSubmissionTime = pickedWorkload.getBaseSubmissionTime(rand);
      // long actualSubmissionTime = (i + 1) * 10;
      listStoryParams
          .add(new StoryParams(pickedJobClass, actualSubmissionTime));
    }
  }

  @Override
  public JobStory getNextJob() throws IOException {
    if (numJobs.decrementAndGet() < 0) {
      return null;
    }
    StoryParams storyParams = listStoryParams.poll();
    return storyParams.pickedJobClass.getJobStory(conf,
        storyParams.actualSubmissionTime);
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return "SynthTraceJobProducer [ conf=" + conf + ", numJobs=" + numJobs
        + ", weightList=" + weightList + ", r=" + rand + ", totalWeight="
        + totalWeight + ", workloads=" + workloads + "]";
  }

  public int getNumJobs() {
    return trace.num_jobs;
  }

}
