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

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer.JobClass;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer.Trace;

/**
 * This is a class that represent a class of Jobs. It is used to generate an
 * individual job, by picking random durations, task counts, container size,
 * etc.
 */
public class SynthJobClass {

  private final JDKRandomGenerator rand;
  private final LogNormalDistribution dur;
  private final LogNormalDistribution mapRuntime;
  private final LogNormalDistribution redRuntime;
  private final LogNormalDistribution mtasks;
  private final LogNormalDistribution rtasks;
  private final LogNormalDistribution mapMem;
  private final LogNormalDistribution redMem;
  private final LogNormalDistribution mapVcores;
  private final LogNormalDistribution redVcores;

  private final Trace trace;
  @SuppressWarnings("VisibilityModifier")
  protected final SynthWorkload workload;
  @SuppressWarnings("VisibilityModifier")
  protected final JobClass jobClass;

  public SynthJobClass(JDKRandomGenerator rand, Trace trace,
      SynthWorkload workload, int classId) {

    this.trace = trace;
    this.workload = workload;
    this.rand = new JDKRandomGenerator();
    this.rand.setSeed(rand.nextLong());
    jobClass = trace.workloads.get(workload.getId()).job_classes.get(classId);

    this.dur = SynthUtils.getLogNormalDist(rand, jobClass.dur_avg,
        jobClass.dur_stddev);
    this.mapRuntime = SynthUtils.getLogNormalDist(rand, jobClass.mtime_avg,
        jobClass.mtime_stddev);
    this.redRuntime = SynthUtils.getLogNormalDist(rand, jobClass.rtime_avg,
        jobClass.rtime_stddev);
    this.mtasks = SynthUtils.getLogNormalDist(rand, jobClass.mtasks_avg,
        jobClass.mtasks_stddev);
    this.rtasks = SynthUtils.getLogNormalDist(rand, jobClass.rtasks_avg,
        jobClass.rtasks_stddev);

    this.mapMem = SynthUtils.getLogNormalDist(rand, jobClass.map_max_memory_avg,
        jobClass.map_max_memory_stddev);
    this.redMem = SynthUtils.getLogNormalDist(rand,
        jobClass.reduce_max_memory_avg, jobClass.reduce_max_memory_stddev);
    this.mapVcores = SynthUtils.getLogNormalDist(rand,
        jobClass.map_max_vcores_avg, jobClass.map_max_vcores_stddev);
    this.redVcores = SynthUtils.getLogNormalDist(rand,
        jobClass.reduce_max_vcores_avg, jobClass.reduce_max_vcores_stddev);
  }

  public JobStory getJobStory(Configuration conf, long actualSubmissionTime) {
    return new SynthJob(rand, conf, this, actualSubmissionTime);
  }

  @Override
  public String toString() {
    return "SynthJobClass [workload=" + workload.getName() + ", class="
        + jobClass.class_name + " job_count=" + jobClass.class_weight + ", dur="
        + ((dur != null) ? dur.getNumericalMean() : 0) + ", mapRuntime="
        + ((mapRuntime != null) ? mapRuntime.getNumericalMean() : 0)
        + ", redRuntime="
        + ((redRuntime != null) ? redRuntime.getNumericalMean() : 0)
        + ", mtasks=" + ((mtasks != null) ? mtasks.getNumericalMean() : 0)
        + ", rtasks=" + ((rtasks != null) ? rtasks.getNumericalMean() : 0)
        + ", chance_of_reservation=" + jobClass.chance_of_reservation + "]\n";

  }

  public double getClassWeight() {
    return jobClass.class_weight;
  }

  public long getDur() {
    return genLongSample(dur);
  }

  public int getMtasks() {
    return genIntSample(mtasks);
  }

  public int getRtasks() {
    return genIntSample(rtasks);
  }

  public long getMapMaxMemory() {
    return genLongSample(mapMem);
  }

  public long getReduceMaxMemory() {
    return genLongSample(redMem);
  }

  public long getMapMaxVcores() {
    return genLongSample(mapVcores);
  }

  public long getReduceMaxVcores() {
    return genLongSample(redVcores);
  }

  public SynthWorkload getWorkload() {
    return workload;
  }

  public int genIntSample(AbstractRealDistribution dist) {
    if (dist == null) {
      return 0;
    }
    double baseSample = dist.sample();
    if (baseSample < 0) {
      baseSample = 0;
    }
    return (int) (Integer.MAX_VALUE & (long) Math.ceil(baseSample));
  }

  public long genLongSample(AbstractRealDistribution dist) {
    return dist != null ? (long) Math.ceil(dist.sample()) : 0;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SynthJobClass)) {
      return false;
    }
    SynthJobClass o = (SynthJobClass) other;
    return workload.equals(o.workload);
  }

  @Override
  public int hashCode() {
    return workload.hashCode() * workload.getId();
  }

  public String getClassName() {
    return jobClass.class_name;
  }

  public long getMapTimeSample() {
    return genLongSample(mapRuntime);
  }

  public long getReduceTimeSample() {
    return genLongSample(redRuntime);
  }

  public String getUserName() {
    return jobClass.user_name;
  }
}
