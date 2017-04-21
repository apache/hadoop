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

import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer.Trace;

import java.util.*;

/**
 * This class represent a workload (made up of multiple SynthJobClass(es)). It
 * also stores the temporal distributions of jobs in this workload.
 */
public class SynthWorkload {

  private final int id;
  private final List<SynthJobClass> classList;
  private final Trace trace;
  private final SortedMap<Integer, Double> timeWeights;

  public SynthWorkload(int identifier, Trace inTrace) {
    classList = new ArrayList<SynthJobClass>();
    this.id = identifier;
    this.trace = inTrace;
    timeWeights = new TreeMap<Integer, Double>();
    for (SynthTraceJobProducer.TimeSample ts : trace.workloads
        .get(id).time_distribution) {
      timeWeights.put(ts.time, ts.jobs);
    }
  }

  public boolean add(SynthJobClass s) {
    return classList.add(s);
  }

  public List<Double> getWeightList() {
    ArrayList<Double> ret = new ArrayList<Double>();
    for (SynthJobClass s : classList) {
      ret.add(s.getClassWeight());
    }
    return ret;
  }

  public int getId() {
    return id;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SynthWorkload)) {
      return false;
    }
    // assume ID determines job classes by construction
    return getId() == ((SynthWorkload) other).getId();
  }

  @Override
  public int hashCode() {
    return getId();
  }

  @Override
  public String toString() {
    return "SynthWorkload " + trace.workloads.get(id).workload_name + "[\n"
        + classList + "]\n";
  }

  public String getName() {
    return trace.workloads.get(id).workload_name;
  }

  public double getWorkloadWeight() {
    return trace.workloads.get(id).workload_weight;
  }

  public String getQueueName() {
    return trace.workloads.get(id).queue_name;
  }

  public long getBaseSubmissionTime(Random rand) {

    // pick based on weights the "bucket" for this start time
    int position = SynthUtils.getWeighted(timeWeights.values(), rand);

    int[] time = new int[timeWeights.keySet().size()];
    int index = 0;
    for (Integer i : timeWeights.keySet()) {
      time[index++] = i;
    }

    // uniformly pick a time between start and end time of this bucket
    int startRange = time[position];
    int endRange = startRange;
    // if there is no subsequent bucket pick startRange
    if (position < timeWeights.keySet().size() - 1) {
      endRange = time[position + 1];
      return startRange + rand.nextInt((endRange - startRange));
    } else {
      return startRange;
    }
  }

  public List<SynthJobClass> getClassList() {
    return classList;
  }

}
