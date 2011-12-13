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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import java.util.ArrayList;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.util.MRApps;

@XmlRootElement(name = "jobCounters")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobCounterInfo {

  @XmlTransient
  protected Counters total = null;
  @XmlTransient
  protected Counters map = null;
  @XmlTransient
  protected Counters reduce = null;

  protected String id;
  protected ArrayList<CounterGroupInfo> counterGroups;

  public JobCounterInfo() {
  }

  public JobCounterInfo(AppContext ctx, Job job) {
    getCounters(ctx, job);
    counterGroups = new ArrayList<CounterGroupInfo>();
    this.id = MRApps.toString(job.getID());

    int numGroups = 0;

    if (total != null) {
      for (CounterGroup g : total.getAllCounterGroups().values()) {
        if (g != null) {
          CounterGroup mg = map == null ? null : map.getCounterGroup(g
              .getName());
          CounterGroup rg = reduce == null ? null : reduce.getCounterGroup(g
              .getName());
          ++numGroups;

          CounterGroupInfo cginfo = new CounterGroupInfo(g.getName(), g, mg, rg);
          counterGroups.add(cginfo);
        }
      }
    }
  }

  private void getCounters(AppContext ctx, Job job) {
    total = JobImpl.newCounters();
    if (job == null) {
      return;
    }
    map = JobImpl.newCounters();
    reduce = JobImpl.newCounters();
    // Get all types of counters
    Map<TaskId, Task> tasks = job.getTasks();
    for (Task t : tasks.values()) {
      Counters counters = t.getCounters();
      JobImpl.incrAllCounters(total, counters);
      switch (t.getType()) {
      case MAP:
        JobImpl.incrAllCounters(map, counters);
        break;
      case REDUCE:
        JobImpl.incrAllCounters(reduce, counters);
        break;
      }
    }
  }

}
