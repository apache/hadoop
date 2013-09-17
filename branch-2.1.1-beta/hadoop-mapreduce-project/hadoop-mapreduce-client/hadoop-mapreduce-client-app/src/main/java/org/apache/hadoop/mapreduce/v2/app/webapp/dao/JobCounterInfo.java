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

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
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
  protected ArrayList<CounterGroupInfo> counterGroup;

  public JobCounterInfo() {
  }

  public JobCounterInfo(AppContext ctx, Job job) {
    getCounters(ctx, job);
    counterGroup = new ArrayList<CounterGroupInfo>();
    this.id = MRApps.toString(job.getID());

    if (total != null) {
      for (CounterGroup g : total) {
        if (g != null) {
          CounterGroup mg = map == null ? null : map.getGroup(g.getName());
          CounterGroup rg = reduce == null ? null : reduce
            .getGroup(g.getName());

          CounterGroupInfo cginfo = new CounterGroupInfo(g.getName(), g,
            mg, rg);
          counterGroup.add(cginfo);
        }
      }
    }
  }

  private void getCounters(AppContext ctx, Job job) {
    if (job == null) {
      return;
    }
    total = job.getAllCounters();
    boolean needTotalCounters = false;
    if (total == null) {
      total = new Counters();
      needTotalCounters = true;
    }
    map = new Counters();
    reduce = new Counters();
    // Get all types of counters
    Map<TaskId, Task> tasks = job.getTasks();
    for (Task t : tasks.values()) {
      Counters counters = t.getCounters();
      if (counters == null) {
        continue;
      }
      switch (t.getType()) {
      case MAP:
        map.incrAllCounters(counters);
        break;
      case REDUCE:
        reduce.incrAllCounters(counters);
        break;
      }
      if (needTotalCounters) {
        total.incrAllCounters(counters);
      }
    }
  }

}
