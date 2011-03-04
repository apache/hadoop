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
import java.util.List;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

public class JhCounters {
  public String name;
  public List<JhCounterGroup> groups;
  
  JhCounters(Counters counters, String name) {
    this.name = name;
    this.groups = new ArrayList<JhCounterGroup>();
    if (counters == null) return;
    for (CounterGroup group : counters) {
      JhCounterGroup g = new JhCounterGroup();
      g.name = group.getName();
      g.displayName = group.getDisplayName();
      g.counts = new ArrayList<JhCounter>(group.size());
      for (Counter counter : group) {
        JhCounter c = new JhCounter();
        c.name = counter.getName();
        c.displayName = counter.getDisplayName();
        c.value = counter.getValue();
        g.counts.add(c);
      }
      this.groups.add(g);
    }
  }

}
