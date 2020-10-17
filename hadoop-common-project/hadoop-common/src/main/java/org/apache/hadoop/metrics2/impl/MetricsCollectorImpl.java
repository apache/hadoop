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

package org.apache.hadoop.metrics2.impl;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import static org.apache.hadoop.metrics2.lib.Interns.*;

@InterfaceAudience.Private
@VisibleForTesting
public class MetricsCollectorImpl implements MetricsCollector,
    Iterable<MetricsRecordBuilderImpl> {

  private final List<MetricsRecordBuilderImpl> rbs = Lists.newArrayList();
  private MetricsFilter recordFilter, metricFilter;

  @Override
  public MetricsRecordBuilderImpl addRecord(MetricsInfo info) {
    boolean acceptable = recordFilter == null ||
                         recordFilter.accepts(info.name());
    MetricsRecordBuilderImpl rb = new MetricsRecordBuilderImpl(this, info,
        recordFilter, metricFilter, acceptable);
    if (acceptable) rbs.add(rb);
    return rb;
  }

  @Override
  public MetricsRecordBuilderImpl addRecord(String name) {
    return addRecord(info(name, name +" record"));
  }

  public List<MetricsRecordImpl> getRecords() {
    List<MetricsRecordImpl> recs = Lists.newArrayListWithCapacity(rbs.size());
    for (MetricsRecordBuilderImpl rb : rbs) {
      MetricsRecordImpl mr = rb.getRecord();
      if (mr != null) {
        recs.add(mr);
      }
    }
    return recs;
  }

  @Override
  public Iterator<MetricsRecordBuilderImpl> iterator() {
    return rbs.iterator();
  }

  @InterfaceAudience.Private
  public void clear() { rbs.clear(); }

  MetricsCollectorImpl setRecordFilter(MetricsFilter rf) {
    recordFilter = rf;
    return this;
  }

  MetricsCollectorImpl setMetricFilter(MetricsFilter mf) {
    metricFilter = mf;
    return this;
  }
}
