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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsFilter;

class MetricsBuilderImpl extends ArrayList<MetricsRecordBuilderImpl>
                         implements MetricsBuilder {
  private static final long serialVersionUID = 1L;
  private MetricsFilter recordFilter, metricFilter;

  @Override
  public MetricsRecordBuilderImpl addRecord(String name) {
    boolean acceptable = recordFilter == null || recordFilter.accepts(name);
    MetricsRecordBuilderImpl rb =
        new MetricsRecordBuilderImpl(name, recordFilter, metricFilter,
                                     acceptable);
    if (acceptable) {
      add(rb);
    }
    return rb;
  }


  public List<MetricsRecordImpl> getRecords() {
    List<MetricsRecordImpl> records =
        new ArrayList<MetricsRecordImpl>(size());
    for (MetricsRecordBuilderImpl rb : this) {
      MetricsRecordImpl mr = rb.getRecord();
      if (mr != null) {
        records.add(mr);
      }
    }
    return records;
  }

  MetricsBuilderImpl setRecordFilter(MetricsFilter rf) {
    recordFilter = rf;
    return this;
  }

  MetricsBuilderImpl setMetricFilter(MetricsFilter mf) {
    metricFilter = mf;
    return this;
  }

}
