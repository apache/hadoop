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
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.util.TryIterator;

class MetricsRecordFiltered implements MetricsRecord {

  private final MetricsRecord delegate;
  private final MetricsFilter filter;

  MetricsRecordFiltered(MetricsRecord delegate, MetricsFilter filter) {
    this.delegate = delegate;
    this.filter = filter;
  }

  public long timestamp() {
    return delegate.timestamp();
  }

  public String name() {
    return delegate.name();
  }

  public String context() {
    return delegate.context();
  }

  public Iterable<MetricsTag> tags() {
    return delegate.tags();
  }

  public Iterable<Metric> metrics() {
    return new Iterable<Metric>() {
      final Iterator<Metric> it = delegate.metrics().iterator();
      public Iterator<Metric> iterator() {
        return new TryIterator<Metric>() {
          public Metric tryNext() {
            if (it.hasNext()) do {
              Metric next = it.next();
              if (filter.accepts(next.name())) {
                return next;
              }
            } while (it.hasNext());
            return done();
          }
        };
      }
    };
  }

}
