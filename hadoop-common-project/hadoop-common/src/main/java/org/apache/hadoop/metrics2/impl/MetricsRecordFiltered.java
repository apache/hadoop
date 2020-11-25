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
import java.util.Collection;

import org.apache.hadoop.thirdparty.com.google.common.collect.AbstractIterator;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

class MetricsRecordFiltered extends AbstractMetricsRecord {
  private final MetricsRecord delegate;
  private final MetricsFilter filter;

  MetricsRecordFiltered(MetricsRecord delegate, MetricsFilter filter) {
    this.delegate = delegate;
    this.filter = filter;
  }

  @Override public long timestamp() {
    return delegate.timestamp();
  }

  @Override public String name() {
    return delegate.name();
  }

  @Override public String description() {
    return delegate.description();
  }

  @Override public String context() {
    return delegate.context();
  }

  @Override public Collection<MetricsTag> tags() {
    return delegate.tags();
  }

  @Override public Iterable<AbstractMetric> metrics() {
    return new Iterable<AbstractMetric>() {
      final Iterator<AbstractMetric> it = delegate.metrics().iterator();
      @Override public Iterator<AbstractMetric> iterator() {
        return new AbstractIterator<AbstractMetric>() {
          @Override public AbstractMetric computeNext() {
            while (it.hasNext()) {
              AbstractMetric next = it.next();
              if (filter.accepts(next.name())) {
                return next;
              }
            }
            return endOfData();
          }
        };
      }
    };
  }
}
