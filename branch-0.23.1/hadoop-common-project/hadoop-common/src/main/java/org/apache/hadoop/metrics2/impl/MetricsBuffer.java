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

/**
 * An immutable element for the sink queues.
 */
class MetricsBuffer implements Iterable<MetricsBuffer.Entry> {

  private final Iterable<Entry> mutable;

  MetricsBuffer(Iterable<MetricsBuffer.Entry> mutable) {
    this.mutable = mutable;
  }

  @Override
  public Iterator<Entry> iterator() {
    return mutable.iterator();
  }

  static class Entry {
    private final String sourceName;
    private final Iterable<MetricsRecordImpl> records;

    Entry(String name, Iterable<MetricsRecordImpl> records) {
      sourceName = name;
      this.records = records;
    }

    String name() {
      return sourceName;
    }

    Iterable<MetricsRecordImpl> records() {
      return records;
    }
  }

}
