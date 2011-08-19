/*
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

package org.apache.hadoop.mapreduce.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;

/**
 * An abstract class to provide common implementation of the
 * generic counter group in both mapred and mapreduce package.
 *
 * @param <T> type of the counter for the group
 */
@InterfaceAudience.Private
public abstract class AbstractCounterGroup<T extends Counter>
    implements CounterGroupBase<T> {

  private final String name;
  private String displayName;
  private final Map<String, T> counters = Maps.newTreeMap();
  private final Limits limits;

  public AbstractCounterGroup(String name, String displayName,
                              Limits limits) {
    this.name = name;
    this.displayName = displayName;
    this.limits = limits;
  }

  @Override
  public synchronized String getName() {
    return name;
  }

  @Override
  public synchronized String getDisplayName() {
    return displayName;
  }

  @Override
  public synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  @Override
  public synchronized void addCounter(T counter) {
    counters.put(counter.getName(), counter);
    limits.incrCounters();
  }

  @Override
  public synchronized T addCounter(String counterName, String displayName,
                                   long value) {
    String saveName = limits.filterCounterName(counterName);
    T counter = findCounterImpl(saveName, false);
    if (counter == null) {
      return addCounterImpl(saveName, displayName, value);
    }
    counter.setValue(value);
    return counter;
  }

  private T addCounterImpl(String name, String displayName, long value) {
    T counter = newCounter(name, displayName, value);
    addCounter(counter);
    return counter;
  }

  @Override
  public T findCounter(String counterName, String displayName) {
    String saveName = limits.filterCounterName(counterName);
    T counter = findCounterImpl(saveName, false);
    if (counter == null) {
      return addCounterImpl(saveName, displayName, 0);
    }
    return counter;
  }

  @Override
  public synchronized T findCounter(String counterName, boolean create) {
    return findCounterImpl(limits.filterCounterName(counterName), create);
  }

  private T findCounterImpl(String counterName, boolean create) {
    T counter = counters.get(counterName);
    if (counter == null && create) {
      String localized =
          ResourceBundles.getCounterName(getName(), counterName, counterName);
      return addCounterImpl(counterName, localized, 0);
    }
    return counter;
  }

  @Override
  public T findCounter(String counterName) {
    return findCounter(counterName, true);
  }

  /**
   * Abstract factory method to create a new counter of type T
   * @param counterName of the counter
   * @param displayName of the counter
   * @param value of the counter
   * @return a new counter
   */
  protected abstract T newCounter(String counterName, String displayName,
                                  long value);

  /**
   * Abstract factory method to create a new counter of type T
   * @return a new counter object
   */
  protected abstract T newCounter();

  @Override
  public synchronized Iterator<T> iterator() {
    return counters.values().iterator();
  }

  /**
   * GenericGroup ::= displayName #counter counter*
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    Text.writeString(out, displayName);
    WritableUtils.writeVInt(out, counters.size());
    for(Counter counter: counters.values()) {
      counter.write(out);
    }
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    displayName = Text.readString(in);
    counters.clear();
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      T counter = newCounter();
      counter.readFields(in);
      counters.put(counter.getName(), counter);
      limits.incrCounters();
    }
  }

  @Override
  public synchronized int size() {
    return counters.size();
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      CounterGroupBase<T> right = (CounterGroupBase<T>) genericRight;
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return counters.hashCode();
  }

  @Override
  public void incrAllCounters(CounterGroupBase<T> rightGroup) {
    try {
      for (Counter right : rightGroup) {
        Counter left = findCounter(right.getName(), right.getDisplayName());
        left.increment(right.getValue());
      }
    } catch (LimitExceededException e) {
      counters.clear();
      throw e;
    }
  }
}
