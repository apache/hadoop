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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

/**
 * An abstract class to provide common implementation for the framework
 * counter group in both mapred and mapreduce packages.
 *
 * @param <T> type of the counter enum class
 * @param <C> type of the counter
 */
@InterfaceAudience.Private
public abstract class FrameworkCounterGroup<T extends Enum<T>,
    C extends Counter> implements CounterGroupBase<C> {
  private static final Logger LOG =
      LoggerFactory.getLogger(FrameworkCounterGroup.class);
  
  private final Class<T> enumClass; // for Enum.valueOf
  private final Object[] counters;  // local casts are OK and save a class ref
  private String displayName = null;

  /**
   * A counter facade for framework counters.
   * Use old (which extends new) interface to make compatibility easier.
   */
  @InterfaceAudience.Private
  public static class FrameworkCounter<T extends Enum<T>> extends AbstractCounter {
    final T key;
    final String groupName;
    private long value;

    public FrameworkCounter(T ref, String groupName) {
      key = ref;
      this.groupName = groupName;
    }
    
    @Private
    public T getKey() {
      return key;
    }

    @Private
    public String getGroupName() {
      return groupName;
    }
    
    @Override
    public String getName() {
      return key.name();
    }

    @Override
    public String getDisplayName() {
      return ResourceBundles.getCounterName(groupName, getName(), getName());
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public void setValue(long value) {
      this.value = value;
    }

    @Override
    public void increment(long incr) {
      if (key.name().endsWith("_MAX")) {
        value = value > incr ? value : incr;
      } else {
        value += incr;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public Counter getUnderlyingCounter() {
      return this;
    }
  }

  @SuppressWarnings("unchecked")
  public FrameworkCounterGroup(Class<T> enumClass) {
    this.enumClass = enumClass;
    T[] enums = enumClass.getEnumConstants();
    counters = new Object[enums.length];
  }

  @Override
  public String getName() {
    return enumClass.getName();
  }

  @Override
  public String getDisplayName() {
    if (displayName == null) {
      displayName = ResourceBundles.getCounterGroupName(getName(), getName());
    }
    return displayName;
  }

  @Override
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  private T valueOf(String name) {
    return Enum.valueOf(enumClass, name);
  }

  @Override
  public void addCounter(C counter) {
    C ours = findCounter(counter.getName());
    if (ours != null) {
      ours.setValue(counter.getValue());
    } else {
      LOG.warn(counter.getName() + "is not a known counter.");
    }
  }

  @Override
  public C addCounter(String name, String displayName, long value) {
    C counter = findCounter(name);
    if (counter != null) {
      counter.setValue(value);
    } else {
      LOG.warn(name + "is not a known counter.");
    }
    return counter;
  }

  @Override
  public C findCounter(String counterName, String displayName) {
    return findCounter(counterName);
  }

  @Override
  public C findCounter(String counterName, boolean create) {
    try {
      return findCounter(valueOf(counterName));
    }
    catch (Exception e) {
      if (create) throw new IllegalArgumentException(e);
      return null;
    }
  }

  @Override
  public C findCounter(String counterName) {
    try {
      T enumValue = valueOf(counterName);
      return findCounter(enumValue);
    } catch (IllegalArgumentException e) {
      LOG.warn(counterName + " is not a recognized counter.");
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private C findCounter(T key) {
    int i = key.ordinal();
    if (counters[i] == null) {
      counters[i] = newCounter(key);
    }
    return (C) counters[i];
  }

  /**
   * Abstract factory method for new framework counter
   * @param key for the enum value of a counter
   * @return a new counter for the key
   */
  protected abstract C newCounter(T key);

  @Override
  public int size() {
    int n = 0;
    for (int i = 0; i < counters.length; ++i) {
      if (counters[i] != null) ++n;
    }
    return n;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void incrAllCounters(CounterGroupBase<C> other) {
    if (checkNotNull(other, "other counter group")
        instanceof FrameworkCounterGroup<?, ?>) {
      for (Counter counter : other) {
        C c = findCounter(((FrameworkCounter) counter).key.name());
        if (c != null) {
          c.increment(counter.getValue());
        }
      }
    }
  }

  /**
   * FrameworkGroup ::= #counter (key value)*
   */
  @Override
  @SuppressWarnings("unchecked")
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, size());
    for (int i = 0; i < counters.length; ++i) {
      Counter counter = (C) counters[i];
      if (counter != null) {
        WritableUtils.writeVInt(out, i);
        WritableUtils.writeVLong(out, counter.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    int len = WritableUtils.readVInt(in);
    T[] enums = enumClass.getEnumConstants();
    for (int i = 0; i < len; ++i) {
      int ord = WritableUtils.readVInt(in);
      Counter counter = newCounter(enums[ord]);
      counter.setValue(WritableUtils.readVLong(in));
      counters[ord] = counter;
    }
  }

  private void clear() {
    for (int i = 0; i < counters.length; ++i) {
      counters[i] = null;
    }
  }

  @Override
  public Iterator<C> iterator() {
    return new AbstractIterator<C>() {
      int i = 0;
      @Override
      protected C computeNext() {
        while (i < counters.length) {
          @SuppressWarnings("unchecked")
          C counter = (C) counters[i++];
          if (counter != null) return counter;
        }
        return endOfData();
      }
    };
  }

  @Override
  public boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      CounterGroupBase<C> right = (CounterGroupBase<C>) genericRight;
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    // need to be deep as counters is an array
    return Arrays.deepHashCode(new Object[]{enumClass, counters, displayName});
  }
}
