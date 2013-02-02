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

package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapreduce.util.CountersStrings.parseEscapedCompactString;
import static org.apache.hadoop.mapreduce.util.CountersStrings.toEscapedCompactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.counters.AbstractCounterGroup;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.apache.hadoop.mapreduce.counters.CounterGroupFactory;
import org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup;
import org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.util.CountersStrings;

import com.google.common.collect.Iterators;

/**
 * A set of named counters.
 *
 * <p><code>Counters</code> represent global counters, defined either by the
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 *
 * <p><code>Counters</code> are bunched into {@link Group}s, each comprising of
 * counters from a particular <code>Enum</code> class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Counters
    extends AbstractCounters<Counters.Counter, Counters.Group> {
  
  public static int MAX_COUNTER_LIMIT = Limits.COUNTERS_MAX;
  
  public Counters() {
    super(groupFactory);
  }

  public Counters(org.apache.hadoop.mapreduce.Counters newCounters) {
    super(newCounters, groupFactory);
  }

  /**
   * Downgrade new {@link org.apache.hadoop.mapreduce.Counters} to old Counters
   * @param newCounters new Counters
   * @return old Counters instance corresponding to newCounters
   */
  static Counters downgrade(org.apache.hadoop.mapreduce.Counters newCounters) {
    return new Counters(newCounters);
  }

  public synchronized Group getGroup(String groupName) {
    return super.getGroup(groupName);
  }

  @SuppressWarnings("unchecked")
  public synchronized Collection<String> getGroupNames() {
    return IteratorUtils.toList(super.getGroupNames().iterator());
  }

  public synchronized String makeCompactString() {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for(Group group: this){
      for(Counter counter: group) {
        if (first) {
          first = false;
        } else {
          builder.append(',');
        }
        builder.append(group.getDisplayName());
        builder.append('.');
        builder.append(counter.getDisplayName());
        builder.append(':');
        builder.append(counter.getCounter());
      }
    }
    return builder.toString();
  }
  
  /**
   * A counter record, comprising its name and value.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static class Counter implements org.apache.hadoop.mapreduce.Counter {
    org.apache.hadoop.mapreduce.Counter realCounter;

    Counter(org.apache.hadoop.mapreduce.Counter counter) {
      this.realCounter = counter;
    }

    public Counter() {
      this(new GenericCounter());
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setDisplayName(String displayName) {
      realCounter.setDisplayName(displayName);
    }

    @Override
    public String getName() {
      return realCounter.getName();
    }

    @Override
    public String getDisplayName() {
      return realCounter.getDisplayName();
    }

    @Override
    public long getValue() {
      return realCounter.getValue();
    }

    @Override
    public void setValue(long value) {
      realCounter.setValue(value);
    }

    @Override
    public void increment(long incr) {
      realCounter.increment(incr);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      realCounter.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      realCounter.readFields(in);
    }

    /**
     * Returns the compact stringified version of the counter in the format
     * [(actual-name)(display-name)(value)]
     * @return the stringified result
     */
    public String makeEscapedCompactString() {
      return toEscapedCompactString(realCounter);
    }

    /**
     * Checks for (content) equality of two (basic) counters
     * @param counter to compare
     * @return true if content equals
     * @deprecated
     */
    @Deprecated
    public boolean contentEquals(Counter counter) {
      return realCounter.equals(counter.getUnderlyingCounter());
    }

    /**
     * @return the value of the counter
     */
    public long getCounter() {
      return realCounter.getValue();
    }

    @Override
    public org.apache.hadoop.mapreduce.Counter getUnderlyingCounter() {
      return realCounter;
    }
    
    @Override
    public synchronized boolean equals(Object genericRight) {
      if (genericRight instanceof Counter) {
        synchronized (genericRight) {
          Counter right = (Counter) genericRight;
          return getName().equals(right.getName()) &&
                 getDisplayName().equals(right.getDisplayName()) &&
                 getValue() == right.getValue();
        }
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      return realCounter.hashCode();
    }
  }


  /**
   *  <code>Group</code> of counters, comprising of counters from a particular
   *  counter {@link Enum} class.
   *
   *  <p><code>Group</code>handles localization of the class name and the
   *  counter names.</p>
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static class Group implements CounterGroupBase<Counter> {
    private CounterGroupBase<Counter> realGroup;
    
    protected Group() {
      realGroup = null;
    }
    
    Group(GenericGroup group) {
      this.realGroup = group;
    }
    Group(FSGroupImpl group) {
      this.realGroup = group;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Group(FrameworkGroupImpl group) {
      this.realGroup = group;
    }
    
    /**
     * @param counterName the name of the counter
     * @return the value of the specified counter, or 0 if the counter does
     * not exist.
     */
    public long getCounter(String counterName)  {
      return getCounterValue(realGroup, counterName);
    }

    /**
     * @return the compact stringified version of the group in the format
     * {(actual-name)(display-name)(value)[][][]} where [] are compact strings
     * for the counters within.
     */
    public String makeEscapedCompactString() {
      return toEscapedCompactString(realGroup);
    }

    /**
     * Get the counter for the given id and create it if it doesn't exist.
     * @param id the numeric id of the counter within the group
     * @param name the internal counter name
     * @return the counter
     * @deprecated use {@link #findCounter(String)} instead
     */
    @Deprecated
    public Counter getCounter(int id, String name) {
      return findCounter(name);
    }

    /**
     * Get the counter for the given name and create it if it doesn't exist.
     * @param name the internal counter name
     * @return the counter
     */
    public Counter getCounterForName(String name) {
      return findCounter(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
     realGroup.write(out); 
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      realGroup.readFields(in);
    }

    @Override
    public Iterator<Counter> iterator() {
      return realGroup.iterator();
    }

    @Override
    public String getName() {
      return realGroup.getName();
    }

    @Override
    public String getDisplayName() {
      return realGroup.getDisplayName();
    }

    @Override
    public void setDisplayName(String displayName) {
      realGroup.setDisplayName(displayName);
    }

    @Override
    public void addCounter(Counter counter) {
      realGroup.addCounter(counter);
    }

    @Override
    public Counter addCounter(String name, String displayName, long value) {
      return realGroup.addCounter(name, displayName, value);
    }

    @Override
    public Counter findCounter(String counterName, String displayName) {
      return realGroup.findCounter(counterName, displayName);
    }

    @Override
    public Counter findCounter(String counterName, boolean create) {
      return realGroup.findCounter(counterName, create);
    }

    @Override
    public Counter findCounter(String counterName) {
      return realGroup.findCounter(counterName);
    }

    @Override
    public int size() {
      return realGroup.size();
    }

    @Override
    public void incrAllCounters(CounterGroupBase<Counter> rightGroup) {
      realGroup.incrAllCounters(rightGroup);
    }
    
    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return realGroup;
    }

    @Override
    public synchronized boolean equals(Object genericRight) {
      if (genericRight instanceof CounterGroupBase<?>) {
        @SuppressWarnings("unchecked")
        CounterGroupBase<Counter> right = ((CounterGroupBase<Counter>) 
        genericRight).getUnderlyingGroup();
        return Iterators.elementsEqual(iterator(), right.iterator());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return realGroup.hashCode();
    }
  }

  // All the group impls need this for legacy group interface
  static long getCounterValue(CounterGroupBase<Counter> group, String counterName) {
    Counter counter = group.findCounter(counterName, false);
    if (counter != null) return counter.getValue();
    return 0L;
  }

  // Mix the generic group implementation into the Group interface
  private static class GenericGroup extends AbstractCounterGroup<Counter> {

    GenericGroup(String name, String displayName, Limits limits) {
      super(name, displayName, limits);
    }

    @Override
    protected Counter newCounter(String counterName, String displayName,
                                 long value) {
      return new Counter(new GenericCounter(counterName, displayName, value));
    }

    @Override
    protected Counter newCounter() {
      return new Counter();
    }
    
    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
     return this;
    }
  }

  // Mix the framework group implementation into the Group interface
  private static class FrameworkGroupImpl<T extends Enum<T>>
      extends FrameworkCounterGroup<T, Counter> {

    FrameworkGroupImpl(Class<T> cls) {
      super(cls);
    }

    @Override
    protected Counter newCounter(T key) {
      return new Counter(new FrameworkCounter<T>(key, getName()));
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix the file system counter group implementation into the Group interface
  private static class FSGroupImpl extends FileSystemCounterGroup<Counter> {

    @Override
    protected Counter newCounter(String scheme, FileSystemCounter key) {
      return new Counter(new FSCounter(scheme, key));
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  public synchronized Counter findCounter(String group, String name) {
    if (name.equals("MAP_INPUT_BYTES")) {
      LOG.warn("Counter name MAP_INPUT_BYTES is deprecated. " +
               "Use FileInputFormatCounters as group name and " +
               " BYTES_READ as counter name instead");
      return findCounter(FileInputFormatCounter.BYTES_READ);
    }
    return getGroup(group).getCounterForName(name);
  }

  /**
   * Provide factory methods for counter group factory implementation.
   * See also the GroupFactory in
   *  {@link org.apache.hadoop.mapreduce.Counters mapreduce.Counters}
   */
  static class GroupFactory extends CounterGroupFactory<Counter, Group> {

    @Override
    protected <T extends Enum<T>>
    FrameworkGroupFactory<Group> newFrameworkGroupFactory(final Class<T> cls) {
      return new FrameworkGroupFactory<Group>() {
        @Override public Group newGroup(String name) {
          return new Group(new FrameworkGroupImpl<T>(cls)); // impl in this package
        }
      };
    }

    @Override
    protected Group newGenericGroup(String name, String displayName,
                                    Limits limits) {
      return new Group(new GenericGroup(name, displayName, limits));
    }

    @Override
    protected Group newFileSystemGroup() {
      return new Group(new FSGroupImpl());
    }
  }

  private static final GroupFactory groupFactory = new GroupFactory();

  /**
   * Find a counter by using strings
   * @param group the name of the group
   * @param id the id of the counter within the group (0 to N-1)
   * @param name the internal name of the counter
   * @return the counter for that name
   * @deprecated use {@link #findCounter(String, String)} instead
   */
  @Deprecated
  public Counter findCounter(String group, int id, String name) {
    return findCounter(group, name);
  }

  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param key identifies a counter
   * @param amount amount by which counter is to be incremented
   */
  public void incrCounter(Enum<?> key, long amount) {
    findCounter(key).increment(amount);
  }

  /**
   * Increments the specified counter by the specified amount, creating it if
   * it didn't already exist.
   * @param group the name of the group
   * @param counter the internal name of the counter
   * @param amount amount by which counter is to be incremented
   */
  public void incrCounter(String group, String counter, long amount) {
    findCounter(group, counter).increment(amount);
  }

  /**
   * Returns current value of the specified counter, or 0 if the counter
   * does not exist.
   * @param key the counter enum to lookup
   * @return the counter value or 0 if counter not found
   */
  public synchronized long getCounter(Enum<?> key) {
    return findCounter(key).getValue();
  }

  /**
   * Increments multiple counters by their amounts in another Counters
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for (Group otherGroup: other) {
      Group group = getGroup(otherGroup.getName());
      group.setDisplayName(otherGroup.getDisplayName());
      for (Counter otherCounter : otherGroup) {
        Counter counter = group.getCounterForName(otherCounter.getName());
        counter.setDisplayName(otherCounter.getDisplayName());
        counter.increment(otherCounter.getValue());
      }
    }
  }

  /**
   * @return the total number of counters
   * @deprecated use {@link #countCounters()} instead
   */
  public int size() {
    return countCounters();
  }

  /**
   * Convenience method for computing the sum of two sets of counters.
   * @param a the first counters
   * @param b the second counters
   * @return a new summed counters object
   */
  public static Counters sum(Counters a, Counters b) {
    Counters counters = new Counters();
    counters.incrAllCounters(a);
    counters.incrAllCounters(b);
    return counters;
  }

  /**
   * Logs the current counter values.
   * @param log The log to use.
   */
  public void log(Log log) {
    log.info("Counters: " + size());
    for(Group group: this) {
      log.info("  " + group.getDisplayName());
      for (Counter counter: group) {
        log.info("    " + counter.getDisplayName() + "=" +
                 counter.getCounter());
      }
    }
  }

  /**
   * Represent the counter in a textual format that can be converted back to
   * its object form
   * @return the string in the following format
   * {(groupName)(group-displayName)[(counterName)(displayName)(value)][]*}*
   */
  public String makeEscapedCompactString() {
    return toEscapedCompactString(this);
  }

  /**
   * Convert a stringified (by {@link #makeEscapedCompactString()} counter
   * representation into a counter object.
   * @param compactString to parse
   * @return a new counters object
   * @throws ParseException
   */
  public static Counters fromEscapedCompactString(String compactString)
      throws ParseException {
    return parseEscapedCompactString(compactString, new Counters());
  }
}
