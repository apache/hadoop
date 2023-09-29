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

import static org.apache.hadoop.mapreduce.counters.CounterGroupFactory.getFrameworkGroupId;
import static org.apache.hadoop.mapreduce.counters.CounterGroupFactory.isFrameworkGroup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.StringInterner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * An abstract class to provide common implementation for the Counters
 * container in both mapred and mapreduce packages.
 *
 * @param <C> type of counter inside the counters
 * @param <G> type of group inside the counters
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractCounters<C extends Counter,
                                       G extends CounterGroupBase<C>>
    implements Writable, Iterable<G> {

  protected static final Logger LOG =
      LoggerFactory.getLogger("mapreduce.Counters");

  /**
   * A cache from enum values to the associated counter.
   */
  private final Map<Enum<?>, C> cache = Maps.newIdentityHashMap();
  //framework & fs groups
  private final Map<String, G> fgroups = new ConcurrentSkipListMap<String, G>();
  // other groups
  private final Map<String, G> groups = new ConcurrentSkipListMap<String, G>();
  private final CounterGroupFactory<C, G> groupFactory;

  // For framework counter serialization without strings
  enum GroupType { FRAMEWORK, FILESYSTEM };

  // Writes only framework and fs counters if false.
  private boolean writeAllCounters = true;

  private static final Map<String, String> legacyMap = Maps.newHashMap();
  static {
    legacyMap.put("org.apache.hadoop.mapred.Task$Counter",
                  TaskCounter.class.getName());
    legacyMap.put("org.apache.hadoop.mapred.JobInProgress$Counter",
                  JobCounter.class.getName());
    legacyMap.put("FileSystemCounters", FileSystemCounter.class.getName());
  }

  private final Limits limits = new Limits();

  @InterfaceAudience.Private
  public AbstractCounters(CounterGroupFactory<C, G> gf) {
    groupFactory = gf;
  }

  /**
   * Construct from another counters object.
   * @param <C1> type of the other counter
   * @param <G1> type of the other counter group
   * @param counters the counters object to copy
   * @param groupFactory the factory for new groups
   */
  @InterfaceAudience.Private
  public <C1 extends Counter, G1 extends CounterGroupBase<C1>>
  AbstractCounters(AbstractCounters<C1, G1> counters,
                   CounterGroupFactory<C, G> groupFactory) {
    this.groupFactory = groupFactory;
    for(G1 group: counters) {
      String name = group.getName();
      G newGroup = groupFactory.newGroup(name, group.getDisplayName(), limits);
      (isFrameworkGroup(name) ? fgroups : groups).put(name, newGroup);
      for(Counter counter: group) {
        newGroup.addCounter(counter.getName(), counter.getDisplayName(),
                            counter.getValue());
      }
    }
  }

  /** Add a group.
   * @param group object to add
   * @return the group
   */
  @InterfaceAudience.Private
  public synchronized G addGroup(G group) {
    String name = group.getName();
    if (isFrameworkGroup(name)) {
      fgroups.put(name, group);
    } else {
      limits.checkGroups(groups.size() + 1);
      groups.put(name, group);
    }
    return group;
  }

  /**
   * Add a new group
   * @param name of the group
   * @param displayName of the group
   * @return the group
   */
  @InterfaceAudience.Private
  public G addGroup(String name, String displayName) {
    return addGroup(groupFactory.newGroup(name, displayName, limits));
  }

  /**
   * Find a counter, create one if necessary
   * @param groupName of the counter
   * @param counterName name of the counter
   * @return the matching counter
   */
  public C findCounter(String groupName, String counterName) {
    G grp = getGroup(groupName);
    return grp.findCounter(counterName);
  }

  /**
   * Find the counter for the given enum. The same enum will always return the
   * same counter.
   * @param key the counter key
   * @return the matching counter object
   */
  public synchronized C findCounter(Enum<?> key) {
    C counter = cache.get(key);
    if (counter == null) {
      counter = findCounter(key.getDeclaringClass().getName(), key.name());
      cache.put(key, counter);
    }
    return counter;
  }

  /**
   * Find the file system counter for the given scheme and enum.
   * @param scheme of the file system
   * @param key the enum of the counter
   * @return the file system counter
   */
  @InterfaceAudience.Private
  public synchronized C findCounter(String scheme, FileSystemCounter key) {
    return ((FileSystemCounterGroup<C>) getGroup(
        FileSystemCounter.class.getName()).getUnderlyingGroup()).
        findCounter(scheme, key);
  }

  /**
   * Returns the names of all counter classes.
   * @return Set of counter names.
   */
  public synchronized Iterable<String> getGroupNames() {
    HashSet<String> deprecated = new HashSet<String>();
    for(Map.Entry<String, String> entry : legacyMap.entrySet()) {
      String newGroup = entry.getValue();
      boolean isFGroup = isFrameworkGroup(newGroup);
      if(isFGroup ? fgroups.containsKey(newGroup) : groups.containsKey(newGroup)) {
        deprecated.add(entry.getKey());
      }
    }
    return Iterables.concat(fgroups.keySet(), groups.keySet(), deprecated);
  }

  @Override
  public Iterator<G> iterator() {
    return Iterators.concat(fgroups.values().iterator(),
                            groups.values().iterator());
  }

  /**
   * Returns the named counter group, or an empty group if there is none
   * with the specified name.
   * @param groupName name of the group
   * @return the group
   */
  public synchronized G getGroup(String groupName) {

    // filterGroupName
    boolean groupNameInLegacyMap = true;
    String newGroupName = legacyMap.get(groupName);
    if (newGroupName == null) {
      groupNameInLegacyMap = false;
      newGroupName = Limits.filterGroupName(groupName);
    }

    boolean isFGroup = isFrameworkGroup(newGroupName);
    G group = isFGroup ? fgroups.get(newGroupName) : groups.get(newGroupName);
    if (group == null) {
      group = groupFactory.newGroup(newGroupName, limits);
      if (isFGroup) {
        fgroups.put(newGroupName, group);
      } else {
        limits.checkGroups(groups.size() + 1);
        groups.put(newGroupName, group);
      }
      if (groupNameInLegacyMap) {
        LOG.warn("Group " + groupName + " is deprecated. Use " + newGroupName
            + " instead");
      }
    }
    return group;
  }

  /**
   * Returns the total number of counters, by summing the number of counters
   * in each group.
   * @return the total number of counters
   */
  public synchronized int countCounters() {
    int result = 0;
    for (G group : this) {
      result += group.size();
    }
    return result;
  }

  /**
   * Write the set of groups.
   * Counters ::= version #fgroups (groupId, group)* #groups (group)*
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, groupFactory.version());
    WritableUtils.writeVInt(out, fgroups.size());  // framework groups first
    for (G group : fgroups.values()) {
      if (group.getUnderlyingGroup() instanceof FrameworkCounterGroup<?, ?>) {
        WritableUtils.writeVInt(out, GroupType.FRAMEWORK.ordinal());
        WritableUtils.writeVInt(out, getFrameworkGroupId(group.getName()));
        group.write(out);
      } else if (group.getUnderlyingGroup() instanceof FileSystemCounterGroup<?>) {
        WritableUtils.writeVInt(out, GroupType.FILESYSTEM.ordinal());
        group.write(out);
      }
    }
    if (writeAllCounters) {
      WritableUtils.writeVInt(out, groups.size());
      for (G group : groups.values()) {
        Text.writeString(out, group.getName());
        group.write(out);
      }
    } else {
      WritableUtils.writeVInt(out, 0);
    }
  }

  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    int version = WritableUtils.readVInt(in);
    if (version != groupFactory.version()) {
      throw new IOException("Counters version mismatch, expected "+
          groupFactory.version() +" got "+ version);
    }
    int numFGroups = WritableUtils.readVInt(in);
    fgroups.clear();
    GroupType[] groupTypes = GroupType.values();
    while (numFGroups-- > 0) {
      GroupType groupType = groupTypes[WritableUtils.readVInt(in)];
      G group;
      switch (groupType) {
        case FILESYSTEM: // with nothing
          group = groupFactory.newFileSystemGroup();
          break;
        case FRAMEWORK:  // with group id
          group = groupFactory.newFrameworkGroup(WritableUtils.readVInt(in));
          break;
        default: // Silence dumb compiler, as it would've thrown earlier
          throw new IOException("Unexpected counter group type: "+ groupType);
      }
      group.readFields(in);
      fgroups.put(group.getName(), group);
    }
    int numGroups = WritableUtils.readVInt(in);
    while (numGroups-- > 0) {
      limits.checkGroups(groups.size() + 1);
      G group = groupFactory.newGenericGroup(
          StringInterner.weakIntern(Text.readString(in)), null, limits);
      group.readFields(in);
      groups.put(group.getName(), group);
    }
  }

  /**
   * Return textual representation of the counter values.
   * @return the string
   */
  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("Counters: " + countCounters());
    for (G group: this) {
      sb.append("\n\t").append(group.getDisplayName());
      for (Counter counter: group) {
        sb.append("\n\t\t").append(counter.getDisplayName()).append("=")
          .append(counter.getValue());
      }
    }
    return sb.toString();
  }

  /**
   * Increments multiple counters by their amounts in another Counters
   * instance.
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(AbstractCounters<C, G> other) {
    for(G right : other) {
      String groupName = right.getName();
      G left = (isFrameworkGroup(groupName) ? fgroups : groups).get(groupName);
      if (left == null) {
        left = addGroup(groupName, right.getDisplayName());
      }
      left.incrAllCounters(right);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object genericRight) {
    if (genericRight instanceof AbstractCounters<?, ?>) {
      return Iterators.elementsEqual(iterator(),
          ((AbstractCounters<C, G>)genericRight).iterator());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return groups.hashCode();
  }

  /**
   * Set the "writeAllCounters" option to true or false
   * @param send  if true all counters would be serialized, otherwise only
   *              framework counters would be serialized in
   *              {@link #write(DataOutput)}
   */
  @InterfaceAudience.Private
  public void setWriteAllCounters(boolean send) {
    writeAllCounters = send;
  }

  /**
   * Get the "writeAllCounters" option
   * @return true of all counters would serialized
   */
  @InterfaceAudience.Private
  public boolean getWriteAllCounters() {
    return writeAllCounters;
  }

  @InterfaceAudience.Private
  public Limits limits() {
    return limits;
  }
}
