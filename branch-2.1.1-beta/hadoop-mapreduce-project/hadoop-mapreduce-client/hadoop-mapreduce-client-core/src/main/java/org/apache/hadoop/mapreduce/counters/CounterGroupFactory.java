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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.util.ResourceBundles;

/**
 * An abstract class to provide common implementation of the
 * group factory in both mapred and mapreduce packages.
 *
 * @param <C> type of the counter
 * @param <G> type of the group
 */
@InterfaceAudience.Private
public abstract class CounterGroupFactory<C extends Counter,
                                          G extends CounterGroupBase<C>> {

  public interface FrameworkGroupFactory<F> {
    F newGroup(String name);
  }

  // Integer mapping (for serialization) for framework groups
  private static final Map<String, Integer> s2i = Maps.newHashMap();
  private static final List<String> i2s = Lists.newArrayList();
  private static final int VERSION = 1;
  private static final String FS_GROUP_NAME = FileSystemCounter.class.getName();

  private final Map<String, FrameworkGroupFactory<G>> fmap = Maps.newHashMap();
  {
    // Add builtin counter class here and the version when changed.
    addFrameworkGroup(TaskCounter.class);
    addFrameworkGroup(JobCounter.class);
  }

  // Initialize the framework counter group mapping
  private synchronized <T extends Enum<T>>
  void addFrameworkGroup(final Class<T> cls) {
    updateFrameworkGroupMapping(cls);
    fmap.put(cls.getName(), newFrameworkGroupFactory(cls));
  }

  // Update static mappings (c2i, i2s) of framework groups
  private static synchronized void updateFrameworkGroupMapping(Class<?> cls) {
    String name = cls.getName();
    Integer i = s2i.get(name);
    if (i != null) return;
    i2s.add(name);
    s2i.put(name, i2s.size() - 1);
  }

  /**
   * Required override to return a new framework group factory
   * @param <T> type of the counter enum class
   * @param cls the counter enum class
   * @return a new framework group factory
   */
  protected abstract <T extends Enum<T>>
  FrameworkGroupFactory<G> newFrameworkGroupFactory(Class<T> cls);

  /**
   * Create a new counter group
   * @param name of the group
   * @param limits the counters limits policy object
   * @return a new counter group
   */
  public G newGroup(String name, Limits limits) {
    return newGroup(name, ResourceBundles.getCounterGroupName(name, name),
                    limits);
  }

  /**
   * Create a new counter group
   * @param name of the group
   * @param displayName of the group
   * @param limits the counters limits policy object
   * @return a new counter group
   */
  public G newGroup(String name, String displayName, Limits limits) {
    FrameworkGroupFactory<G> gf = fmap.get(name);
    if (gf != null) return gf.newGroup(name);
    if (name.equals(FS_GROUP_NAME)) {
      return newFileSystemGroup();
    } else if (s2i.get(name) != null) {
      return newFrameworkGroup(s2i.get(name));
    }
    return newGenericGroup(name, displayName, limits);
  }

  /**
   * Create a new framework group
   * @param id of the group
   * @return a new framework group
   */
  public G newFrameworkGroup(int id) {
    String name;
    synchronized(CounterGroupFactory.class) {
      if (id < 0 || id >= i2s.size()) throwBadFrameGroupIdException(id);
      name = i2s.get(id); // should not throw here.
    }
    FrameworkGroupFactory<G> gf = fmap.get(name);
    if (gf == null) throwBadFrameGroupIdException(id);
    return gf.newGroup(name);
  }

  /**
   * Get the id of a framework group
   * @param name of the group
   * @return the framework group id
   */
  public static synchronized int getFrameworkGroupId(String name) {
    Integer i = s2i.get(name);
    if (i == null) throwBadFrameworkGroupNameException(name);
    return i;
  }

  /**
   * @return the counter factory version
   */
  public int version() {
    return VERSION;
  }

  /**
   * Check whether a group name is a name of a framework group (including
   * the filesystem group).
   *
   * @param name  to check
   * @return true for framework group names
   */
  public static synchronized boolean isFrameworkGroup(String name) {
    return s2i.get(name) != null || name.equals(FS_GROUP_NAME);
  }

  private static void throwBadFrameGroupIdException(int id) {
    throw new IllegalArgumentException("bad framework group id: "+ id);
  }

  private static void throwBadFrameworkGroupNameException(String name) {
    throw new IllegalArgumentException("bad framework group name: "+ name);
  }

  /**
   * Abstract factory method to create a generic (vs framework) counter group
   * @param name  of the group
   * @param displayName of the group
   * @param limits limits of the counters
   * @return a new generic counter group
   */
  protected abstract G newGenericGroup(String name, String displayName,
                                       Limits limits);

  /**
   * Abstract factory method to create a file system counter group
   * @return a new file system counter group
   */
  protected abstract G newFileSystemGroup();
}
