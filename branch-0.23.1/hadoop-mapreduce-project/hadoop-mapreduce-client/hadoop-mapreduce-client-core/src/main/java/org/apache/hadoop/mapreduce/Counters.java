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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.counters.AbstractCounterGroup;
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;
import org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup;
import org.apache.hadoop.mapreduce.counters.AbstractCounters;
import org.apache.hadoop.mapreduce.counters.CounterGroupFactory;
import org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup;

/**
 * <p><code>Counters</code> holds per job/task counters, defined either by the
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 *
 * <p><code>Counters</code> are bunched into {@link CounterGroup}s, each
 * comprising of counters from a particular <code>Enum</code> class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Counters extends AbstractCounters<Counter, CounterGroup> {

  // Mix framework group implementation into CounterGroup interface
  private static class FrameworkGroupImpl<T extends Enum<T>>
      extends FrameworkCounterGroup<T, Counter> implements CounterGroup {

    FrameworkGroupImpl(Class<T> cls) {
      super(cls);
    }

    @Override
    protected FrameworkCounter newCounter(T key) {
      return new FrameworkCounter(key);
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix generic group implementation into CounterGroup interface
  // and provide some mandatory group factory methods.
  private static class GenericGroup extends AbstractCounterGroup<Counter>
      implements CounterGroup {

    GenericGroup(String name, String displayName, Limits limits) {
      super(name, displayName, limits);
    }

    @Override
    protected Counter newCounter(String name, String displayName, long value) {
      return new GenericCounter(name, displayName, value);
    }

    @Override
    protected Counter newCounter() {
      return new GenericCounter();
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  // Mix file system group implementation into the CounterGroup interface
  private static class FileSystemGroup extends FileSystemCounterGroup<Counter>
      implements CounterGroup {

    @Override
    protected Counter newCounter(String scheme, FileSystemCounter key) {
      return new FSCounter(scheme, key);
    }

    @Override
    public CounterGroupBase<Counter> getUnderlyingGroup() {
      return this;
    }
  }

  /**
   * Provide factory methods for counter group factory implementation.
   * See also the GroupFactory in
   *  {@link org.apache.hadoop.mapred.Counters mapred.Counters}
   */
  private static class GroupFactory
      extends CounterGroupFactory<Counter, CounterGroup> {

    @Override
    protected <T extends Enum<T>>
    FrameworkGroupFactory<CounterGroup>
        newFrameworkGroupFactory(final Class<T> cls) {
      return new FrameworkGroupFactory<CounterGroup>() {
        @Override public CounterGroup newGroup(String name) {
          return new FrameworkGroupImpl<T>(cls); // impl in this package
        }
      };
    }

    @Override
    protected CounterGroup newGenericGroup(String name, String displayName,
                                           Limits limits) {
      return new GenericGroup(name, displayName, limits);
    }

    @Override
    protected CounterGroup newFileSystemGroup() {
      return new FileSystemGroup();
    }
  }

  private static final GroupFactory groupFactory = new GroupFactory();

  /**
   * Default constructor
   */
  public Counters() {
    super(groupFactory);
  }

  /**
   * Construct the Counters object from the another counters object
   * @param <C> the type of counter
   * @param <G> the type of counter group
   * @param counters the old counters object
   */
  public <C extends Counter, G extends CounterGroupBase<C>>
  Counters(AbstractCounters<C, G> counters) {
    super(counters, groupFactory);
  }
}
