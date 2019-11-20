/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.util.Collection;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.CurrentTasksFactory.RetryStrategy.LIMITED;

public class CurrentTasksFactory {


  private static final IntPredicate neverExpire = i -> false;
  private static final IntFunction<IntPredicate> thresholdExpire =
      threshold -> i -> {
        return i >= threshold;
      };
  private Configuration conf;
  public CurrentTasksFactory(Configuration conf) {
    this.conf = conf;
  }

  public <T extends TrackableTask> CurrentTasks<T> create(Collection<T> tasks) {
    RetryStrategy retryStrategy =
        conf.getEnum(DFSConfigKeys.DFS_SNAPSHOT_UPDATE_TRACKER_RETRY_STRATEGY,
            RetryStrategy.FOREVER);
    if (retryStrategy == LIMITED) {
      int numberOfRetries = conf.getInt(DFSConfigKeys.DFS_SNAPSHOT_UPDATE_TRACKER_RETRY_STRATEGY_THRESHOLD,
          3);
      return new CurrentTasks<>(tasks, thresholdExpire.apply(numberOfRetries));
    }
    return new CurrentTasks<>(tasks, neverExpire);
  }

  public <T extends TrackableTask> CurrentTasks<T> create(T task) {
    return this.create(Lists.newArrayList(task));
  }

  public <T extends TrackableTask> CurrentTasks<T> empty() {
    return create(Lists.newArrayList());
  }


  public enum RetryStrategy {
    FOREVER,
    LIMITED

  }

}
