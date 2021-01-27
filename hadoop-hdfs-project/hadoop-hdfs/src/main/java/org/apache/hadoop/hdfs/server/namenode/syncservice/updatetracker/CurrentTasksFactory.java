/**
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

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.util.Collection;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_SYNC_LIMITED_RETRY_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROVIDED_SYNC_RETRY_STRATEGY_DEFAULT;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.CurrentTasksFactory.RetryStrategy.LIMITED;

/**
 * Factory to create CurrentTasks in sync service.
 */
public class CurrentTasksFactory {
  private static final IntPredicate NEVER_EXPIRE = i -> false;
  private static final IntFunction<IntPredicate> THRESHOLD_EXPIRE =
      threshold -> i -> i >= threshold;
  private Configuration conf;
  public CurrentTasksFactory(Configuration conf) {
    this.conf = conf;
  }

  public <T extends TrackableTask> CurrentTasks<T> create(Collection<T> tasks) {
    RetryStrategy retryStrategy =
        conf.getEnum(DFSConfigKeys.DFS_PROVIDED_SYNC_RETRY_STRATEGY,
            DFS_PROVIDED_SYNC_RETRY_STRATEGY_DEFAULT);
    if (retryStrategy == LIMITED) {
      int numberOfRetries = conf.getInt(
          DFSConfigKeys.DFS_PROVIDED_SYNC_LIMITED_RETRY_THRESHOLD,
          DFS_PROVIDED_SYNC_LIMITED_RETRY_THRESHOLD_DEFAULT);
      return new CurrentTasks<>(tasks, THRESHOLD_EXPIRE.apply(numberOfRetries));
    }
    return new CurrentTasks<>(tasks, NEVER_EXPIRE);
  }

  public <T extends TrackableTask> CurrentTasks<T> create(T task) {
    return this.create(Lists.newArrayList(task));
  }

  public <T extends TrackableTask> CurrentTasks<T> empty() {
    return create(Lists.newArrayList());
  }

  /**
   * Retry Strategy if task fails.
   */
  public enum RetryStrategy {
    FOREVER,
    LIMITED
  }
}
