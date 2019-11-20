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

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

public class CurrentTasks<T extends TrackableTask> {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentTasks.class);

  final private Map<UUID, Pair<T, Integer>> toDo;
  final private Map<UUID, Pair<T, Integer>> inProgress;
  //Need to get this from config
  private final IntPredicate thresholdExpired;


  CurrentTasks(Collection<T> tasks, IntPredicate thresholdExpired) {
    this.thresholdExpired = thresholdExpired;
    this.toDo = Maps.newConcurrentMap();
    this.inProgress = Maps.newConcurrentMap();
    tasks.forEach(t -> this.toDo.put(t.getSyncTaskId(), Pair.of(t, 0)));
  }

  /**
   * mark a failure for a task. If it can still be retried, offer the task to
   * the to-do list again..
   *
   * @param uuid
   * @return true if the task will be retried or the item is not found.
   * Otherwise false.
   */
  public boolean markFailure(UUID uuid) {
    if (inProgress.keySet().contains(uuid)) {
      Pair<T, Integer> failedTask =
          inProgress.remove(uuid);
      Integer amountOfFailureTimes = failedTask.getRight();
      if (thresholdExpired.test(amountOfFailureTimes)) {
        return false;
      } else {
        toDo.put(uuid, Pair.of(failedTask.getLeft(), ++amountOfFailureTimes));
      }
    }
    return true;
  }

  public Optional<T> markFinished(UUID uuid) {
    Pair<T, Integer> finishedPair = inProgress.remove(uuid);
    return (finishedPair == null) ?
        Optional.empty() : Optional.of(finishedPair.getLeft());
  }

  public List<T> getTasksToDo() {
    return toDo.entrySet()
        .stream()
        .map(entry -> {
          toDo.remove(entry.getKey());
          inProgress.put(entry.getKey(), entry.getValue());
          return entry.getValue().getLeft();
        })
        .collect(Collectors.toList());
  }

  public boolean isFinished() {
    return inProgress.isEmpty() && toDo.isEmpty();
  }

  public boolean isNotFinished() {
    return !isFinished();
  }

}
