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
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart;

import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.CurrentTasksFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SchedulableSyncPhase;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.CreateFileSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase.INIT_PHASE;

/**
 * Multipart plan for syncing data to S3 or S3 compatible file system.
 */
public final class MultipartPlan {

  private static final Logger LOG = LoggerFactory
      .getLogger(MultipartPlan.class);
  private MultipartPhase multipartPhase;
  private List<SingleMultipart> multiparts;

  private MultipartPlan(List<SingleMultipart> multiparts) {
    this.multiparts = multiparts;
    this.multipartPhase = INIT_PHASE;
  }

  public static MultipartPlan create(List<CreateFileSyncTask> createFiles,
      CurrentTasksFactory currentTaskFactory,
      Function<SyncTask, Consumer<SyncTaskExecutionResult>> finalizer) {
    List<SingleMultipart> multiparts = createFiles
        .stream()
        .map(task -> SingleMultipart.create(task,
            currentTaskFactory,
            finalizer))
        .collect(Collectors.toList());
    return new MultipartPlan(multiparts);
  }

  public SchedulableSyncPhase handlePhase() {
    if (isCurrentPhaseStillInProgress()) {
      return getCurrentSchedulablePhase();
    } else {
      this.multipartPhase = multipartPhase.next();
      return getCurrentSchedulablePhase();
    }
  }

  public SchedulableSyncPhase getInitPhase() {
    return SchedulableSyncPhase.createMeta(multiparts
        .stream()
        .map(SingleMultipart::getInits)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList()));
  }

  public void markFinished(UUID syncTaskId, SyncTaskExecutionResult result) {
    LOG.debug("Mark {} finished in MultipartPlan", syncTaskId);
    multiparts
        .forEach(singleMp -> singleMp.markFinished(syncTaskId, result,
            multipartPhase));
  }

  public boolean isFinished() {
    return multiparts
        .stream()
        .allMatch(SingleMultipart::isFinished);
  }

  public boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result) {
    return multiparts
        .stream()
        .map(singleMp -> singleMp.markFailed(
            syncTaskId, result, multipartPhase))
        .reduce(true, Boolean::logicalAnd);
  }

  private SchedulableSyncPhase getPutPartPhase() {
    List<BlockSyncTask> multipartsReady = multiparts
        .stream()
        .map(SingleMultipart::getPuts)
        .flatMap(optList -> optList.map(Collection::stream).orElse(
            Stream.empty()))
        .collect(Collectors.toList());
    return SchedulableSyncPhase.createBlock(multipartsReady);
  }

  private SchedulableSyncPhase getCompletePhase() {
    return SchedulableSyncPhase.createMeta(multiparts
        .stream()
        .map(SingleMultipart::getCompletes)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList())
    );
  }

  private SchedulableSyncPhase getCurrentSchedulablePhase() {
    switch (multipartPhase) {
    case INIT_PHASE:
      return getInitPhase();
    case PUT_PHASE:
      return getPutPartPhase();
    case COMPLETE_PHASE:
      return getCompletePhase();
    default:
      return SchedulableSyncPhase.empty();
    }
  }

  private boolean isCurrentPhaseStillInProgress() {
    return multiparts
        .stream()
        .map(mp -> mp.inProgress(multipartPhase))
        .reduce(false, Boolean::logicalOr);
  }

  public MultipartPhase getMultipartPhase() {
    return multipartPhase;
  }

  public boolean isTaskUnderTrack(UUID syncTaskId) {
    return multiparts.stream().anyMatch(mp -> mp.isTaskUnderTrack(syncTaskId));
  }
}
