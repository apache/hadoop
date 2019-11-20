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
package org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.CurrentTasksFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask.MultipartCompleteMetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask.MultipartInitMetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SingleMultipart {

  private static final Logger LOG = LoggerFactory
      .getLogger(SingleMultipart.class);
  private static SyncTask.CreateFileSyncTask current;
  private final InitTracker initTracker;
  private Optional<PartsTracker> partsTrackerOpt;
  private Optional<CompleteTracker> completeTrackerOpt;
  private boolean finished;
  private Consumer<SyncTaskExecutionResult> finalizer;


  private SingleMultipart(MultipartInitMetadataSyncTask init,
      Function<ByteBuffer, List<BlockSyncTask>> createPutParts,
      BiFunction<ByteBuffer, List<ByteBuffer>, MultipartCompleteMetadataSyncTask> createComplete,
      CurrentTasksFactory currentTaskFactory, Consumer<SyncTaskExecutionResult> finalizer) {
    this.finalizer = finalizer;
    this.initTracker = new InitTracker(init, createPutParts, createComplete, currentTaskFactory);
    this.partsTrackerOpt = Optional.empty();
    this.completeTrackerOpt = Optional.empty();
    this.finished = false;
  }

  public static SingleMultipart create(
      SyncTask.CreateFileSyncTask current,
      CurrentTasksFactory currentTaskFactory,
      Function<SyncTask, Consumer<SyncTaskExecutionResult>> finalizer) {
    SingleMultipart.current = current;

    MultipartInitMetadataSyncTask fileMultipartInit =
        (MultipartInitMetadataSyncTask)
            MetadataSyncTask.createFileMultipartInit(current.getUri(),
                current.getSyncMountId());

    List<LocatedBlock> locatedBlocks = current
        .getLocatedBlocks();


    Function<ByteBuffer, List<BlockSyncTask>> createMultipartPuts =
        uploadHandle -> createParts(current, locatedBlocks, uploadHandle);

    BiFunction<ByteBuffer, List<ByteBuffer>, MultipartCompleteMetadataSyncTask> createMultipartCompletes =
        MetadataSyncTask.multipartComplete(current.getUri(),
            current.getLocatedBlocks()
                .stream()
                .map(LocatedBlock::getBlock)
                .collect(Collectors.toList()),
            current.getSyncMountId(),
            current.getBlockCollectionId());

    return new SingleMultipart(
        fileMultipartInit,
        createMultipartPuts,
        createMultipartCompletes,
        currentTaskFactory,
        finalizer.apply(current)
    );
  }

  private static List<BlockSyncTask> createParts(SyncTask.CreateFileSyncTask current,
      List<LocatedBlock> locatedBlocks, ByteBuffer uploadHandle) {
    List<BlockSyncTask> puts = Lists.newArrayList();
    for (int i = 0; i < locatedBlocks.size(); i++) {
      BlockSyncTask put = createPart(current, locatedBlocks, uploadHandle, i);
      puts.add(put);
    }
    return puts;
  }

  private static BlockSyncTask createPart(SyncTask.CreateFileSyncTask current,
      List<LocatedBlock> locatedBlocks, ByteBuffer uploadHandle, int i) {
    return BlockSyncTask.multipartPut(
        current.getUri(),
        locatedBlocks.get(i),
        i + 1,
        current.getSyncMountId()
    ).apply(uploadHandle);
  }

  public void markFinished(UUID syncTaskId, SyncTaskExecutionResult result,
      MultipartPhase multipartPhase) {
    switch (multipartPhase) {
    case INIT_PHASE:
      //TODO This could be improved with a multimap
      if (!this.partsTrackerOpt.isPresent()) {
        this.partsTrackerOpt = initTracker.markFinished(syncTaskId, result);
      }
      break;
    case PUT_PHASE:
      //TODO This could be improved with a multimap
      if (!completeTrackerOpt.isPresent()) {
        this.completeTrackerOpt = partsTrackerOpt
            .flatMap(psm -> psm.markFinished(syncTaskId, result));
      }
      break;
    case COMPLETE_PHASE:
      this.completeTrackerOpt.ifPresent(completeTracker ->
          completeTracker.markFinished(syncTaskId,
              //TODO This could be improved with a multimap
              () -> {
                this.finished = true;
                this.finalizer.accept(result);
              }));
      break;
    }
  }

  public boolean markFailed(UUID syncTaskId, SyncTaskExecutionResult result,
      MultipartPhase multipartPhase) {
    switch (multipartPhase) {
    case INIT_PHASE:
      return initTracker.markFailed(syncTaskId, result);
    case PUT_PHASE:
      return this.partsTrackerOpt.map(
          partsTracker -> partsTracker.markFailed(syncTaskId, result)
      ).orElse(true);
    case COMPLETE_PHASE:
      return this.completeTrackerOpt.map(
          completeTracker -> completeTracker.markFailed(syncTaskId, result)
      ).orElse(true);
    }
    return true;
  }

  public Optional<MetadataSyncTask> getInits() {
    return this.initTracker.getTasks();
  }

  public Optional<MetadataSyncTask> getCompletes() {
    return completeTrackerOpt
        .flatMap(CompleteTracker::getComplete);
  }

  public Optional<List<BlockSyncTask>> getPuts() {
    return this.partsTrackerOpt
        .map(PartsTracker::getPuts)
        .map(this::castList);
  }

  public boolean isFinished() {
    return finished;
  }

  /**
   * TODO: This turns List<MultipartPutPartMetadataSyncTask> into
   * List<MetadataSyncTask>. Needs to disappear when working with better
   * typed structures all the way or need to find out better way of casting.
   */
  private List<BlockSyncTask> castList(Collection<BlockSyncTask> mpList) {
    return mpList
        .stream()
        .map(bst -> (BlockSyncTask) bst)
        .collect(Collectors.toList());
  }

  public boolean inProgress(MultipartPhase multipartPhase) {
    switch (multipartPhase) {
    case INIT_PHASE:
      //Init is in progress as long as the parts tracking has not been started
      return !this.partsTrackerOpt.isPresent();
    case PUT_PHASE:
      //Put is in progress as long as the complete tracking has not been started
      return !this.completeTrackerOpt.isPresent();
    case COMPLETE_PHASE:
      //Complete is in progress as long as the finalizer has not been run
      return !this.finished;
    }
    return false;
  }
}
