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

package org.apache.hadoop.fs.s3a;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;

/**
 * Context for passing information down to S3 write operations.
 */
public class S3AWriteOpContext extends S3AOpContext {

  private final DeleteParentPolicy deleteParentPolicy;

  private final ExecutorService executorService;

  private final Progressable progress;

  private final S3AInstrumentation.OutputStreamStatistics statistics;

  private final WriteOperationHelper writeOperationHelper;

  /**
   * Instantiate.
   * @param isS3GuardEnabled
   * @param invoker
   * @param stats
   * @param instrumentation
   * @param dstFileStatus
   * @param deleteParentPolicy policy about parent deletion.
   * @param executorService the executor service to use to schedule work
   * @param progress report progress in order to prevent timeouts. If
   * this object implements {@code ProgressListener} then it will be
   * directly wired up to the AWS client, so receive detailed progress
   * information.
   * @param statistics stats for this stream
   * @param writeOperationHelper state of the write operation.
   */
  S3AWriteOpContext(
      final boolean isS3GuardEnabled,
      final Invoker invoker,
      @Nullable final FileSystem.Statistics stats,
      final S3AInstrumentation instrumentation,
      final FileStatus dstFileStatus,
      final DeleteParentPolicy deleteParentPolicy,
      final ExecutorService executorService,
      final Progressable progress,
      final S3AInstrumentation.OutputStreamStatistics statistics,
      final WriteOperationHelper writeOperationHelper) {
    super(isS3GuardEnabled, invoker, stats, instrumentation, dstFileStatus);
    this.deleteParentPolicy = deleteParentPolicy;
    this.executorService = executorService;
    this.progress = progress;
    this.statistics = statistics;
    this.writeOperationHelper = writeOperationHelper;
  }

  public DeleteParentPolicy getDeleteParentPolicy() {
    return deleteParentPolicy;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public Progressable getProgress() {
    return progress;
  }

  public S3AInstrumentation.OutputStreamStatistics getStatistics() {
    return statistics;
  }

  public WriteOperationHelper getWriteOperationHelper() {
    return writeOperationHelper;
  }

  /**
   * What is the delete policy here.
   */
  public enum DeleteParentPolicy {
    /** Single large bulk delete. */
    bulk,

    /** Incremental GET / + delete; bail out on first found. */
    incremental,

    /** No attempt to delete. */
    none
  }
}
