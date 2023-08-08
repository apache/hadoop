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

package org.apache.hadoop.fs.s3a.commit.impl;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.impl.WeakReferenceThreadMap;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.files.PendingSet;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.fs.s3a.Constants.THREAD_POOL_SHUTDOWN_DELAY_SECONDS;
import static org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter.THREAD_PREFIX;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.S3A_COMMITTER_EXPERIMENTAL_COLLECT_IOSTATISTICS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.S3A_COMMITTER_EXPERIMENTAL_COLLECT_IOSTATISTICS_DEFAULT;
import static org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants.THREAD_KEEP_ALIVE_TIME;

/**
 * Commit context.
 *
 * It is used to manage the final commit sequence where files become
 * visible.
 *
 * Once the commit operation has completed, it must be closed.
 * It MUST NOT be reused.
 *
 * Audit integration: job and task attributes are added to the thread local context
 * on create, removed on close().
 *
 * JSON Serializers are created on demand, on a per thread basis.
 * A {@link WeakReferenceThreadMap} is used here; a GC may lose the
 * references, but they will recreated as needed.
 */
public final class CommitContext implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(
      CommitContext.class);

  /**
   * The actual commit operations.
   */
  private final CommitOperations commitOperations;

  /**
   * Job Context.
   */
  private final JobContext jobContext;

  /**
   * Serializer pool.
   */

  private final WeakReferenceThreadMap<JsonSerialization<PendingSet>>
      pendingSetSerializer =
        new WeakReferenceThreadMap<>((k) -> PendingSet.serializer(), null);

  private final WeakReferenceThreadMap<JsonSerialization<SinglePendingCommit>>
      singleCommitSerializer =
        new WeakReferenceThreadMap<>((k) -> SinglePendingCommit.serializer(), null);

  /**
   * Submitter for per task operations, e.g loading manifests.
   */
  private PoolSubmitter outerSubmitter;

  /**
   * Submitter for operations within the tasks,
   * such as POSTing the final commit operations.
   */
  private PoolSubmitter innerSubmitter;

  /**
   * Job Configuration.
   */
  private final Configuration conf;

  /**
   * Job ID.
   */
  private final String jobId;

  /**
   * Audit context; will be reset when this is closed.
   */
  private final AuditContextUpdater auditContextUpdater;

  /**
   * Number of committer threads.
   */
  private final int committerThreads;

  /**
   * Should IOStatistics be collected by the committer?
   */
  private final boolean collectIOStatistics;

  /**
   * IOStatisticsContext to switch to in all threads
   * taking part in the commit operation.
   * This ensures that the IOStatistics collected in the
   * worker threads will be aggregated into the total statistics
   * of the thread calling the committer commit/abort methods.
   */
  private final IOStatisticsContext ioStatisticsContext;

  /**
   * Create.
   * @param commitOperations commit callbacks
   * @param jobContext job context
   * @param committerThreads number of commit threads
   * @param ioStatisticsContext IOStatistics context of current thread
   */
  public CommitContext(
      final CommitOperations commitOperations,
      final JobContext jobContext,
      final int committerThreads,
      final IOStatisticsContext ioStatisticsContext) {
    this.commitOperations = commitOperations;
    this.jobContext = jobContext;
    this.conf = jobContext.getConfiguration();
    JobID contextJobID = jobContext.getJobID();
    // either the job ID or make one up as it will be
    // used for the filename of any reports.
    this.jobId = contextJobID != null
        ? contextJobID.toString()
        : ("job-without-id-at-" + System.currentTimeMillis());
    this.collectIOStatistics = conf.getBoolean(
        S3A_COMMITTER_EXPERIMENTAL_COLLECT_IOSTATISTICS,
        S3A_COMMITTER_EXPERIMENTAL_COLLECT_IOSTATISTICS_DEFAULT);
    this.ioStatisticsContext = Objects.requireNonNull(ioStatisticsContext);
    this.auditContextUpdater = new AuditContextUpdater(jobContext);
    this.auditContextUpdater.updateCurrentAuditContext();
    this.committerThreads = committerThreads;
    buildSubmitters();
  }

  /**
   * Create for testing.
   * This has no job context; instead the values
   * are set explicitly.
   * @param commitOperations commit callbacks
   * @param conf job conf
   * @param jobId ID
   * @param committerThreads number of commit threads
   * @param ioStatisticsContext IOStatistics context of current thread
   */
  public CommitContext(final CommitOperations commitOperations,
      final Configuration conf,
      final String jobId,
      final int committerThreads,
      final IOStatisticsContext ioStatisticsContext) {
    this.commitOperations = commitOperations;
    this.jobContext = null;
    this.conf = conf;
    this.jobId = jobId;
    this.collectIOStatistics = false;
    this.ioStatisticsContext = Objects.requireNonNull(ioStatisticsContext);
    this.auditContextUpdater = new AuditContextUpdater(jobId);
    this.auditContextUpdater.updateCurrentAuditContext();
    this.committerThreads = committerThreads;
    buildSubmitters();
  }

  /**
   * Build the submitters and thread pools if the number of committerThreads
   * is greater than zero.
   * This should only be called in constructors; it is synchronized to keep
   * SpotBugs happy.
   */
  private synchronized void buildSubmitters() {
    if (committerThreads != 0) {
      outerSubmitter = new PoolSubmitter(buildThreadPool(committerThreads));
    }
  }

  /**
   * Returns an {@link ExecutorService} for parallel tasks. The number of
   * threads in the thread-pool is set by fs.s3a.committer.threads.
   * If num-threads is 0, this will raise an exception.
   * The threads have a lifespan set by
   * {@link InternalCommitterConstants#THREAD_KEEP_ALIVE_TIME}.
   * When the thread pool is full, the caller runs
   * policy takes over.
   * @param numThreads thread count, may be negative.
   * @return an {@link ExecutorService} for the number of threads
   */
  private ExecutorService buildThreadPool(
      int numThreads) {
    if (numThreads < 0) {
      // a negative number means "multiple of available processors"
      numThreads = numThreads * -Runtime.getRuntime().availableProcessors();
    }
    Preconditions.checkArgument(numThreads > 0,
        "Cannot create a thread pool with no threads");
    LOG.debug("creating thread pool of size {}", numThreads);
    final ThreadFactory factory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(THREAD_PREFIX + jobId + "-%d")
        .build();
    return new HadoopThreadPoolExecutor(numThreads, numThreads,
        THREAD_KEEP_ALIVE_TIME,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        factory,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  /**
   * Commit the operation, throwing an exception on any failure.
   * See {@code CommitOperations#commitOrFail(SinglePendingCommit)}.
   * @param commit commit to execute
   * @throws IOException on a failure
   */
  public void commitOrFail(SinglePendingCommit commit) throws IOException {
    commitOperations.commitOrFail(commit);
  }

  /**
   * Commit a single pending commit; exceptions are caught
   * and converted to an outcome.
   * See {@link CommitOperations#commit(SinglePendingCommit, String)}.
   * @param commit entry to commit
   * @param origin origin path/string for outcome text
   * @return the outcome
   */
  public CommitOperations.MaybeIOE commit(SinglePendingCommit commit,
      String origin) {
    return commitOperations.commit(commit, origin);
  }

  /**
   * See {@link CommitOperations#abortSingleCommit(SinglePendingCommit)}.
   * @param commit pending commit to abort
   * @throws FileNotFoundException if the abort ID is unknown
   * @throws IOException on any failure
   */
  public void abortSingleCommit(final SinglePendingCommit commit)
      throws IOException {
    commitOperations.abortSingleCommit(commit);
  }

  /**
   * See {@link CommitOperations#revertCommit(SinglePendingCommit)}.
   * @param commit pending commit
   * @throws IOException failure
   */
  public void revertCommit(final SinglePendingCommit commit)
      throws IOException {
    commitOperations.revertCommit(commit);
  }

  /**
   * See {@link CommitOperations#abortMultipartCommit(String, String)}..
   * @param destKey destination key
   * @param uploadId upload to cancel
   * @throws FileNotFoundException if the abort ID is unknown
   * @throws IOException on any failure
   */
  public void abortMultipartCommit(
      final String destKey,
      final String uploadId)
      throws IOException {
    commitOperations.abortMultipartCommit(destKey, uploadId);
  }

  @Override
  public synchronized void close() throws IOException {

    destroyThreadPools();
    auditContextUpdater.resetCurrentAuditContext();
  }

  @Override
  public String toString() {
    return "CommitContext{}";
  }

  /**
   * Job Context.
   * @return job context.
   */
  public JobContext getJobContext() {
    return jobContext;
  }

  /**
   * Return a submitter.
   * If created with 0 threads, this returns null so
   * TaskPool knows to run it in the current thread.
   * @return a submitter or null
   */
  public synchronized TaskPool.Submitter getOuterSubmitter() {
    return outerSubmitter;
  }

  /**
   * Return a submitter. As this pool is used less often,
   * create it on demand.
   * If created with 0 threads, this returns null so
   * TaskPool knows to run it in the current thread.
   * @return a submitter or null
   */
  public synchronized TaskPool.Submitter getInnerSubmitter() {
    if (innerSubmitter == null && committerThreads > 0) {
      innerSubmitter = new PoolSubmitter(buildThreadPool(committerThreads));
    }
    return innerSubmitter;
  }

  /**
   * Get a serializer for .pending files.
   * @return a serializer.
   */
  public JsonSerialization<SinglePendingCommit> getSinglePendingFileSerializer() {
    return singleCommitSerializer.getForCurrentThread();
  }

  /**
   * Get a serializer for .pendingset files.
   * @return a serializer.
   */
  public JsonSerialization<PendingSet> getPendingSetSerializer() {
    return pendingSetSerializer.getForCurrentThread();
  }

  /**
   * Destroy any thread pools; wait for that to finish,
   * but don't overreact if it doesn't finish in time.
   */
  private synchronized void destroyThreadPools() {
    try {
      IOUtils.cleanupWithLogger(LOG, outerSubmitter, innerSubmitter);
    } finally {
      outerSubmitter = null;
      innerSubmitter = null;
    }
  }

  /**
   * Job configuration.
   * @return configuration (never null)
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the job ID.
   * @return job ID.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * Collecting thread level IO statistics?
   * @return true if thread level IO stats should be collected.
   */
  public boolean isCollectIOStatistics() {
    return collectIOStatistics;
  }

  /**
   * IOStatistics context of the created thread.
   * @return the IOStatistics.
   */
  public IOStatisticsContext getIOStatisticsContext() {
    return ioStatisticsContext;
  }

  /**
   * Switch to the context IOStatistics context,
   * if needed.
   */
  public void switchToIOStatisticsContext() {
    IOStatisticsContext.setThreadIOStatisticsContext(ioStatisticsContext);
  }

  /**
   * Reset the IOStatistics context if statistics are being
   * collected.
   * Logs at info.
   */
  public void maybeResetIOStatisticsContext() {
    if (collectIOStatistics) {

      LOG.info("Resetting IO statistics context {}",
          ioStatisticsContext.getID());
      ioStatisticsContext.reset();
    }
  }

  /**
   * Submitter for a given thread pool.
   */
  private final class PoolSubmitter implements TaskPool.Submitter, Closeable {

    private ExecutorService executor;

    private PoolSubmitter(ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    public synchronized void close() throws IOException {
      if (executor != null) {
        HadoopExecutors.shutdown(executor, LOG,
            THREAD_POOL_SHUTDOWN_DELAY_SECONDS, TimeUnit.SECONDS);
      }
      executor = null;
    }

    /**
     * Forward to the submitter, wrapping in task
     * context setting, so as to ensure that all operations
     * have job/task attributes.
     * @param task task to execute
     * @return the future.
     */
    @Override
    public Future<?> submit(Runnable task) {
      return executor.submit(() -> {
        auditContextUpdater.updateCurrentAuditContext();
        try {
          task.run();
        } finally {
          auditContextUpdater.resetCurrentAuditContext();
        }
      });
    }

  }
}
