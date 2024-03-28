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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@link OutputCommitter} that commits files specified
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends PathOutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileOutputCommitter.class);

  /** 
   * Name of directory where pending data is placed.  Data that has not been
   * committed yet.
   */
  public static final String PENDING_DIR_NAME = "_temporary";
  /**
   * Temporary directory name 
   *
   * The static variable to be compatible with M/R 1.x
   */
  @Deprecated
  protected static final String TEMP_DIR_NAME = PENDING_DIR_NAME;
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
      "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  public static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION =
      "mapreduce.fileoutputcommitter.algorithm.version";
  public static final int FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 2;
  // Skip cleanup _temporary folders under job's output directory
  public static final String FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED =
      "mapreduce.fileoutputcommitter.cleanup.skipped";
  public static final boolean
      FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT = false;

  // Ignore exceptions in cleanup _temporary folder under job's output directory
  public static final String FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED =
      "mapreduce.fileoutputcommitter.cleanup-failures.ignored";
  public static final boolean
      FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT = false;

  // Number of attempts when failure happens in commit job
  public static final String FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS =
      "mapreduce.fileoutputcommitter.failures.attempts";

  // default value to be 1 to keep consistent with previous behavior
  public static final int FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT = 1;

  // Whether tasks should delete their task temporary directories. This is
  // purely an optimization for filesystems without O(1) recursive delete, as
  // commitJob will recursively delete the entire job temporary directory.
  // HDFS has O(1) recursive delete, so this parameter is left false by default.
  // Users of object stores, for example, may want to set this to true. Note:
  // this is only used if mapreduce.fileoutputcommitter.algorithm.version=2
  public static final String FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED =
      "mapreduce.fileoutputcommitter.task.cleanup.enabled";
  public static final boolean
      FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED_DEFAULT = false;

  public static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_MV_THREADS =
      "mapreduce.fileoutputcommitter.algorithm.version.v1.experimental.mv.threads";
  public static final int FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_MV_THREADS_DEFAULT = 1;

  public static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_PARALLEL_TASK_COMMIT =
      "mapreduce.fileoutputcommitter.algorithm.version.v1.experimental.parallel.task.commit";
  public static final boolean
      FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_PARALLEL_TASK_COMMIT_DEFAULT = true;

  private Path outputPath = null;
  private Path workPath = null;
  private final int algorithmVersion;
  private final boolean skipCleanup;
  private final boolean ignoreCleanupFailures;
  @VisibleForTesting
  final int moveThreads;
  final AtomicInteger numberOfTasks = new AtomicInteger();
  final boolean isParallelTaskCommitEnabled;

  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  public FileOutputCommitter(Path outputPath, 
                             TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext)context);
    if (getOutputPath() != null) {
      workPath = Preconditions.checkNotNull(
          getTaskAttemptPath(context, getOutputPath()),
          "Null task attempt path in %s and output path %s",
          context, outputPath);
    }
  }
  
  /**
   * Create a file output committer
   * @param outputPath the job's output path, or null if you want the output
   * committer to act as a noop.
   * @param context the task's context
   * @throws IOException
   */
  @Private
  public FileOutputCommitter(Path outputPath, 
                             JobContext context) throws IOException {
    super(outputPath, context);
    Configuration conf = context.getConfiguration();
    algorithmVersion =
        conf.getInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                    FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT);
    int configuredThreads =
        context.getConfiguration().getInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_MV_THREADS,
            FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_MV_THREADS_DEFAULT);
    int minThreads = Math.max(1, configuredThreads);
    moveThreads = Math.min(minThreads, (Runtime.getRuntime().availableProcessors() * 16));
    isParallelTaskCommitEnabled = context.getConfiguration().getBoolean(
        FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_PARALLEL_TASK_COMMIT,
        FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_V1_PARALLEL_TASK_COMMIT_DEFAULT);
    LOG.info("File Output Committer Algorithm version is " + algorithmVersion);
    if (algorithmVersion != 1 && algorithmVersion != 2) {
      throw new IOException("Only 1 or 2 algorithm version is supported");
    }

    // if skip cleanup
    skipCleanup = conf.getBoolean(
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED,
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT);

    // if ignore failures in cleanup
    ignoreCleanupFailures = conf.getBoolean(
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED,
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT);

    LOG.info("FileOutputCommitter skip cleanup _temporary folders under " +
        "output directory:" + skipCleanup + ", ignore cleanup failures: " +
        ignoreCleanupFailures);

    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      this.outputPath = fs.makeQualified(outputPath);
    }
  }
  
  /**
   * @return the path where final output of the job should be placed.  This
   * could also be considered the committed application attempt path.
   */
  @Override
  public Path getOutputPath() {
    return this.outputPath;
  }

  /**
   * @return the path where the output of pending job attempts are
   * stored.
   */
  private Path getPendingJobAttemptsPath() {
    return getPendingJobAttemptsPath(getOutputPath());
  }
  
  /**
   * Get the location of pending job attempts.
   * @param out the base output directory.
   * @return the location of pending job attempts.
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, PENDING_DIR_NAME);
  }
  
  /**
   * Get the Application Attempt Id for this job
   * @param context the context to look in
   * @return the Application Attempt Id for a given job.
   */
  private static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @return the path to store job attempt data.
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(context, getOutputPath());
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param context the context of the job.  This is used to get the
   * application attempt id.
   * @param out the output path to place these in.
   * @return the path to store job attempt data.
   */
  public static Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getJobAttemptPath(appAttemptId, getOutputPath());
  }
  
  /**
   * Compute the path where the output of a given job attempt will be placed. 
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store job attempt data.
   */
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out), String.valueOf(appAttemptId));
  }
  
  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks. 
   * @return the path where the output of pending task attempts are stored.
   */
  private Path getPendingTaskAttemptsPath(JobContext context) {
    return getPendingTaskAttemptsPath(context, getOutputPath());
  }
  
  /**
   * Compute the path where the output of pending task attempts are stored.
   * @param context the context of the job with pending tasks. 
   * @return the path where the output of pending task attempts are stored.
   */
  private static Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out), PENDING_DIR_NAME);
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @return the path where a task attempt should be stored.
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return new Path(getPendingTaskAttemptsPath(context), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * 
   * @param context the context of the task attempt.
   * @param out The output path to put things in.
   * @return the path where a task attempt should be stored.
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * Compute the path where the output of a committed task is stored until
   * the entire job is committed.
   * @param context the context of the task attempt
   * @return the path where the output of a committed task is stored until
   * the entire job is committed.
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }
  
  public static Path getCommittedTaskPath(TaskAttemptContext context, Path out) {
    return getCommittedTaskPath(getAppAttemptId(context), context, out);
  }
  
  /**
   * Compute the path where the output of a committed task is stored until the
   * entire job is committed for a specific application attempt.
   * @param appAttemptId the id of the application attempt to use
   * @param context the context of any task.
   * @return the path where the output of a committed task is stored.
   */
  protected Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }
  
  private static Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context, Path out) {
    return new Path(getJobAttemptPath(appAttemptId, out),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  private static class CommittedTaskFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !PENDING_DIR_NAME.equals(path.getName());
    }
  }

  /**
   * Get a list of all paths where output from committed tasks are stored.
   * @param context the context of the current job
   * @return the list of these Paths/FileStatuses. 
   * @throws IOException
   */
  private FileStatus[] getAllCommittedTaskPaths(JobContext context) 
    throws IOException {
    Path jobAttemptPath = getJobAttemptPath(context);
    FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
    return fs.listStatus(jobAttemptPath, new CommittedTaskFilter());
  }

  /**
   * Get the directory that the task should write results into.
   * @return the work directory
   * @throws IOException
   */
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  /**
   * Create the temporary directory that is the root of all of the task 
   * work directories.
   * @param context the job's context
   */
  public void setupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = jobAttemptPath.getFileSystem(
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        LOG.error("Mkdirs failed to create " + jobAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in setupJob()");
    }
  }

  /**
   * The job has completed, so do works in commitJobInternal().
   * Could retry on failure if using algorithm 2.
   * @param context the job's context
   */
  public void commitJob(JobContext context) throws IOException {
    int maxAttemptsOnFailure = isCommitJobRepeatable(context) ?
        context.getConfiguration().getInt(FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS,
            FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT) : 1;
    int attempt = 0;
    boolean jobCommitNotFinished = true;
    while (jobCommitNotFinished) {
      try {
        commitJobInternal(context);
        jobCommitNotFinished = false;
      } catch (Exception e) {
        if (++attempt >= maxAttemptsOnFailure) {
          throw e;
        } else {
          LOG.warn("Exception get thrown in job commit, retry (" + attempt +
              ") time.", e);
        }
      }
    }
  }

  /**
   * The job has completed, so do following commit job, include:
   * Move all committed tasks to the final output dir (algorithm 1 only).
   * Delete the temporary directory, including all of the work directories.
   * Create a _SUCCESS file to make it as successful.
   * @param context the job's context
   */
  @VisibleForTesting
  protected void commitJobInternal(JobContext context) throws IOException {
    if (isParallelMoveEnabled()) {
      // Note: Code paths are intentionally copied
      parallelCommitJobInternal(context);
      return;
    }
    if (hasOutputPath()) {
      Path finalOutput = getOutputPath();
      FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());

      if (algorithmVersion == 1) {
        for (FileStatus stat: getAllCommittedTaskPaths(context)) {
          mergePaths(fs, stat, finalOutput, context);
        }
      }

      if (skipCleanup) {
        LOG.info("Skip cleanup the _temporary folders under job's output " +
            "directory in commitJob.");
      } else {
        // delete the _temporary folder and create a _SUCCESS file in the o/p
        // folder
        try {
          cleanupJob(context);
        } catch (IOException e) {
          if (ignoreCleanupFailures) {
            // swallow exceptions in cleanup as user configure to make sure
            // commitJob could be success even when cleanup get failure.
            LOG.error("Error in cleanup job, manually cleanup is needed.", e);
          } else {
            // throw back exception to fail commitJob.
            throw e;
          }
        }
      }
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      // Create a _SUCCESS file in the o/p folder.
      if (context.getConfiguration().getBoolean(
          SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
        Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        // If job commit is repeatable and previous/another AM could write
        // mark file already, we need to set overwritten to be true explicitly
        // in case other FS implementations don't overwritten by default.
        if (isCommitJobRepeatable(context)) {
          fs.create(markerPath, true).close();
        } else {
          fs.create(markerPath).close();
        }
      }
    } else {
      LOG.warn("Output Path is null in commitJob()");
    }
  }

  /**
   * Merge two paths together.  Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fs the File System to use
   * @param from the path data is coming from.
   * @param to the path data is going to.
   * @throws IOException on any error
   */
  private void mergePaths(FileSystem fs, final FileStatus from,
      final Path to, JobContext context) throws IOException {
    try (DurationInfo d = new DurationInfo(LOG,
        false,
        "Merging data from %s to %s", from, to)) {
      reportProgress(context);
      FileStatus toStat;
      try {
        toStat = fs.getFileStatus(to);
      } catch (FileNotFoundException fnfe) {
        toStat = null;
      }

      if (from.isFile()) {
        if (toStat != null) {
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to);
          }
        }

        if (!fs.rename(from.getPath(), to)) {
          throw new IOException("Failed to rename " + from + " to " + to);
        }
      } else if (from.isDirectory()) {
        if (toStat != null) {
          if (!toStat.isDirectory()) {
            if (!fs.delete(to, true)) {
              throw new IOException("Failed to delete " + to);
            }
            renameOrMerge(fs, from, to, context);
          } else {
            //It is a directory so merge everything in the directories
            for (FileStatus subFrom : fs.listStatus(from.getPath())) {
              Path subTo = new Path(to, subFrom.getPath().getName());
              mergePaths(fs, subFrom, subTo, context);
            }
          }
        } else {
          renameOrMerge(fs, from, to, context);
        }
      }
    }
  }

  public boolean isParallelMoveEnabled() {
    // Only available for algo v1
    return (moveThreads > 1 && algorithmVersion == 1);
  }

  void validateParallelMove() throws IOException {
    if (!isParallelMoveEnabled()) {
      throw new IOException("Parallel file move is not enabled. "
          + "moveThreads=" + moveThreads
          + ", algo=" + algorithmVersion);
    }
  }

  void validateThreadPool(ExecutorService pool, BlockingQueue<Future<Void>> futures)
      throws IOException {
    boolean threadPoolEnabled = isParallelMoveEnabled();
    if (!threadPoolEnabled || pool == null || futures == null) {
      String errorMsg = "Thread pool is not configured correctly. "
          + "threadPoolEnabled: " + threadPoolEnabled
          + ", pool: " + pool
          + ", futures: " + futures;
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }

  /**
   * Get executor service for moving files for v1 algorithm.
   * @return executor service
   * @throws IOException on error
   */
  private ExecutorService createExecutorService() throws IOException {
    // intentional validation
    validateParallelMove();

    ExecutorService pool = new ThreadPoolExecutor(moveThreads, moveThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("FileCommitter-v1-move-thread-%d")
            .build(),
        new ThreadPoolExecutor.CallerRunsPolicy()
    );
    LOG.info("Size of move thread pool: {}, pool: {}", moveThreads, pool);
    return pool;
  }

  private void parallelCommitJobInternal(JobContext context) throws IOException {
    // validate to be on safer side.
    validateParallelMove();

    if (hasOutputPath()) {
      Path finalOutput = getOutputPath();
      FileSystem fs = finalOutput.getFileSystem(context.getConfiguration());

      // No need to check for algo ver=1, as this entire code path is for V1.
      try (DurationInfo d = new DurationInfo(LOG, true,
          "Merging data from committed tasks %s", finalOutput)) {
        mergeCommittedTasksInParallel(fs, context, finalOutput);
      }

      if (skipCleanup) {
        LOG.info("Skip cleanup the _temporary folders under job's output " +
            "directory in commitJob.");
      } else {
        // delete the _temporary folder.
        try {
          cleanupJob(context);
        } catch (IOException e) {
          if (ignoreCleanupFailures) {
            // swallow exceptions in cleanup as user configure to make sure
            // commitJob could be success even when cleanup get failure.
            LOG.error("Error in cleanup job, manually cleanup is needed.", e);
          } else {
            // throw back exception to fail commitJob.
            throw e;
          }
        }
      }
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      if (context.getConfiguration().getBoolean(
          SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)) {
        Path markerPath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        // If job commit is repeatable and previous/another AM could write
        // mark file already, we need to set overwritten to be true explicitly
        // in case other FS implementations don't overwritten by default.
        fs.create(markerPath, true).close();
      }
    } else {
      LOG.warn("Output Path is null in commitJob()");
    }
  }

  private void mergeCommittedTasksInParallel(FileSystem fs, JobContext context,
      Path finalOutput) throws IOException {

    // validate to be on safer side.
    validateParallelMove();

    ExecutorService pool = createExecutorService();
    BlockingQueue<Future<Void>> futures = new LinkedBlockingQueue<>();
    try {
      for (FileStatus stat : getAllCommittedTaskPaths(context)) {
        LOG.info("Merging in parallel, from: {}, to: {}, parallelTaskCommitEnabled: {}",
            stat.getPath(), finalOutput, isParallelTaskCommitEnabled);
        if (isParallelTaskCommitEnabled) {
          futures.add(pool.submit(() -> {
            mergePathsInParallel(fs, stat, finalOutput, context, pool, futures);
            return null;
          }));
          numberOfTasks.getAndIncrement();
        } else {
          mergePathsInParallel(fs, stat, finalOutput, context, pool, futures);
        }
      }
      drainFutures(pool, futures);
    } finally {
      shutdownThreadPool(pool);
    }
  }

  /**
   * Drain all future tasks and clean up thread pool.
   *
   * @throws IOException if an exception was raised in any pool.
   */
  private void drainFutures(ExecutorService pool, BlockingQueue<Future<Void>> futures)
      throws IOException {
    if (futures == null) {
      return;
    }
    try {
      int i = 0;
      while(!futures.isEmpty()) {
        futures.take().get();
        if (i % 1000 == 0) {
          LOG.info("Drained task id: {}, overall: {}", i, numberOfTasks.get());
        }
        i++;
      }
    } catch (InterruptedException | ExecutionException e) {
      throw cancelTasks(pool, e);
    }
  }

  @VisibleForTesting
  public int getNumCompletedTasks() {
    return numberOfTasks.get();
  }

  /**
   * Cancel pending tasks in case of exception.
   *
   * @param pool threadpool
   * @param e exception
   * @return IOException
   */
  private IOException cancelTasks(ExecutorService pool, Exception e) {
    if (e == null) {
      // shouldn't land here
      return new IOException("exception was null");
    }
    LOG.error("Cancelling all tasks and shutting down thread pool.", e);
    pool.shutdownNow();
    if (e.getCause() instanceof IOException) {
      return (IOException) e.getCause();
    }
    return new IOException(e);
  }

  /**
   * Shutdown thread pool.
   */
  private void shutdownThreadPool(ExecutorService pool) {
    if (pool != null && !pool.isTerminated()) {
      LOG.info("Shutting down thread pool");
      pool.shutdown();
      numberOfTasks.set(0);
    }
  }

  /**
   * Merge two paths together with parallel threads.
   * Anything in from will be moved into to, if there
   * are any name conflicts while merging the files or directories in from win.
   * @param fs the File System to use
   * @param from the path data is coming from.
   * @param to the path data is going to.
   * @throws IOException on any error
   */
  private void mergePathsInParallel(FileSystem fs, final FileStatus from,
      final Path to, JobContext context, ExecutorService pool,
      BlockingQueue<Future<Void>> futures) throws IOException {
    validateThreadPool(pool, futures);

    try (DurationInfo d = new DurationInfo(LOG, false,
        "Merging data from %s to %s", from.getPath(), to)) {
      reportProgress(context);
      FileStatus toStat;
      try {
        toStat = fs.getFileStatus(to);
      } catch (FileNotFoundException fnfe) {
        toStat = null;
      }

      if (from.isFile()) {
        if (toStat != null) {
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to);
          }
        }

        if (!fs.rename(from.getPath(), to)) {
          throw new IOException("Failed to rename " + from.getPath() + " to " + to);
        }
      } else if (from.isDirectory()) {
        if (toStat != null) {
          if (!toStat.isDirectory()) {
            if (!fs.delete(to, true)) {
              throw new IOException("Failed to delete " + to);
            }
            boolean dirCreated = fs.mkdirs(to);
            LOG.debug("Merging from:{} to:{}, destCreated: {}", from.getPath(), to, dirCreated);
            mergePathsInParallel(fs, from, to, context, pool, futures);
          } else {
            //It is a directory so merge everything in the directories
            LOG.debug("Dir merge from : {} to: {}", from.getPath(), to);
            mergeDirInParallel(fs, from, to, context, pool, futures);
          }
        } else {
          // Init dir to avoid conflicting multi-threaded rename
          boolean dirCreated = fs.mkdirs(to);
          LOG.debug("Created dir upfront. from: {} --> to:{}, totalTasks:{}, destCreated: {}",
              from.getPath(), to, futures.size(), dirCreated);
          mergePathsInParallel(fs, from, to, context, pool, futures);
        }
      }
    }
  }

  /**
   * Merge a directory in parallel fashion.
   *
   * @param fs
   * @param from
   * @param to
   * @param context
   * @throws IOException
   */
  private void mergeDirInParallel(FileSystem fs, FileStatus from, Path to,
      JobContext context, ExecutorService pool,
      BlockingQueue<Future<Void>> futures) throws IOException {

    validateThreadPool(pool, futures);
    RemoteIterator<FileStatus> it = fs.listStatusIterator(from.getPath());
    while(it.hasNext()) {
      FileStatus subFrom = it.next();
      Path subTo = new Path(to, subFrom.getPath().getName());
      LOG.debug("Merge from: {}, to: {} in parallel", subFrom.getPath(), subTo);
      futures.add(pool.submit(() -> {
        mergePathsInParallel(fs, subFrom, subTo, context, pool, futures);
        return null;
      }));
      numberOfTasks.getAndIncrement();
    }
  }

  private void reportProgress(JobContext context) {
    if (context instanceof Progressable) {
      ((Progressable) context).progress();
    }
  }

  private void renameOrMerge(FileSystem fs, FileStatus from, Path to,
      JobContext context) throws IOException {
    if (algorithmVersion == 1) {
      if (!fs.rename(from.getPath(), to)) {
        throw new IOException("Failed to rename " + from + " to " + to);
      }
    } else {
      fs.mkdirs(to);
      for (FileStatus subFrom : fs.listStatus(from.getPath())) {
        Path subTo = new Path(to, subFrom.getPath().getName());
        mergePaths(fs, subFrom, subTo, context);
      }
    }
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path pendingJobAttemptsPath = getPendingJobAttemptsPath();
      FileSystem fs = pendingJobAttemptsPath
          .getFileSystem(context.getConfiguration());
      // if job allow repeatable commit and pendingJobAttemptsPath could be
      // deleted by previous AM, we should tolerate FileNotFoundException in
      // this case.
      try {
        fs.delete(pendingJobAttemptsPath, true);
      } catch (FileNotFoundException e) {
        if (!isCommitJobRepeatable(context)) {
          throw e;
        }
      }
    } else {
      LOG.warn("Output Path is null in cleanupJob()");
    }
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   */
  @Override
  public void abortJob(JobContext context, JobStatus.State state) 
  throws IOException {
    // delete the _temporary folder
    cleanupJob(context);
  }
  
  /**
   * No task setup required.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the 
    // task is writing.
  }

  /**
   * Move the files from the work directory to the job output directory
   * @param context the task context
   */
  @Override
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    commitTask(context, null);
  }

  @Private
  public void commitTask(TaskAttemptContext context, Path taskAttemptPath) 
      throws IOException {

    TaskAttemptID attemptId = context.getTaskAttemptID();
    if (hasOutputPath()) {
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      FileStatus taskAttemptDirStatus;
      try {
        taskAttemptDirStatus = fs.getFileStatus(taskAttemptPath);
      } catch (FileNotFoundException e) {
        taskAttemptDirStatus = null;
      }

      if (taskAttemptDirStatus != null) {
        if (algorithmVersion == 1) {
          Path committedTaskPath = getCommittedTaskPath(context);
          if (fs.exists(committedTaskPath)) {
             if (!fs.delete(committedTaskPath, true)) {
               throw new IOException("Could not delete " + committedTaskPath);
             }
          }
          if (!fs.rename(taskAttemptPath, committedTaskPath)) {
            throw new IOException("Could not rename " + taskAttemptPath + " to "
                + committedTaskPath);
          }
          LOG.info("Saved output of task '" + attemptId + "' to " +
              committedTaskPath);
        } else {
          // directly merge everything from taskAttemptPath to output directory
          mergePaths(fs, taskAttemptDirStatus, outputPath, context);
          LOG.info("Saved output of task '" + attemptId + "' to " +
              outputPath);

          if (context.getConfiguration().getBoolean(
              FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED,
              FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED_DEFAULT)) {
            LOG.debug(String.format(
                "Deleting the temporary directory of '%s': '%s'",
                attemptId, taskAttemptPath));
            if(!fs.delete(taskAttemptPath, true)) {
              LOG.warn("Could not delete " + taskAttemptPath);
            }
          }
        }
      } else {
        LOG.warn("No Output found for " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in commitTask()");
    }
  }

  /**
   * Delete the work directory
   * @throws IOException 
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    abortTask(context, null);
  }

  @Private
  public void abortTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
    if (hasOutputPath()) { 
      context.progress();
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      if(!fs.delete(taskAttemptPath, true)) {
        LOG.warn("Could not delete "+taskAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in abortTask()");
    }
  }

  /**
   * Did this task write any files in the work directory?
   * @param context the task's context
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context
                                 ) throws IOException {
    return needsTaskCommit(context, null);
  }

  @Private
  public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath
    ) throws IOException {
    if(hasOutputPath()) {
      if(taskAttemptPath == null) {
        taskAttemptPath = getTaskAttemptPath(context);
      }
      FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
      return fs.exists(taskAttemptPath);
    }
    return false;
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return true;
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext context) throws IOException {
    return algorithmVersion == 2;
  }

  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    if(hasOutputPath()) {
      context.progress();
      TaskAttemptID attemptId = context.getTaskAttemptID();
      int previousAttempt = getAppAttemptId(context) - 1;
      if (previousAttempt < 0) {
        throw new IOException ("Cannot recover task output for first attempt...");
      }

      Path previousCommittedTaskPath = getCommittedTaskPath(
          previousAttempt, context);
      FileSystem fs = previousCommittedTaskPath.getFileSystem(context.getConfiguration());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Trying to recover task from " + previousCommittedTaskPath);
      }
      if (algorithmVersion == 1) {
        if (fs.exists(previousCommittedTaskPath)) {
          Path committedTaskPath = getCommittedTaskPath(context);
          if (!fs.delete(committedTaskPath, true) &&
              fs.exists(committedTaskPath)) {
            throw new IOException("Could not delete " + committedTaskPath);
          }
          //Rename can fail if the parent directory does not yet exist.
          Path committedParent = committedTaskPath.getParent();
          fs.mkdirs(committedParent);
          if (!fs.rename(previousCommittedTaskPath, committedTaskPath)) {
            throw new IOException("Could not rename " + previousCommittedTaskPath +
                " to " + committedTaskPath);
          }
        } else {
            LOG.warn(attemptId+" had no output to recover.");
        }
      } else {
        // essentially a no-op, but for backwards compatibility
        // after upgrade to the new fileOutputCommitter,
        // check if there are any output left in committedTaskPath
        try {
          FileStatus from = fs.getFileStatus(previousCommittedTaskPath);
          LOG.info("Recovering task for upgrading scenario, moving files from "
              + previousCommittedTaskPath + " to " + outputPath);
          mergePaths(fs, from, outputPath, context);
        } catch (FileNotFoundException ignored) {
        }
        LOG.info("Done recovering task " + attemptId);
      }
    } else {
      LOG.warn("Output Path is null in recoverTask()");
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "FileOutputCommitter{");
    sb.append(super.toString()).append("; ");
    sb.append("outputPath=").append(outputPath);
    sb.append(", workPath=").append(workPath);
    sb.append(", algorithmVersion=").append(algorithmVersion);
    sb.append(", skipCleanup=").append(skipCleanup);
    sb.append(", ignoreCleanupFailures=").append(ignoreCleanupFailures);
    sb.append('}');
    return sb.toString();
  }
}
