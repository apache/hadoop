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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;

/**
 * Utility class to fetch block locations for specified Input paths using a
 * configured number of threads.
 * The thread count is determined from the value of
 * "mapreduce.input.fileinputformat.list-status.num-threads" in the
 * configuration.
 */
@Private
public class LocatedFileStatusFetcher implements IOStatisticsSource {

  public static final Logger LOG =
      LoggerFactory.getLogger(LocatedFileStatusFetcher.class.getName());
  private final Path[] inputDirs;
  private final PathFilter inputFilter;
  private final Configuration conf;
  private final boolean recursive;
  private final boolean newApi;
  
  private final ExecutorService rawExec;
  private final ListeningExecutorService exec;
  private final BlockingQueue<List<FileStatus>> resultQueue;
  private final List<IOException> invalidInputErrors = new LinkedList<>();

  private final ProcessInitialInputPathCallback processInitialInputPathCallback = 
      new ProcessInitialInputPathCallback();
  private final ProcessInputDirCallback processInputDirCallback = 
      new ProcessInputDirCallback();

  private final AtomicInteger runningTasks = new AtomicInteger(0);

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();

  private volatile Throwable unknownError;

  /**
   * Demand created IO Statistics: only if the filesystem
   * returns statistics does this fetch collect them.
   */
  private IOStatisticsSnapshot iostats;

  /**
   * Instantiate.
   * The newApi switch is only used to configure what exception is raised
   * on failure of {@link #getFileStatuses()}, it does not change the algorithm.
   * @param conf configuration for the job
   * @param dirs the initial list of paths
   * @param recursive whether to traverse the paths recursively
   * @param inputFilter inputFilter to apply to the resulting paths
   * @param newApi whether using the mapred or mapreduce API
   * @throws InterruptedException
   * @throws IOException
   */
  public LocatedFileStatusFetcher(Configuration conf, Path[] dirs,
      boolean recursive, PathFilter inputFilter, boolean newApi)
      throws InterruptedException, IOException {
    int numThreads = conf.getInt(FileInputFormat.LIST_STATUS_NUM_THREADS,
        FileInputFormat.DEFAULT_LIST_STATUS_NUM_THREADS);
    LOG.debug("Instantiated LocatedFileStatusFetcher with {} threads",
        numThreads);
    rawExec = HadoopExecutors.newFixedThreadPool(
        numThreads,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("GetFileInfo #%d").build());
    exec = MoreExecutors.listeningDecorator(rawExec);
    resultQueue = new LinkedBlockingQueue<>();
    this.conf = conf;
    this.inputDirs = dirs;
    this.recursive = recursive;
    this.inputFilter = inputFilter;
    this.newApi = newApi;
  }

  /**
   * Start executing and return FileStatuses based on the parameters specified.
   * @return fetched file statuses
   * @throws InterruptedException interruption waiting for results.
   * @throws IOException IO failure or other error.
   * @throws InvalidInputException on an invalid input and the old API
   * @throws org.apache.hadoop.mapreduce.lib.input.InvalidInputException on an
   *         invalid input and the new API.
   */
  public Iterable<FileStatus> getFileStatuses() throws InterruptedException,
      IOException {
    // Increment to make sure a race between the first thread completing and the
    // rest being scheduled does not lead to a termination.
    runningTasks.incrementAndGet();
    for (Path p : inputDirs) {
      LOG.debug("Queuing scan of directory {}", p);
      runningTasks.incrementAndGet();
      ListenableFuture<ProcessInitialInputPathCallable.Result> future = exec
          .submit(new ProcessInitialInputPathCallable(p, conf, inputFilter));
      Futures.addCallback(future, processInitialInputPathCallback,
          MoreExecutors.directExecutor());
    }

    runningTasks.decrementAndGet();

    lock.lock();
    try {
      LOG.debug("Waiting scan completion");
      while (runningTasks.get() != 0 && unknownError == null) {
        condition.await();
      }
    } finally {
      lock.unlock();
      // either the scan completed or an error was raised.
      // in the case of an error shutting down the executor will interrupt all
      // active threads, which can add noise to the logs.
      LOG.debug("Scan complete: shutting down");
      this.exec.shutdownNow();
    }

    if (this.unknownError != null) {
      LOG.debug("Scan failed", this.unknownError);
      if (this.unknownError instanceof Error) {
        throw (Error) this.unknownError;
      } else if (this.unknownError instanceof RuntimeException) {
        throw (RuntimeException) this.unknownError;
      } else if (this.unknownError instanceof IOException) {
        throw (IOException) this.unknownError;
      } else if (this.unknownError instanceof InterruptedException) {
        throw (InterruptedException) this.unknownError;
      } else {
        throw new IOException(this.unknownError);
      }
    }
    if (!this.invalidInputErrors.isEmpty()) {
      LOG.debug("Invalid Input Errors raised");
      for (IOException error : invalidInputErrors) {
        LOG.debug("Error", error);
      }
      if (this.newApi) {
        throw new org.apache.hadoop.mapreduce.lib.input.InvalidInputException(
            invalidInputErrors);
      } else {
        throw new InvalidInputException(invalidInputErrors);
      }
    }
    return Iterables.concat(resultQueue);
  }

  /**
   * Collect misconfigured Input errors. Errors while actually reading file info
   * are reported immediately.
   */
  private void registerInvalidInputError(List<IOException> errors) {
    synchronized (this) {
      this.invalidInputErrors.addAll(errors);
    }
  }

  /**
   * Register fatal errors - example an IOException while accessing a file or a
   * full execution queue.
   */
  private void registerError(Throwable t) {
    LOG.debug("Error", t);
    lock.lock();
    try {
      if (unknownError == null) {
        unknownError = t;
        condition.signal();
      }

    } finally {
      lock.unlock();
    }
  }

  private void decrementRunningAndCheckCompletion() {
    lock.lock();
    try {
      if (runningTasks.decrementAndGet() == 0) {
        condition.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return any IOStatistics collected during listing.
   * @return IO stats accrued.
   */
  @Override
  public synchronized IOStatistics getIOStatistics() {
    return iostats;
  }

  /**
   * Add the statistics of an individual thread's scan.
   * @param stats possibly null statistics.
   */
  private void addResultStatistics(IOStatistics stats) {
    if (stats != null) {
      // demand creation of IO statistics.
      synchronized (this) {
        LOG.debug("Adding IOStatistics: {}", stats);
        if (iostats == null) {
          // demand create the statistics
          iostats = snapshotIOStatistics(stats);
        } else {
          iostats.aggregate(stats);
        }
      }
    }
  }

  @Override
  public String toString() {
    final IOStatistics ioStatistics = getIOStatistics();
    StringJoiner stringJoiner = new StringJoiner(", ",
        LocatedFileStatusFetcher.class.getSimpleName() + "[", "]");
    if (ioStatistics != null) {
      stringJoiner.add("IOStatistics=" + ioStatistics);
    }
    return stringJoiner.toString();
  }

  /**
   * Retrieves block locations for the given @link {@link FileStatus}, and adds
   * additional paths to the process queue if required.
   */
  private static class ProcessInputDirCallable implements
      Callable<ProcessInputDirCallable.Result> {

    private final FileSystem fs;
    private final FileStatus fileStatus;
    private final boolean recursive;
    private final PathFilter inputFilter;

    ProcessInputDirCallable(FileSystem fs, FileStatus fileStatus,
        boolean recursive, PathFilter inputFilter) {
      this.fs = fs;
      this.fileStatus = fileStatus;
      this.recursive = recursive;
      this.inputFilter = inputFilter;
    }

    @Override
    public Result call() throws Exception {
      Result result = new Result();
      result.fs = fs;
      LOG.debug("ProcessInputDirCallable {}", fileStatus);
      if (fileStatus.isDirectory()) {
        RemoteIterator<LocatedFileStatus> iter = fs
            .listLocatedStatus(fileStatus.getPath());
        while (iter.hasNext()) {
          LocatedFileStatus stat = iter.next();
          if (inputFilter.accept(stat.getPath())) {
            if (recursive && stat.isDirectory()) {
              result.dirsNeedingRecursiveCalls.add(stat);
            } else {
              result.locatedFileStatuses.add(stat);
            }
          }
        }
        // aggregate any stats
        result.stats = retrieveIOStatistics(iter);
      } else {
        result.locatedFileStatuses.add(fileStatus);
      }
      return result;
    }

    private static class Result {
      private List<FileStatus> locatedFileStatuses = new LinkedList<>();
      private List<FileStatus> dirsNeedingRecursiveCalls = new LinkedList<>();
      private FileSystem fs;
      private IOStatistics stats;
    }
  }

  /**
   * The callback handler to handle results generated by
   * {@link ProcessInputDirCallable}. This populates the final result set.
   * 
   */
  private class ProcessInputDirCallback implements
      FutureCallback<ProcessInputDirCallable.Result> {

    @Override
    public void onSuccess(ProcessInputDirCallable.Result result) {
      try {
        addResultStatistics(result.stats);
        if (!result.locatedFileStatuses.isEmpty()) {
          resultQueue.add(result.locatedFileStatuses);
        }
        if (!result.dirsNeedingRecursiveCalls.isEmpty()) {
          for (FileStatus fileStatus : result.dirsNeedingRecursiveCalls) {
            LOG.debug("Queueing directory scan {}", fileStatus.getPath());
            runningTasks.incrementAndGet();
            ListenableFuture<ProcessInputDirCallable.Result> future = exec
                .submit(new ProcessInputDirCallable(result.fs, fileStatus,
                    recursive, inputFilter));
            Futures.addCallback(future, processInputDirCallback,
                MoreExecutors.directExecutor());
          }
        }
        decrementRunningAndCheckCompletion();
      } catch (Throwable t) { // Error within the callback itself.
        registerError(t);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Any generated exceptions. Leads to immediate termination.
      registerError(t);
    }
  }


  /**
   * Processes an initial Input Path pattern through the globber and PathFilter
   * to generate a list of files which need further processing.
   */
  private static class ProcessInitialInputPathCallable implements
      Callable<ProcessInitialInputPathCallable.Result> {

    private final Path path;
    private final Configuration conf;
    private final PathFilter inputFilter;

    public ProcessInitialInputPathCallable(Path path, Configuration conf,
        PathFilter pathFilter) {
      this.path = path;
      this.conf = conf;
      this.inputFilter = pathFilter;
    }

    @Override
    public Result call() throws Exception {
      Result result = new Result();
      FileSystem fs = path.getFileSystem(conf);
      result.fs = fs;
      LOG.debug("ProcessInitialInputPathCallable path {}", path);
      FileStatus[] matches = fs.globStatus(path, inputFilter);
      if (matches == null) {
        result.addError(new IOException("Input path does not exist: " + path));
      } else if (matches.length == 0) {
        result.addError(new IOException("Input Pattern " + path
            + " matches 0 files"));
      } else {
        result.matchedFileStatuses = matches;
      }
      return result;
    }

    private static class Result {
      private List<IOException> errors;
      private FileStatus[] matchedFileStatuses;
      private FileSystem fs;

      void addError(IOException ioe) {
        if (errors == null) {
          errors = new LinkedList<IOException>();
        }
        errors.add(ioe);
      }
    }
  }

  /**
   * The callback handler to handle results generated by
   * {@link ProcessInitialInputPathCallable}.
   * 
   */
  private class ProcessInitialInputPathCallback implements
      FutureCallback<ProcessInitialInputPathCallable.Result> {

    @Override
    public void onSuccess(ProcessInitialInputPathCallable.Result result) {
      try {
        if (result.errors != null) {
          registerInvalidInputError(result.errors);
        }
        if (result.matchedFileStatuses != null) {
          for (FileStatus matched : result.matchedFileStatuses) {
            runningTasks.incrementAndGet();
            ListenableFuture<ProcessInputDirCallable.Result> future = exec
                .submit(new ProcessInputDirCallable(result.fs, matched,
                    recursive, inputFilter));
            Futures.addCallback(future, processInputDirCallback,
                MoreExecutors.directExecutor());
          }
        }
        decrementRunningAndCheckCompletion();
      } catch (Throwable t) { // Exception within the callback
        registerError(t);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Any generated exceptions. Leads to immediate termination.
      registerError(t);
    }
  }

  @VisibleForTesting
  ListeningExecutorService getListeningExecutorService() {
    return exec;
  }

}
