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

package org.apache.hadoop.util.functional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.functional.RemoteIterators.remoteIteratorFromIterable;

/**
 * Utility class for parallel execution, takes closures for the various
 * actions.
 * There is no retry logic: it is expected to be handled by the closures.
 * From {@code org.apache.hadoop.fs.s3a.commit.Tasks} which came from
 * the Netflix committer patch.
 * Apache Iceberg has its own version of this, with a common ancestor
 * at some point in its history.
 * A key difference with this class is that the iterator is always,
 * internally, an {@link RemoteIterator}.
 * This is to allow tasks to be scheduled while incremental operations
 * such as paged directory listings are still collecting in results.
 *
 * While awaiting completion, this thread spins and sleeps a time of
 * {@link #SLEEP_INTERVAL_AWAITING_COMPLETION}, which, being a
 * busy-wait, is inefficient.
 * There's an implicit assumption that remote IO is being performed, and
 * so this is not impacting throughput/performance.
 *
 * History:
 * This class came with the Netflix contributions to the S3A committers
 * in HADOOP-13786.
 * It was moved into hadoop-common for use in the manifest committer and
 * anywhere else it is needed, and renamed in the process as
 * "Tasks" has too many meanings in the hadoop source.
 * The iterator was then changed from a normal java iterable
 * to a hadoop {@link org.apache.hadoop.fs.RemoteIterator}.
 * This allows a task pool to be supplied with incremental listings
 * from object stores, scheduling work as pages of listing
 * results come in, rather than blocking until the entire
 * directory/directory tree etc has been enumerated.
 *
 * There is a variant of this in Apache Iceberg in
 * {@code org.apache.iceberg.util.Tasks}
 * That is not derived from any version in the hadoop codebase, it
 * just shares a common ancestor somewhere in the Netflix codebase.
 * It is the more sophisticated version.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class TaskPool {
  private static final Logger LOG =
      LoggerFactory.getLogger(TaskPool.class);

  /**
   * Interval in milliseconds to await completion.
   */
  private static final int SLEEP_INTERVAL_AWAITING_COMPLETION = 10;

  private TaskPool() {
  }

  /**
   * Callback invoked to process an item.
   * @param <I> item type being processed
   * @param <E> exception class which may be raised
   */
  @FunctionalInterface
  public interface Task<I, E extends Exception> {
    void run(I item) throws E;
  }

  /**
   * Callback invoked on a failure.
   * @param <I> item type being processed
   * @param <E> exception class which may be raised
   */
  @FunctionalInterface
  public interface FailureTask<I, E extends Exception> {

    /**
     * process a failure.
     * @param item item the task is processing
     * @param exception the exception which was raised.
     * @throws E Exception of type E
     */
    void run(I item, Exception exception) throws E;
  }

  /**
   * Builder for task execution.
   * @param <I> item type
   */
  public static class Builder<I> {
    private final RemoteIterator<I> items;
    private Submitter service = null;
    private FailureTask<I, ?> onFailure = null;
    private boolean stopOnFailure = false;
    private boolean suppressExceptions = false;
    private Task<I, ?> revertTask = null;
    private boolean stopRevertsOnFailure = false;
    private Task<I, ?> abortTask = null;
    private boolean stopAbortsOnFailure = false;
    private int sleepInterval = SLEEP_INTERVAL_AWAITING_COMPLETION;

    /**
     * IOStatisticsContext to switch to in all threads
     * taking part in the commit operation.
     * This ensures that the IOStatistics collected in the
     * worker threads will be aggregated into the total statistics
     * of the thread calling the committer commit/abort methods.
     */
    private IOStatisticsContext ioStatisticsContext = null;

    /**
     * Create the builder.
     * @param items items to process
     */
    Builder(RemoteIterator<I> items) {
      this.items = requireNonNull(items, "items");
    }

    /**
     * Create the builder.
     * @param items items to process
     */
    Builder(Iterable<I> items) {
      this(remoteIteratorFromIterable(items));
    }

    /**
     * Declare executor service: if null, the tasks are executed in a single
     * thread.
     * @param submitter service to schedule tasks with.
     * @return this builder.
     */
    public Builder<I> executeWith(@Nullable Submitter submitter) {

      this.service = submitter;
      return this;
    }

    /**
     * Task to invoke on failure.
     * @param task task
     * @return the builder
     */
    public Builder<I> onFailure(FailureTask<I, ?> task) {
      this.onFailure = task;
      return this;
    }

    public Builder<I> stopOnFailure() {
      this.stopOnFailure = true;
      return this;
    }

    /**
     * Suppress exceptions from tasks.
     * RemoteIterator exceptions are not suppressable.
     * @return the builder.
     */
    public Builder<I> suppressExceptions() {
      return suppressExceptions(true);
    }

    /**
     * Suppress exceptions from tasks.
     * RemoteIterator exceptions are not suppressable.
     * @param suppress new value
     * @return the builder.
     */
    public Builder<I> suppressExceptions(boolean suppress) {
      this.suppressExceptions = suppress;
      return this;
    }

    /**
     * Task to revert with after another task failed.
     * @param task task to execute
     * @return the builder
     */
    public Builder<I> revertWith(Task<I, ?> task) {
      this.revertTask = task;
      return this;
    }

    /**
     * Stop trying to revert if one operation fails.
     * @return the builder
     */
    public Builder<I> stopRevertsOnFailure() {
      this.stopRevertsOnFailure = true;
      return this;
    }

    /**
     * Task to abort with after another task failed.
     * @param task task to execute
     * @return the builder
     */
    public Builder<I> abortWith(Task<I, ?> task) {
      this.abortTask = task;
      return this;
    }

    /**
     * Stop trying to abort if one operation fails.
     * @return the builder
     */
    public Builder<I> stopAbortsOnFailure() {
      this.stopAbortsOnFailure = true;
      return this;
    }

    /**
     * Set the sleep interval.
     * @param value new value
     * @return the builder
     */
    public Builder<I> sleepInterval(final int value) {
      sleepInterval = value;
      return this;
    }

    /**
     * Execute the task across the data.
     * @param task task to execute
     * @param <E> exception which may be raised in execution.
     * @return true if the operation executed successfully
     * @throws E any exception raised.
     * @throws IOException IOExceptions raised by remote iterator or in execution.
     */
    public <E extends Exception> boolean run(Task<I, E> task) throws E, IOException {
      requireNonNull(items, "items");
      if (!items.hasNext()) {
        // if there are no items, return without worrying about
        // execution pools, errors etc.
        return true;
      }
      if (service != null) {
        // thread pool, so run in parallel
        return runParallel(task);
      } else {
        // single threaded execution.
        return runSingleThreaded(task);
      }
    }

    /**
     * Single threaded execution.
     * @param task task to execute
     * @param <E> exception which may be raised in execution.
     * @return true if the operation executed successfully
     * @throws E any exception raised.
     * @throws IOException IOExceptions raised by remote iterator or in execution.
     */
    private <E extends Exception> boolean runSingleThreaded(Task<I, E> task)
        throws E, IOException {
      List<I> succeeded = new ArrayList<>();
      List<Exception> exceptions = new ArrayList<>();

      RemoteIterator<I> iterator = items;
      boolean threw = true;
      try {
        while (iterator.hasNext()) {
          I item = iterator.next();
          try {
            task.run(item);
            succeeded.add(item);

          } catch (Exception e) {
            exceptions.add(e);

            if (onFailure != null) {
              try {
                onFailure.run(item, e);
              } catch (Exception failException) {
                LOG.error("Failed to clean up on failure", e);
                // keep going
              }
            }

            if (stopOnFailure) {
              break;
            }
          }
        }

        threw = false;
      } catch (IOException iteratorIOE) {
        // an IOE is reaised here during iteration
        LOG.debug("IOException when iterating through {}", iterator, iteratorIOE);
        throw iteratorIOE;
      } finally {
        // threw handles exceptions that were *not* caught by the catch block,
        // and exceptions that were caught and possibly handled by onFailure
        // are kept in exceptions.
        if (threw || !exceptions.isEmpty()) {
          if (revertTask != null) {
            boolean failed = false;
            for (I item : succeeded) {
              try {
                revertTask.run(item);
              } catch (Exception e) {
                LOG.error("Failed to revert task", e);
                failed = true;
                // keep going
              }
              if (stopRevertsOnFailure && failed) {
                break;
              }
            }
          }

          if (abortTask != null) {
            boolean failed = false;
            while (iterator.hasNext()) {
              try {
                abortTask.run(iterator.next());
              } catch (Exception e) {
                failed = true;
                LOG.error("Failed to abort task", e);
                // keep going
              }
              if (stopAbortsOnFailure && failed) {
                break;
              }
            }
          }
        }
      }

      if (!suppressExceptions && !exceptions.isEmpty()) {
        TaskPool.<E>throwOne(exceptions);
      }

      return exceptions.isEmpty();
    }

    /**
     * Parallel execution.
     * All tasks run within the same IOStatisticsContext as the
     * thread calling this method.
     * @param task task to execute
     * @param <E> exception which may be raised in execution.
     * @return true if the operation executed successfully
     * @throws E any exception raised.
     * @throws IOException IOExceptions raised by remote iterator or in execution.
     */
    private <E extends Exception> boolean runParallel(final Task<I, E> task)
        throws E, IOException {
      final Queue<I> succeeded = new ConcurrentLinkedQueue<>();
      final Queue<Exception> exceptions = new ConcurrentLinkedQueue<>();
      final AtomicBoolean taskFailed = new AtomicBoolean(false);
      final AtomicBoolean abortFailed = new AtomicBoolean(false);
      final AtomicBoolean revertFailed = new AtomicBoolean(false);

      List<Future<?>> futures = new ArrayList<>();
      ioStatisticsContext = IOStatisticsContext.getCurrentIOStatisticsContext();

      IOException iteratorIOE = null;
      final RemoteIterator<I> iterator = this.items;
      try {
        while (iterator.hasNext()) {
          final I item = iterator.next();
          // submit a task for each item that will either run or abort the task
          futures.add(service.submit(() -> {
            setStatisticsContext();
            try {
              if (!(stopOnFailure && taskFailed.get())) {
                // prepare and run the task
                boolean threw = true;
                try {
                  LOG.debug("Executing task");
                  task.run(item);
                  succeeded.add(item);
                  LOG.debug("Task succeeded");

                  threw = false;

                } catch (Exception e) {
                  taskFailed.set(true);
                  exceptions.add(e);
                  LOG.info("Task failed {}", e.toString());
                  LOG.debug("Task failed", e);

                  if (onFailure != null) {
                    try {
                      onFailure.run(item, e);
                    } catch (Exception failException) {
                      LOG.warn("Failed to clean up on failure", e);
                      // swallow the exception
                    }
                  }
                } finally {
                  if (threw) {
                    taskFailed.set(true);
                  }
                }

              } else if (abortTask != null) {
                // abort the task instead of running it
                if (stopAbortsOnFailure && abortFailed.get()) {
                  return;
                }

                boolean failed = true;
                try {
                  LOG.info("Aborting task");
                  abortTask.run(item);
                  failed = false;
                } catch (Exception e) {
                  LOG.error("Failed to abort task", e);
                  // swallow the exception
                } finally {
                  if (failed) {
                    abortFailed.set(true);
                  }
                }
              }
            } finally {
              resetStatisticsContext();
            }
          }));
        }
      } catch (IOException e) {
        // iterator failure.
        LOG.debug("IOException when iterating through {}", iterator, e);
        iteratorIOE = e;
        // mark as a task failure so all submitted tasks will halt/abort
        taskFailed.set(true);
      }
      // let the above tasks complete (or abort)
      waitFor(futures, sleepInterval);
      int futureCount = futures.size();
      futures.clear();

      if (taskFailed.get() && revertTask != null) {
        // at least one task failed, revert any that succeeded
        LOG.info("Reverting all {} succeeded tasks from {} futures",
            succeeded.size(), futureCount);
        for (final I item : succeeded) {
          futures.add(service.submit(() -> {
            if (stopRevertsOnFailure && revertFailed.get()) {
              return;
            }

            boolean failed = true;
            setStatisticsContext();
            try {
              revertTask.run(item);
              failed = false;
            } catch (Exception e) {
              LOG.error("Failed to revert task", e);
              // swallow the exception
            } finally {
              if (failed) {
                revertFailed.set(true);
              }
              resetStatisticsContext();
            }
          }));
        }

        // let the revert tasks complete
        waitFor(futures, sleepInterval);
      }

      // give priority to execution exceptions over
      // iterator exceptions.
      if (!suppressExceptions && !exceptions.isEmpty()) {
        // there's an exception list to build up, cast and throw.
        TaskPool.<E>throwOne(exceptions);
      }

      // raise any iterator exception.
      // this can not be suppressed.
      if (iteratorIOE != null) {
        throw iteratorIOE;
      }

      // return true if all tasks succeeded.
      return !taskFailed.get();
    }

    /**
     * Set the statistics context for this thread.
     */
    private void setStatisticsContext() {
      if (ioStatisticsContext != null) {
        IOStatisticsContext.setThreadIOStatisticsContext(ioStatisticsContext);
      }
    }

    /**
     * Reset the statistics context if it was set earlier.
     * This unbinds the current thread from any statistics
     * context.
     */
    private void resetStatisticsContext() {
      if (ioStatisticsContext != null) {
        IOStatisticsContext.setThreadIOStatisticsContext(null);
      }
    }
  }

  /**
   * Wait for all the futures to complete; there's a small sleep between
   * each iteration; enough to yield the CPU.
   * @param futures futures.
   * @param sleepInterval Interval in milliseconds to await completion.
   */
  private static void waitFor(Collection<Future<?>> futures, int sleepInterval) {
    int size = futures.size();
    LOG.debug("Waiting for {} tasks to complete", size);
    int oldNumFinished = 0;
    while (true) {
      int numFinished = (int) futures.stream().filter(Future::isDone).count();

      if (oldNumFinished != numFinished) {
        LOG.debug("Finished count -> {}/{}", numFinished, size);
        oldNumFinished = numFinished;
      }

      if (numFinished == size) {
        // all of the futures are done, stop looping
        break;
      } else {
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
          futures.forEach(future -> future.cancel(true));
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  /**
   * Create a task builder for the iterable.
   * @param items item source.
   * @param <I> type of result.
   * @return builder.
   */
  public static <I> Builder<I> foreach(Iterable<I> items) {
    return new Builder<>(requireNonNull(items, "items"));
  }

  /**
   * Create a task builder for the remote iterator.
   * @param items item source.
   * @param <I> type of result.
   * @return builder.
   */
  public static <I> Builder<I> foreach(RemoteIterator<I> items) {
    return new Builder<>(items);
  }

  public static <I> Builder<I> foreach(I[] items) {
    return new Builder<>(Arrays.asList(requireNonNull(items, "items")));
  }

  /**
   * Throw one exception, adding the others as suppressed
   * exceptions attached to the one thrown.
   * This method never completes normally.
   * @param exceptions collection of exceptions
   * @param <E> class of exceptions
   * @throws E an extracted exception.
   */
  private static <E extends Exception> void throwOne(
      Collection<Exception> exceptions)
      throws E {
    Iterator<Exception> iter = exceptions.iterator();
    Exception e = iter.next();
    Class<? extends Exception> exceptionClass = e.getClass();

    while (iter.hasNext()) {
      Exception other = iter.next();
      if (!exceptionClass.isInstance(other)) {
        e.addSuppressed(other);
      }
    }

    TaskPool.<E>castAndThrow(e);
  }

  /**
   * Raise an exception of the declared type.
   * This method never completes normally.
   * @param e exception
   * @param <E> class of exceptions
   * @throws E a recast exception.
   */
  @SuppressWarnings("unchecked")
  private static <E extends Exception> void castAndThrow(Exception e) throws E {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    throw (E) e;
  }

  /**
   * Interface to whatever lets us submit tasks.
   */
  public interface Submitter {

    /**
     * Submit work.
     * @param task task to execute
     * @return the future of the submitted task.
     */
    Future<?> submit(Runnable task);
  }

}
