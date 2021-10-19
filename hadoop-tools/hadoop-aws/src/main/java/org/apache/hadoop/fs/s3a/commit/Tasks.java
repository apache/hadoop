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

package org.apache.hadoop.fs.s3a.commit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for parallel execution, takes closures for the various
 * actions.
 * There is no retry logic: it is expected to be handled by the closures.
 */
public final class Tasks {
  private static final Logger LOG = LoggerFactory.getLogger(Tasks.class);

  private Tasks() {
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
    private final Iterable<I> items;
    private Submitter service = null;
    private FailureTask<I, ?> onFailure = null;
    private boolean stopOnFailure = false;
    private boolean suppressExceptions = false;
    private Task<I, ?> revertTask = null;
    private boolean stopRevertsOnFailure = false;
    private Task<I, ?> abortTask = null;
    private boolean stopAbortsOnFailure = false;

    /**
     * Create the builder.
     * @param items items to process
     */
    Builder(Iterable<I> items) {
      this.items = items;
    }

    /**
     * Declare executor service: if null, the tasks are executed in a single
     * thread.
     * @param submitter service to schedule tasks with.
     * @return this builder.
     */
    public Builder<I> executeWith(Submitter submitter) {
      this.service = submitter;
      return this;
    }

    public Builder<I> onFailure(FailureTask<I, ?> task) {
      this.onFailure = task;
      return this;
    }

    public Builder<I> stopOnFailure() {
      this.stopOnFailure = true;
      return this;
    }

    public Builder<I> suppressExceptions() {
      return suppressExceptions(true);
    }

    public Builder<I> suppressExceptions(boolean suppress) {
      this.suppressExceptions = suppress;
      return this;
    }

    public Builder<I> revertWith(Task<I, ?> task) {
      this.revertTask = task;
      return this;
    }

    public Builder<I> stopRevertsOnFailure() {
      this.stopRevertsOnFailure = true;
      return this;
    }

    public Builder<I> abortWith(Task<I, ?> task) {
      this.abortTask = task;
      return this;
    }

    public Builder<I> stopAbortsOnFailure() {
      this.stopAbortsOnFailure = true;
      return this;
    }

    public <E extends Exception> boolean run(Task<I, E> task) throws E {
      if (service != null) {
        return runParallel(task);
      } else {
        return runSingleThreaded(task);
      }
    }

    private <E extends Exception> boolean runSingleThreaded(Task<I, E> task)
        throws E {
      List<I> succeeded = new ArrayList<>();
      List<Exception> exceptions = new ArrayList<>();

      Iterator<I> iterator = items.iterator();
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
        Tasks.<E>throwOne(exceptions);
      }

      return !threw && exceptions.isEmpty();
    }

    private <E extends Exception> boolean runParallel(final Task<I, E> task)
        throws E {
      final Queue<I> succeeded = new ConcurrentLinkedQueue<>();
      final Queue<Exception> exceptions = new ConcurrentLinkedQueue<>();
      final AtomicBoolean taskFailed = new AtomicBoolean(false);
      final AtomicBoolean abortFailed = new AtomicBoolean(false);
      final AtomicBoolean revertFailed = new AtomicBoolean(false);

      List<Future<?>> futures = new ArrayList<>();

      for (final I item : items) {
        // submit a task for each item that will either run or abort the task
        futures.add(service.submit(new Runnable() {
          @Override
          public void run() {
            if (!(stopOnFailure && taskFailed.get())) {
              // run the task
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
                LOG.info("Task failed", e);

                if (onFailure != null) {
                  try {
                    onFailure.run(item, e);
                  } catch (Exception failException) {
                    LOG.error("Failed to clean up on failure", e);
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
          }
        }));
      }

      // let the above tasks complete (or abort)
      waitFor(futures);
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
            }
          }));
        }

        // let the revert tasks complete
        waitFor(futures);
      }

      if (!suppressExceptions && !exceptions.isEmpty()) {
        Tasks.<E>throwOne(exceptions);
      }

      return !taskFailed.get();
    }
  }

  /**
   * Wait for all the futures to complete; there's a small sleep between
   * each iteration; enough to yield the CPU.
   * @param futures futures.
   */
  private static void waitFor(Collection<Future<?>> futures) {
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
          Thread.sleep(10);
        } catch (InterruptedException e) {
          futures.forEach(future -> future.cancel(true));
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  public static <I> Builder<I> foreach(Iterable<I> items) {
    return new Builder<>(items);
  }

  public static <I> Builder<I> foreach(I[] items) {
    return new Builder<>(Arrays.asList(items));
  }

  @SuppressWarnings("unchecked")
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

    Tasks.<E>castAndThrow(e);
  }

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
