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

package org.apache.hadoop.fs.tosfs.common;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Copied from Apache Iceberg, please see.
 * <a href="https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/util/Tasks.java">
 * Tasks</a>
 */
public class Tasks {

  private static final Logger LOG = LoggerFactory.getLogger(Tasks.class);

  private Tasks() {
  }

  public static class UnrecoverableException extends RuntimeException {
    public UnrecoverableException(String message) {
      super(message);
    }

    public UnrecoverableException(String message, Throwable cause) {
      super(message, cause);
    }

    public UnrecoverableException(Throwable cause) {
      super(cause);
    }
  }

  public interface FailureTask<I, E extends Exception> {
    void run(I item, Exception exception) throws E;
  }

  public interface Task<I, E extends Exception> {
    void run(I item) throws E;
  }

  public static class Builder<I> {
    private final Iterable<I> items;
    private ExecutorService service = null;
    private FailureTask<I, ?> onFailure = null;
    private boolean stopOnFailure = false;
    private boolean throwFailureWhenFinished = true;
    private Task<I, ?> revertTask = null;
    private boolean stopRevertsOnFailure = false;
    private Task<I, ?> abortTask = null;
    private boolean stopAbortsOnFailure = false;

    // retry settings
    private List<Class<? extends Exception>> stopRetryExceptions =
        Lists.newArrayList(UnrecoverableException.class);
    private List<Class<? extends Exception>> onlyRetryExceptions = null;
    private Predicate<Exception> shouldRetryPredicate = null;
    private int maxAttempts = 1; // not all operations can be retried
    private long minSleepTimeMs = 1000; // 1 second
    private long maxSleepTimeMs = 600000; // 10 minutes
    private long maxDurationMs = 600000; // 10 minutes
    private double scaleFactor = 2.0; // exponential

    public Builder(Iterable<I> items) {
      this.items = items;
    }

    public Builder<I> executeWith(ExecutorService svc) {
      this.service = svc;
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

    public Builder<I> throwFailureWhenFinished() {
      this.throwFailureWhenFinished = true;
      return this;
    }

    public Builder<I> throwFailureWhenFinished(boolean throwWhenFinished) {
      this.throwFailureWhenFinished = throwWhenFinished;
      return this;
    }

    public Builder<I> suppressFailureWhenFinished() {
      this.throwFailureWhenFinished = false;
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

    @SafeVarargs public final Builder<I> stopRetryOn(Class<? extends Exception>... exceptions) {
      stopRetryExceptions.addAll(Arrays.asList(exceptions));
      return this;
    }

    public Builder<I> shouldRetryTest(Predicate<Exception> shouldRetry) {
      this.shouldRetryPredicate = shouldRetry;
      return this;
    }

    public Builder<I> noRetry() {
      this.maxAttempts = 1;
      return this;
    }

    public Builder<I> retry(int nTimes) {
      this.maxAttempts = nTimes + 1;
      return this;
    }

    public Builder<I> onlyRetryOn(Class<? extends Exception> exception) {
      this.onlyRetryExceptions = Collections.singletonList(exception);
      return this;
    }

    @SafeVarargs public final Builder<I> onlyRetryOn(Class<? extends Exception>... exceptions) {
      this.onlyRetryExceptions = Lists.newArrayList(exceptions);
      return this;
    }

    public Builder<I> exponentialBackoff(long backoffMinSleepTimeMs, long backoffMaxSleepTimeMs,
        long backoffMaxRetryTimeMs, double backoffScaleFactor) {
      this.minSleepTimeMs = backoffMinSleepTimeMs;
      this.maxSleepTimeMs = backoffMaxSleepTimeMs;
      this.maxDurationMs = backoffMaxRetryTimeMs;
      this.scaleFactor = backoffScaleFactor;
      return this;
    }

    public boolean run(Task<I, RuntimeException> task) {
      return run(task, RuntimeException.class);
    }

    public <E extends Exception> boolean run(Task<I, E> task, Class<E> exceptionClass) throws E {
      if (service != null) {
        return runParallel(task, exceptionClass);
      } else {
        return runSingleThreaded(task, exceptionClass);
      }
    }

    private <E extends Exception> boolean runSingleThreaded(
        Task<I, E> task, Class<E> exceptionClass) throws E {
      List<I> succeeded = Lists.newArrayList();
      List<Throwable> exceptions = Lists.newArrayList();

      Iterator<I> iterator = items.iterator();
      boolean threw = true;
      try {
        while (iterator.hasNext()) {
          I item = iterator.next();
          try {
            runTaskWithRetry(task, item);
            succeeded.add(item);
          } catch (Exception e) {
            exceptions.add(e);

            if (onFailure != null) {
              tryRunOnFailure(item, e);
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
                failed = true;
                LOG.error("Failed to revert task", e);
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

      if (throwFailureWhenFinished && !exceptions.isEmpty()) {
        Tasks.throwOne(exceptions, exceptionClass);
      } else if (throwFailureWhenFinished && threw) {
        throw new RuntimeException("Task set failed with an uncaught throwable");
      }

      return !threw;
    }

    private void tryRunOnFailure(I item, Exception failure) {
      try {
        onFailure.run(item, failure);
      } catch (Exception failException) {
        failure.addSuppressed(failException);
        LOG.error("Failed to clean up on failure", failException);
        // keep going
      }
    }

    private <E extends Exception> boolean runParallel(
        final Task<I, E> task, Class<E> exceptionClass) throws E {
      final Queue<I> succeeded = new ConcurrentLinkedQueue<>();
      final Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
      final AtomicBoolean taskFailed = new AtomicBoolean(false);
      final AtomicBoolean abortFailed = new AtomicBoolean(false);
      final AtomicBoolean revertFailed = new AtomicBoolean(false);

      List<Future<?>> futures = Lists.newArrayList();

      for (final I item : items) {
        // submit a task for each item that will either run or abort the task
        futures.add(service.submit(() -> {
          if (!(stopOnFailure && taskFailed.get())) {
            // run the task with retries
            boolean threw = true;
            try {
              runTaskWithRetry(task, item);

              succeeded.add(item);

              threw = false;

            } catch (Exception e) {
              taskFailed.set(true);
              exceptions.add(e);

              if (onFailure != null) {
                tryRunOnFailure(item, e);
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
        }));
      }

      // let the above tasks complete (or abort)
      exceptions.addAll(waitFor(futures));
      futures.clear();

      if (taskFailed.get() && revertTask != null) {
        // at least one task failed, revert any that succeeded
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
        exceptions.addAll(waitFor(futures));
      }

      if (throwFailureWhenFinished && !exceptions.isEmpty()) {
        Tasks.throwOne(exceptions, exceptionClass);
      } else if (throwFailureWhenFinished && taskFailed.get()) {
        throw new RuntimeException("Task set failed with an uncaught throwable");
      }

      return !taskFailed.get();
    }

    private <E extends Exception> void runTaskWithRetry(
        Task<I, E> task, I item) throws E {
      long start = System.currentTimeMillis();
      int attempt = 0;
      while (true) {
        attempt += 1;
        try {
          task.run(item);
          break;

        } catch (Exception e) {
          long durationMs = System.currentTimeMillis() - start;
          if (attempt >= maxAttempts || (durationMs > maxDurationMs && attempt > 1)) {
            if (durationMs > maxDurationMs) {
              LOG.info("Stopping retries after {} ms", durationMs);
            }
            throw e;
          }

          if (shouldRetryPredicate != null) {
            if (!shouldRetryPredicate.test(e)) {
              throw e;
            }

          } else if (onlyRetryExceptions != null) {
            // if onlyRetryExceptions are present, then this retries if one is found
            boolean matchedRetryException = false;
            for (Class<? extends Exception> exClass : onlyRetryExceptions) {
              if (exClass.isInstance(e)) {
                matchedRetryException = true;
                break;
              }
            }
            if (!matchedRetryException) {
              throw e;
            }

          } else {
            // otherwise, always retry unless one of the stop exceptions is found
            for (Class<? extends Exception> exClass : stopRetryExceptions) {
              if (exClass.isInstance(e)) {
                throw e;
              }
            }
          }

          int delayMs =
              (int) Math.min(minSleepTimeMs * Math.pow(scaleFactor, attempt - 1), maxSleepTimeMs);
          int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMs * 0.1)));

          LOG.warn("Retrying task after failure: {}", e.getMessage(), e);

          try {
            TimeUnit.MILLISECONDS.sleep(delayMs + jitter);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
          }
        }
      }
    }
  }

  private static Collection<Throwable> waitFor(
      Collection<Future<?>> futures) {
    while (true) {
      int numFinished = 0;
      for (Future<?> future : futures) {
        if (future.isDone()) {
          numFinished += 1;
        }
      }

      if (numFinished == futures.size()) {
        List<Throwable> uncaught = Lists.newArrayList();
        // all of the futures are done, get any uncaught exceptions
        for (Future<?> future : futures) {
          try {
            future.get();

          } catch (InterruptedException e) {
            LOG.warn("Interrupted while getting future results", e);
            for (Throwable t : uncaught) {
              e.addSuppressed(t);
            }
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);

          } catch (CancellationException e) {
            // ignore cancellations

          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (Error.class.isInstance(cause)) {
              for (Throwable t : uncaught) {
                cause.addSuppressed(t);
              }
              throw (Error) cause;
            }

            if (cause != null) {
              uncaught.add(e);
            }

            LOG.warn("Task threw uncaught exception", cause);
          }
        }

        return uncaught;

      } else {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for tasks to finish", e);

          for (Future<?> future : futures) {
            future.cancel(true);
          }
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A range, [ 0, size ).
   */
  private static class Range implements Iterable<Integer> {
    private final int size;

    Range(int size) {
      this.size = size;
    }

    @Override
    public Iterator<Integer> iterator() {
      return new Iterator<Integer>() {
        private int current = 0;

        @Override
        public boolean hasNext() {
          return current < size;
        }

        @Override
        public Integer next() {
          if (!hasNext()) {
            throw new NoSuchElementException("No more items.");
          }
          int ret = current;
          current += 1;
          return ret;
        }
      };
    }
  }

  public static Builder<Integer> range(int upTo) {
    return new Builder<>(new Range(upTo));
  }

  public static <I> Builder<I> foreach(Iterator<I> items) {
    return new Builder<>(() -> items);
  }

  public static <I> Builder<I> foreach(Iterable<I> items) {
    return new Builder<>(items);
  }

  @SafeVarargs public static <I> Builder<I> foreach(I... items) {
    return new Builder<>(Arrays.asList(items));
  }

  public static <I> Builder<I> foreach(Stream<I> items) {
    return new Builder<>(items::iterator);
  }

  private static <E extends Exception> void throwOne(Collection<Throwable> exceptions,
      Class<E> allowedException) throws E {
    Iterator<Throwable> iter = exceptions.iterator();
    Throwable exception = iter.next();
    Class<? extends Throwable> exceptionClass = exception.getClass();

    while (iter.hasNext()) {
      Throwable other = iter.next();
      if (!exceptionClass.isInstance(other)) {
        exception.addSuppressed(other);
      }
    }

    castAndThrow(exception, allowedException);
  }

  public static <E extends Exception> void castAndThrow(
      Throwable exception, Class<E> exceptionClass) throws E {
    if (exception instanceof RuntimeException) {
      throw (RuntimeException) exception;
    } else if (exception instanceof Error) {
      throw (Error) exception;
    } else if (exceptionClass.isInstance(exception)) {
      throw (E) exception;
    }
    throw new RuntimeException(exception);
  }
}
