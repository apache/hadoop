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

package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ForwardingExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This ExecutorService blocks the submission of new tasks when its queue is
 * already full by using a semaphore. Task submissions require permits, task
 * completions release permits.
 * <p>
 * This is a refactoring of {@link BlockingThreadPoolExecutorService}; that code
 * contains the thread pool logic, whereas this isolates the semaphore
 * and submit logic for use with other thread pools and delegation models.
 * <p>
 * This is inspired by <a href="https://github.com/apache/incubator-s4/blob/master/subprojects/s4-comm/src/main/java/org/apache/s4/comm/staging/BlockingThreadPoolExecutorService.java">
 * this s4 threadpool</a>
 */
@SuppressWarnings("NullableProblems")
@InterfaceAudience.Private
public class SemaphoredDelegatingExecutor extends
    ForwardingExecutorService {

  private final Semaphore queueingPermits;
  private final ExecutorService executorDelegatee;
  private final int permitCount;

  /**
   * Instantiate.
   * @param executorDelegatee Executor to delegate to
   * @param permitCount number of permits into the queue permitted
   * @param fair should the semaphore be "fair"
   */
  public SemaphoredDelegatingExecutor(
      ExecutorService executorDelegatee,
      int permitCount,
      boolean fair) {
    this.permitCount = permitCount;
    queueingPermits = new Semaphore(permitCount, fair);
    this.executorDelegatee = executorDelegatee;
  }

  @Override
  protected ExecutorService delegate() {
    return executorDelegatee;
  }


  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
      long timeout, TimeUnit unit) throws InterruptedException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
      TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    try {
      queueingPermits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Futures.immediateFailedFuture(e);
    }
    return super.submit(new CallableWithPermitRelease<>(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    try {
      queueingPermits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Futures.immediateFailedFuture(e);
    }
    return super.submit(new RunnableWithPermitRelease(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    try {
      queueingPermits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Futures.immediateFailedFuture(e);
    }
    return super.submit(new RunnableWithPermitRelease(task));
  }

  @Override
  public void execute(Runnable command) {
    try {
      queueingPermits.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    super.execute(new RunnableWithPermitRelease(command));
  }

  /**
   * Get the number of permits available; guaranteed to be
   * {@code 0 <= availablePermits <= size}.
   * @return the number of permits available at the time of invocation.
   */
  public int getAvailablePermits() {
    return queueingPermits.availablePermits();
  }

  /**
   * Get the number of threads waiting to acquire a permit.
   * @return snapshot of the length of the queue of blocked threads.
   */
  public int getWaitingCount() {
    return queueingPermits.getQueueLength();
  }

  /**
   * Total number of permits.
   * @return the number of permits as set in the constructor
   */
  public int getPermitCount() {
    return permitCount;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "SemaphoredDelegatingExecutor{");
    sb.append("permitCount=").append(getPermitCount())
        .append(", available=").append(getAvailablePermits())
        .append(", waiting=").append(getWaitingCount())
        .append('}');
    return sb.toString();
  }

  /**
   * Releases a permit after the task is executed.
   */
  class RunnableWithPermitRelease implements Runnable {

    private Runnable delegatee;

    RunnableWithPermitRelease(Runnable delegatee) {
      this.delegatee = delegatee;
    }

    @Override
    public void run() {
      try {
        delegatee.run();
      } finally {
        queueingPermits.release();
      }

    }
  }

  /**
   * Releases a permit after the task is completed.
   */
  class CallableWithPermitRelease<T> implements Callable<T> {

    private Callable<T> delegatee;

    CallableWithPermitRelease(Callable<T> delegatee) {
      this.delegatee = delegatee;
    }

    @Override
    public T call() throws Exception {
      try {
        return delegatee.call();
      } finally {
        queueingPermits.release();
      }
    }

  }

}
