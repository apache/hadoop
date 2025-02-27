/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.util.concurrent;

import org.apache.hadoop.util.SubjectUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** An extension of ScheduledThreadPoolExecutor that provides additional
 * functionality. */
public class HadoopScheduledThreadPoolExecutor extends
    ScheduledThreadPoolExecutor {

  private static final Logger LOG = LoggerFactory
      .getLogger(HadoopScheduledThreadPoolExecutor.class);

  public HadoopScheduledThreadPoolExecutor(int corePoolSize) {
    super(corePoolSize);
  }

  public HadoopScheduledThreadPoolExecutor(int corePoolSize,
      ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
  }

  public HadoopScheduledThreadPoolExecutor(int corePoolSize,
      RejectedExecutionHandler handler) {
    super(corePoolSize, handler);
  }

  public HadoopScheduledThreadPoolExecutor(int corePoolSize,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, threadFactory, handler);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("beforeExecute in thread: " + Thread.currentThread()
          .getName() + ", runnable type: " + r.getClass().getName());
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    ExecutorHelper.logThrowableFromAfterExecute(r, t);
  }

  /**
   * @throws RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   */
  @Override
  public ScheduledFuture<?> schedule(Runnable command,
                                     long delay,
                                     TimeUnit unit) {
      return super.schedule(SubjectUtil.wrap(command), delay, unit);
  }

  /**
   * @throws RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   */
  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable,
                                         long delay,
                                         TimeUnit unit) {
    return super.schedule(SubjectUtil.wrap(callable), delay, unit);
  }

  /**
   * @throws RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   * @throws IllegalArgumentException   {@inheritDoc}
   */
  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                long initialDelay,
                                                long period,
                                                TimeUnit unit) {
    return super.scheduleAtFixedRate(SubjectUtil.wrap(command), initialDelay, period, unit);
  }

  /**
   * @throws RejectedExecutionException {@inheritDoc}
   * @throws NullPointerException       {@inheritDoc}
   * @throws IllegalArgumentException   {@inheritDoc}
   */
  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                   long initialDelay,
                                                   long delay,
                                                   TimeUnit unit) {
    return super.scheduleWithFixedDelay(SubjectUtil.wrap(command), initialDelay, delay, unit);
  }

}
