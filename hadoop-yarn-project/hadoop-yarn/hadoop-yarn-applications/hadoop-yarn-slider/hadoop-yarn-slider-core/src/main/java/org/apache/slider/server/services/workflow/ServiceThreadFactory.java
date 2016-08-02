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

package org.apache.slider.server.services.workflow;

import com.google.common.base.Preconditions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread factory that creates threads (possibly daemon threads)
 * using the name and naming policy supplied.
 * The thread counter starts at 1, increments atomically, 
 * and is supplied as the second argument in the format string.
 * 
 * A static method, {@link #singleThreadExecutor(String, boolean)},
 * exists to simplify the construction of an executor with a single well-named
 * threads. 
 * 
 * Example
 * <pre>
 *  ExecutorService exec = ServiceThreadFactory.newSingleThreadExecutor("live", true)
 * </pre>
 */
public class ServiceThreadFactory implements ThreadFactory {

  private static final AtomicInteger counter = new AtomicInteger(1);

  /**
   * Default format for thread names: {@value}.
   */
  public static final String DEFAULT_NAMING_FORMAT = "%s-%03d";
  private final String name;
  private final boolean daemons;
  private final String namingFormat;

  /**
   * Create an instance
   * @param name base thread name
   * @param daemons flag to indicate the threads should be marked as daemons
   * @param namingFormat format string to generate thread names from
   */
  public ServiceThreadFactory(String name,
      boolean daemons,
      String namingFormat) {
    Preconditions.checkArgument(name != null, "null name");
    Preconditions.checkArgument(namingFormat != null, "null naming format");
    this.name = name;
    this.daemons = daemons;
    this.namingFormat = namingFormat;
  }

  /**
   * Create an instance with the default naming format.
   * @param name base thread name
   * @param daemons flag to indicate the threads should be marked as daemons
   */
  public ServiceThreadFactory(String name,
      boolean daemons) {
    this(name, daemons, DEFAULT_NAMING_FORMAT);
  }

  @Override
  public Thread newThread(Runnable r) {
    Preconditions.checkArgument(r != null, "null runnable");
    String threadName =
        String.format(namingFormat, name, counter.getAndIncrement());
    Thread thread = new Thread(r, threadName);
    thread.setDaemon(daemons);
    return thread;
  }

  /**
   * Create a single thread executor using this naming policy.
   * @param name base thread name
   * @param daemons flag to indicate the threads should be marked as daemons
   * @return an executor
   */
  public static ExecutorService singleThreadExecutor(String name,
      boolean daemons) {
    return Executors.newSingleThreadExecutor(
        new ServiceThreadFactory(name, daemons));
  }
}
