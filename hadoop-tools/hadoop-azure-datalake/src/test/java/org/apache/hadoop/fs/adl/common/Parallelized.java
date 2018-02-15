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
 *
 */

package org.apache.hadoop.fs.adl.common;

import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Provided for convenience to execute parametrized test cases concurrently.
 */
public class Parallelized extends Parameterized {

  public Parallelized(Class classObj) throws Throwable {
    super(classObj);
    setScheduler(new ThreadPoolScheduler());
  }

  private static class ThreadPoolScheduler implements RunnerScheduler {
    private ExecutorService executor;

    ThreadPoolScheduler() {
      int numThreads = 10;
      executor = Executors.newFixedThreadPool(numThreads);
    }

    public void finished() {
      executor.shutdown();
      try {
        executor.awaitTermination(10, TimeUnit.MINUTES);
      } catch (InterruptedException exc) {
        throw new RuntimeException(exc);
      }
    }

    public void schedule(Runnable childStatement) {
      executor.submit(childStatement);
    }
  }
}
