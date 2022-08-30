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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;

/**
 * Thread Leak tracker.
 * Initialized with the current set of threads and some known-long-life
 * threads whose presence must not be considered a failure.
 */
public class ThreadLeakTracker {

  private final Set<String> trackedThreads =
      listInitialThreadsForLifecycleChecks();

  public void assertNoThreadLeakage() {
    Assertions.assertThat(getCurrentThreadNames())
        .describedAs("The threads at the end of the test run")
        .isSubsetOf(trackedThreads);
  }

  public Set<String> getTrackedThreads() {
    return trackedThreads;
  }

  /**
   * Get a set containing the names of all active threads,
   * stripping out all test runner threads.
   * @return the current set of threads.
   */
  public static Set<String> getCurrentThreadNames() {
    TreeSet<String> threads = Thread.getAllStackTraces().keySet()
        .stream()
        .map(Thread::getName)
        .filter(n -> n.startsWith("JUnit"))
        .filter(n -> n.startsWith("surefire"))
        .collect(Collectors.toCollection(TreeSet::new));
    return threads;
  }

  /**
   * This creates a set containing all current threads and some well-known
   * thread names whose existence should not fail test runs.
   * They are generally static cleaner threads created by various classes
   * on instantiation.
   * @return a set of threads to use in later assertions.
   */
  public static Set<String> listInitialThreadsForLifecycleChecks() {
    Set<String> threadSet = getCurrentThreadNames();
    // static filesystem statistics cleaner
    threadSet.add("org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner");

    // java.lang.UNIXProcess. maybe if chmod is called?
    threadSet.add("process reaper");
    // once a quantile has been scheduled, the mutable quantile thread pool
    // is initialized; it has a minimum thread size of 1.
    threadSet.add("MutableQuantiles-0");
    // IDE?
    threadSet.add("Attach Listener");
    return threadSet;
  }
}
