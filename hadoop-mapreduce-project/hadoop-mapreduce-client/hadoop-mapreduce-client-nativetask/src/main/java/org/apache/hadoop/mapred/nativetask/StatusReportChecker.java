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
package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;

/**
 * Will periodically check status from native and report to MR framework.
 * 
 */
class StatusReportChecker implements Runnable {

  private static Log LOG = LogFactory.getLog(StatusReportChecker.class);
  public static final int INTERVAL = 1000; // milliseconds

  private Thread checker;
  private final TaskReporter reporter;
  private final long interval;

  public StatusReportChecker(TaskReporter reporter) {
    this(reporter, INTERVAL);
  }

  public StatusReportChecker(TaskReporter reporter, long interval) {
    this.reporter = reporter;
    this.interval = interval;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(interval);
      } catch (final InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("StatusUpdater thread exiting " + "since it got interrupted");
        }
        break;
      }
      try {
        NativeRuntime.reportStatus(reporter);
      } catch (final IOException e) {
        LOG.warn("Update native status got exception", e);
        reporter.setStatus(e.toString());
        break;
      }
    }
  }

  protected void initUsedCounters() {
    reporter.getCounter(TaskCounter.MAP_INPUT_RECORDS);
    reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    reporter.getCounter(FileInputFormatCounter.BYTES_READ);
    reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    reporter.getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);
    reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
    reporter.getCounter(TaskCounter.SPILLED_RECORDS);
  }

  public synchronized void start() {
    if (checker == null) {
      // init counters used by native side,
      // so they will have correct display name
      initUsedCounters();
      checker = new Thread(this);
      checker.setDaemon(true);
      checker.start();
    }
  }

  public synchronized void stop() throws InterruptedException {
    if (checker != null) {
      checker.interrupt();
      checker.join();
    }
  }
}