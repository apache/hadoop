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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Identity mapper that continuously reports progress via a background thread.
 */
public class AutoProgressMapper<KEYIN, VALIN, KEYOUT, VALOUT>
    extends Mapper<KEYIN, VALIN, KEYOUT, VALOUT> {

  public static final Log LOG = LogFactory.getLog(AutoProgressMapper.class.getName());

  /** Total number of millis for which progress will be reported
      by the auto-progress thread. If this is zero, then the auto-progress
      thread will never voluntarily exit.
    */
  private int maxProgressPeriod;

  /** Number of milliseconds to sleep for between loop iterations. Must be less
      than report interval.
    */
  private int sleepInterval;

  /** Number of milliseconds between calls to Reporter.progress(). Should be a multiple
      of the sleepInterval.
    */
  private int reportInterval;

  public static final String MAX_PROGRESS_PERIOD_KEY = "sqoop.mapred.auto.progress.max";
  public static final String SLEEP_INTERVAL_KEY = "sqoop.mapred.auto.progress.sleep";
  public static final String REPORT_INTERVAL_KEY = "sqoop.mapred.auto.progress.report";

  // Sleep for 10 seconds at a time.
  static final int DEFAULT_SLEEP_INTERVAL = 10000;

  // Report progress every 30 seconds.
  static final int DEFAULT_REPORT_INTERVAL = 30000;

  // Disable max progress, by default.
  static final int DEFAULT_MAX_PROGRESS = 0;

  private class ProgressThread extends Thread {

    private volatile boolean keepGoing; // while this is true, thread runs.
    private Context context;
    private long startTimeMillis;
    private long lastReportMillis;

    public ProgressThread(final Context ctxt) {
      this.context = ctxt;
      this.keepGoing = true;
    }

    public void signalShutdown() {
      this.keepGoing = false; // volatile update.
      this.interrupt();
    }

    public void run() {
      this.lastReportMillis = System.currentTimeMillis();
      this.startTimeMillis = this.lastReportMillis;

      final long MAX_PROGRESS = AutoProgressMapper.this.maxProgressPeriod;
      final long REPORT_INTERVAL = AutoProgressMapper.this.reportInterval;
      final long SLEEP_INTERVAL = AutoProgressMapper.this.sleepInterval;

      // in a loop:
      //   * Check that we haven't run for too long (maxProgressPeriod)
      //   * If it's been a report interval since we last made progress, make more.
      //   * Sleep for a bit.
      //   * If the parent thread has signaled for exit, do so.
      while (this.keepGoing) {
        long curTimeMillis = System.currentTimeMillis();

        if (MAX_PROGRESS != 0 && curTimeMillis - this.startTimeMillis > MAX_PROGRESS) {
          this.keepGoing = false;
          LOG.info("Auto-progress thread exiting after " + MAX_PROGRESS + " ms.");
          break;
        }

        if (curTimeMillis - this.lastReportMillis > REPORT_INTERVAL) {
          // It's been a full report interval -- claim progress.
          LOG.debug("Auto-progress thread reporting progress");
          this.context.progress();
          this.lastReportMillis = curTimeMillis;
        }

        // Unless we got an interrupt while we were working,
        // sleep a bit before doing more work.
        if (!this.interrupted()) {
          try {
            Thread.sleep(SLEEP_INTERVAL);
          } catch (InterruptedException ie) {
            // we were notified on something; not necessarily an error.
          }
        }
      }

      LOG.info("Auto-progress thread is finished. keepGoing=" + this.keepGoing);
    }
  }

  /**
   * Set configuration parameters for the auto-progress thread.
   */
  private final void configureAutoProgress(Configuration job) {
    this.maxProgressPeriod = job.getInt(MAX_PROGRESS_PERIOD_KEY, DEFAULT_MAX_PROGRESS);
    this.sleepInterval = job.getInt(SLEEP_INTERVAL_KEY, DEFAULT_SLEEP_INTERVAL);
    this.reportInterval = job.getInt(REPORT_INTERVAL_KEY, DEFAULT_REPORT_INTERVAL);

    if (this.reportInterval < 1) {
      LOG.warn("Invalid " + REPORT_INTERVAL_KEY + "; setting to " + DEFAULT_REPORT_INTERVAL);
      this.reportInterval = DEFAULT_REPORT_INTERVAL;
    }

    if (this.sleepInterval > this.reportInterval || this.sleepInterval < 1) {
      LOG.warn("Invalid " + SLEEP_INTERVAL_KEY + "; setting to " + DEFAULT_SLEEP_INTERVAL);
      this.sleepInterval = DEFAULT_SLEEP_INTERVAL;
    }

    if (this.maxProgressPeriod < 0) {
      LOG.warn("Invalid " + MAX_PROGRESS_PERIOD_KEY + "; setting to " + DEFAULT_MAX_PROGRESS);
      this.maxProgressPeriod = DEFAULT_MAX_PROGRESS;
    }
  }


  // map() method intentionally omitted; Mapper.map() is the identity mapper.


  /**
   * Run the mapping process for this task, wrapped in an auto-progress system.
   */
  public void run(Context context) throws IOException, InterruptedException {
    configureAutoProgress(context.getConfiguration());
    ProgressThread thread = this.new ProgressThread(context);

    try {
      thread.setDaemon(true);
      thread.start();

      // use default run() method to actually drive the mapping.
      super.run(context);
    } finally {
      // Tell the progress thread to exit..
      LOG.debug("Instructing auto-progress thread to quit.");
      thread.signalShutdown();
      try {
        // And wait for that to happen.
        LOG.debug("Waiting for progress thread shutdown...");
        thread.join();
        LOG.debug("Progress thread shutdown detected.");
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted when waiting on auto-progress thread: " + ie.toString());
      }
    }
  }

}
