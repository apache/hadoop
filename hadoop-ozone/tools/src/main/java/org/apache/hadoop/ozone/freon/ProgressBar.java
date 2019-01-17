/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.freon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Creates and runs a ProgressBar in new Thread which gets printed on
 * the provided PrintStream.
 */
public class ProgressBar {

  private static final Logger LOG = LoggerFactory.getLogger(ProgressBar.class);
  private static final long REFRESH_INTERVAL = 1000L;

  private final long maxValue;
  private final Supplier<Long> currentValue;
  private final Thread progressBar;

  private volatile boolean running;

  private volatile long startTime;

  /**
   * Creates a new ProgressBar instance which prints the progress on the given
   * PrintStream when started.
   *
   * @param stream to display the progress
   * @param maxValue Maximum value of the progress
   * @param currentValue Supplier that provides the current value
   */
  public ProgressBar(final PrintStream stream, final Long maxValue,
                     final Supplier<Long> currentValue) {
    this.maxValue = maxValue;
    this.currentValue = currentValue;
    this.progressBar = new Thread(getProgressBar(stream));
    this.running = false;
  }

  /**
   * Starts the ProgressBar in a new Thread.
   * This is a non blocking call.
   */
  public synchronized void start() {
    if (!running) {
      running = true;
      startTime = System.nanoTime();
      progressBar.start();
    }
  }

  /**
   * Graceful shutdown, waits for the progress bar to complete.
   * This is a blocking call.
   */
  public synchronized void shutdown() {
    if (running) {
      try {
        progressBar.join();
        running = false;
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for the progress bar to " +
                "complete.");
      }
    }
  }

  /**
   * Terminates the progress bar. This doesn't wait for the progress bar
   * to complete.
   */
  public synchronized void terminate() {
    if (running) {
      try {
        running = false;
        progressBar.join();
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for the progress bar to " +
                "complete.");
      }
    }
  }

  private Runnable getProgressBar(final PrintStream stream) {
    return () -> {
      stream.println();
      while (running && currentValue.get() < maxValue) {
        print(stream, currentValue.get());
        try {
          Thread.sleep(REFRESH_INTERVAL);
        } catch (InterruptedException e) {
          LOG.warn("ProgressBar was interrupted.");
        }
      }
      print(stream, maxValue);
      stream.println();
      running = false;
    };
  }

  /**
   * Given current value prints the progress bar.
   *
   * @param value current progress position
   */
  private void print(final PrintStream stream, final long value) {
    stream.print('\r');
    double percent = 100.0 * value / maxValue;
    StringBuilder sb = new StringBuilder();
    sb.append(" " + String.format("%.2f", percent) + "% |");

    for (int i = 0; i <= percent; i++) {
      sb.append('█');
    }
    for (int j = 0; j < 100 - percent; j++) {
      sb.append(' ');
    }
    sb.append("|  ");
    sb.append(value + "/" + maxValue);
    long timeInSec = TimeUnit.SECONDS.convert(
            System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    String timeToPrint = String.format("%d:%02d:%02d", timeInSec / 3600,
            (timeInSec % 3600) / 60, timeInSec % 60);
    sb.append(" Time: " + timeToPrint);
    stream.print(sb.toString());
  }
}