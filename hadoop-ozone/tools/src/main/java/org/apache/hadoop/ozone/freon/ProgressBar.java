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

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Run an arbitrary code and print progress on the provided stream. The
 * progressbar stops when: - the provided currentvalue is less the the maxvalue
 * - exception thrown
 */
public class ProgressBar {

  private static final long REFRESH_INTERVAL = 1000L;

  private PrintStream stream;
  private AtomicLong currentValue;
  private long maxValue;
  private Thread progressBar;
  private volatile boolean exception = false;
  private long startTime;

  /**
   * @param stream Used to display the progress
   * @param maxValue Maximum value of the progress
   */
  ProgressBar(PrintStream stream, long maxValue) {
    this.stream = stream;
    this.maxValue = maxValue;
    this.currentValue = new AtomicLong(0);
    this.progressBar = new Thread(new ProgressBarThread());
  }

  /**
   * Start a task with a progessbar without any in/out parameters Runnable used
   * just a task wrapper.
   *
   * @param task Runnable
   */
  public void start(Runnable task) {

    startTime = System.nanoTime();

    try {

      progressBar.start();
      task.run();

    } catch (Exception e) {
      exception = true;
    } finally {

      try {
        progressBar.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Start a task with only out parameters.
   *
   * @param task Supplier that represents the task
   * @param <T> Generic return type
   * @return Whatever the supllier produces
   */
  public <T> T start(Supplier<T> task) {

    startTime = System.nanoTime();
    T result = null;

    try {

      progressBar.start();
      result = task.get();

    } catch (Exception e) {
      exception = true;
    } finally {

      try {
        progressBar.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      return result;
    }
  }

  /**
   * Start a task with in/out parameters.
   *
   * @param input Input of the function
   * @param task A Function that does the task
   * @param <T> type of the input
   * @param <R> return type
   * @return Whatever the Function returns
   */
  public <T, R> R start(T input, Function<T, R> task) {

    startTime = System.nanoTime();
    R result = null;

    try {

      progressBar.start();
      result = task.apply(input);

    } catch (Exception e) {
      exception = true;
      throw e;
    } finally {

      try {
        progressBar.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      return result;
    }
  }

  /**
   * Increment the progress with one step.
   */
  public void incrementProgress() {
    currentValue.incrementAndGet();
  }

  private class ProgressBarThread implements Runnable {

    @Override
    public void run() {
      try {

        stream.println();
        long value;

        while ((value = currentValue.get()) < maxValue) {
          print(value);

          if (exception) {
            break;
          }
          Thread.sleep(REFRESH_INTERVAL);
        }

        if (exception) {
          stream.println();
          stream.println("Incomplete termination, " + "check log for " +
              "exception.");
        } else {
          print(maxValue);
        }
        stream.println();
      } catch (InterruptedException e) {
        stream.println(e);
      }
    }

    /**
     * Given current value prints the progress bar.
     *
     * @param value current progress position
     */
    private void print(long value) {
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

}
