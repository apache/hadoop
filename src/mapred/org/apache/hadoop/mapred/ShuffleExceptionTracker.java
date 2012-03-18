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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is used to track shuffle exceptions. It contains routines
 * to check an exception that occurred while fetching the Map output to see if it
 * matches what was configured. It also provides functions to keep track of
 * the number of exceptions that occurred and if a limit is set, it will
 * abort the TT.  The limit is a percent of exceptions of the last X number of
 * requests.
 *
 */
public class ShuffleExceptionTracker {
  public static final Log LOG = LogFactory
      .getLog(ShuffleExceptionTracker.class);

  // a clear bit is success, set bit is exception occurred
  private BitSet requests;
  final private int size;
  private int index;
  final private String exceptionStackRegex;
  final private String exceptionMsgRegex;
  final private float shuffleExceptionLimit;

  /**
   * Constructor
   *
   * @param numberRequests
   *          the tailing number of requests to track
   * @param exceptionStackRegex
   *          the exception stack regular expression to look for
   * @param exceptionMsgRegex
   *          the exception message regular expression to look for
   * @param shuffleExceptionLimit
   *          the exception limit (0-1.0) representing a percent. 0 disables the
   *          abort check.
   */
  ShuffleExceptionTracker(int numberRequests, String exceptionStackRegex,
      String exceptionMsgRegex, float shuffleExceptionLimit) {
    this.exceptionStackRegex = exceptionStackRegex;
    this.exceptionMsgRegex = exceptionMsgRegex;
    this.shuffleExceptionLimit = shuffleExceptionLimit;
    this.size = numberRequests;
    this.index = 0;
    this.requests = new BitSet(size);
  }

  /**
   * Gets the number of requests we are tracking
   *
   * @return number of requests
   */
  public int getNumRequests() {
    return this.size;
  }

  /**
   * Gets the percent of the requests that had exceptions occur.
   *
   * @return percent failures as float
   */
  public synchronized float getPercentExceptions() {
    return (float) requests.cardinality() / (float) size;
  }

  /**
   * Mark the request as success.
   */
  public synchronized void success() {
    if (shuffleExceptionLimit > 0) {
      requests.clear(index);
      incrIndex();
    }
  }

  /**
   * Mark the request as an exception occurred.
   */
  public synchronized void exception() {
    if (shuffleExceptionLimit > 0) {
      requests.set(index);
      incrIndex();
    }
  }

  /**
   * Parse the exception to see if it matches the regular expression you
   * configured. If both msgRegex and StackRegex are set then make sure both
   * match, otherwise only one has to match. Abort if the limit is hit.
   * @param ie - the shuffle exception that occurred
   * @return true if the exception matches, false otherwise
   */
  public boolean checkException(IOException ie) {
    if (exceptionMsgRegex != null) {
      String msg = ie.getMessage();
      if (msg == null || !msg.matches(exceptionMsgRegex)) {
        // for exception tracking purposes, if the exception didn't
        // match the one we are looking for consider it a successful
        // request
        this.success();
        return false;
      }
    }
    if (exceptionStackRegex != null && !checkStackException(ie)) {
      this.success();
      return false;
    }
    this.exception();
    if (shuffleExceptionLimit > 0
        && this.getPercentExceptions() >= shuffleExceptionLimit) {
      LOG.fatal("************************************************************\n"
          + "Shuffle exception count is greater than the fatal limit: "
          + shuffleExceptionLimit
          + "Aborting JVM.\n"
          + "************************************************************");
      doAbort();
    }

    return true;
  }

  private boolean checkStackException(IOException ie) {
    StackTraceElement[] stack = ie.getStackTrace();

    for (StackTraceElement elem : stack) {
      String stacktrace = elem.toString();
      if (stacktrace.matches(exceptionStackRegex)) {
        return true;
      }
    }
    return false;
  }

  protected void doAbort() {
    System.exit(1);
  }

  private void incrIndex() {
    if (index == (size - 1)) {
      index = 0;
    } else {
      index++;
    }
  }

}
