/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.service.utils;

import java.io.Closeable;

/**
 * A duration in milliseconds. This class can be used
 * to count time, and to be polled to see if a time limit has
 * passed.
 */
public class Duration implements Closeable {
  public long start, finish;
  public final long limit;

  /**
   * Create a duration instance with a limit of 0
   */
  public Duration() {
    this(0);
  }

  /**
   * Create a duration with a limit specified in millis
   * @param limit duration in milliseconds
   */
  public Duration(long limit) {
    this.limit = limit;
  }

  /**
   * Start
   * @return self
   */
  public Duration start() {
    start = now();
    return this;
  }

  /**
   * The close operation relays to {@link #finish()}.
   * Implementing it allows Duration instances to be automatically
   * finish()'d in Java7 try blocks for when used in measuring durations.
   */
  @Override
  public final void close() {
    finish();
  }

  public void finish() {
    finish = now();
  }

  protected long now() {
    return System.nanoTime()/1000000;
  }

  public long getInterval() {
    return finish - start;
  }

  /**
   * return true if the limit has been exceeded
   * @return true if a limit was set and the current time
   * exceeds it.
   */
  public boolean getLimitExceeded() {
    return limit >= 0 && ((now() - start) > limit);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Duration");
     if (finish >= start) {
       builder.append(" finished at ").append(getInterval()).append(" millis;");
     } else {
       if (start > 0) {
         builder.append(" started but not yet finished;");
       } else {
         builder.append(" unstarted;");
       }
     }
    if (limit > 0) {
      builder.append(" limit: ").append(limit).append(" millis");
      if (getLimitExceeded()) {
        builder.append(" -  exceeded");
      }
    }
    return  builder.toString();
  }

}
