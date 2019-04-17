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

package org.apache.hadoop.util;

import org.slf4j.Logger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A duration with logging of final state at info or debug
 * in the {@code close()} call.
 * This allows it to be used in a try-with-resources clause, and have the
 * duration automatically logged.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DurationInfo extends OperationDuration
    implements AutoCloseable {
  private final String text;

  private final Logger log;

  /**
   * Should the log be at INFO rather than DEBUG.
   */
  private final boolean logAtInfo;

  /**
   * Create the duration text from a {@code String.format()} code call;
   * log output at info level.
   * @param log log to write to
   * @param format format string
   * @param args list of arguments
   */
  public DurationInfo(Logger log, String format, Object... args) {
    this(log, true, format, args);
  }

  /**
   * Create the duration text from a {@code String.format()} code call
   * and log either at info or debug.
   * @param log log to write to
   * @param logAtInfo should the log be at info, rather than debug
   * @param format format string
   * @param args list of arguments
   */
  public DurationInfo(Logger log,
      boolean logAtInfo,
      String format,
      Object... args) {
    this.text = String.format(format, args);
    this.log = log;
    this.logAtInfo = logAtInfo;
    if (logAtInfo) {
      log.info("Starting: {}", text);
    } else {
      log.debug("Starting: {}", text);
    }
  }

  @Override
  public String toString() {
    return text + ": duration " + super.toString();
  }

  @Override
  public void close() {
    finished();
    if (logAtInfo) {
      log.info("{}", this);
    } else {
      log.debug("{}", this);
    }
  }
}
