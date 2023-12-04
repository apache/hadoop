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

package org.apache.hadoop.fs.s3a.impl.logging;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import static java.util.Objects.requireNonNull;

/**
 * A controller for logging in log4j 1.x.
 * Must be loaded through reflection to ensure no use of log4j APIs
 * elsewhere.
 */
@SuppressWarnings("unused")
public class Log4J1Controller implements LogControl {

  @Override
  public void setLogLevel(final String log, final LogLevel level) {
    getLogger(log).setLevel(Level.toLevel(level.name()));
  }

  @Override
  public LogLevel getLogLevel(final String name) {
    final Logger log = getLogger(name);
    if (log.isTraceEnabled()) {
      return LogLevel.TRACE;
    }
    if (log.isDebugEnabled()) {
      return LogLevel.DEBUG;
    }
    if (log.isInfoEnabled()) {
      return LogLevel.INFO;
    }
    if (log.isEnabledFor(Priority.WARN)) {
      return LogLevel.WARN;
    }
    if (log.isEnabledFor(Priority.FATAL)) {
      return LogLevel.FATAL;
    }

    return LogLevel.OFF;
  }


  /**
   * Get a logger.
   * @param log log name
   * @return logger
   */
  private static Logger getLogger(final String log) {
    return requireNonNull(Logger.getLogger(log),
        () -> "No logger for " + log);
  }
}
