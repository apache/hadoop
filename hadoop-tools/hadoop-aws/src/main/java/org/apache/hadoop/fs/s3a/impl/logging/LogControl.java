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

import java.util.logging.Level;

/**
 * Interface for a long controller; allows for log levels to be set
 * via reflection.
 */
public interface LogControl {

  /**
   * Log levels.
   */
  enum LogLevel {
    OFF("OFF", Level.OFF),
    ALL("ALL", Level.ALL),
    FATAL("FATAL", Level.SEVERE),
    ERROR("ERROR", Level.SEVERE),
    WARN("WARN",Level.WARNING),
    INFO("INFO", Level.INFO),
    DEBUG("DEBUG", Level.FINE),
    TRACE("TRACE", Level.FINEST);

    /**
     * Log name/key.
     */
     final String key;

    /**
     * JDK level.
     */
    final Level jdkLevel;

    LogLevel(final String key, final Level jdkLevel) {
      this.key = key;
      this.jdkLevel = jdkLevel;
    }

  }

  /**
   * Sets a log level for a class/package.
   * @param log log to set
   * @param level level to set
   */
  void setLogLevel(String log, LogLevel level);


  /**
   * Gets the log level for a class/package.
   * @param log log to get
   * @return log level or null if you can't get it.
   */
  LogLevel getLogLevel(String log);

}
