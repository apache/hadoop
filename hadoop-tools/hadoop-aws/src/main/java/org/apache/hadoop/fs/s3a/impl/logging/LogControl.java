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

/**
 * class to assist reflection-based control of logger back ends.
 * <p>
 * An instance of LogControl is able to control the log levels of
 * loggers for log libraries such as Log4j, yet can be created in
 * code designed to support multiple back end loggers behind
 * SLF4J.
 */
public abstract class LogControl {

  /**
   * Enumeration of log levels.
   * <p>
   * The list is in descending order.
   */
  public enum LogLevel {
    ALL("ALL"),
    FATAL("FATAL"),
    ERROR("ERROR"),
    WARN("WARN"),
    INFO("INFO"),
    DEBUG("DEBUG"),
    TRACE("TRACE"),
    OFF("OFF");

    /**
     * Level name as used in Log4J.
     */
    private final String log4Jname;

    LogLevel(final String log4Jname) {
      this.log4Jname = log4Jname;
    }

    /**
     * Get the log4j name of this level.
     * @return the log name for use in configuring Log4J.
     */
    public String getLog4Jname() {
      return log4Jname;
    }
  }

  /**
   * Sets a log level for a class/package.
   * @param log log to set
   * @param level level to set
   * @return true if the log was set
   */
  public final boolean setLogLevel(String log, LogLevel level) {
    try {
      return setLevel(log, level);
    } catch (Exception ignored) {
      // ignored.
      return false;
    }

  }


  /**
   * Sets a log level for a class/package.
   * Exceptions may be raised; they will be caught in
   * {@link #setLogLevel(String, LogLevel)} and ignored.
   * @param log log to set
   * @param level level to set
   * @return true if the log was set
   * @throws Exception any problem loading/updating the log
   */
  protected abstract boolean setLevel(String log, LogLevel level) throws Exception;

}
