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

/**
 * Something to control logging levels in Log4j.
 * <p>
 * Package private to avoid any direct instantiation.
 * <p>
 * Important: this must never be instantiated exception through
 * reflection code which can catch and swallow exceptions related
 * to not finding Log4J on the classpath.
 * The Hadoop libraries can and are used with other logging
 * back ends and we MUST NOT break that.
 */
class Log4JController extends LogControl {

  /**
   * Set the log4J level, ignoring all exceptions raised.
   * {@inheritDoc}
   */
  @Override
  protected boolean setLevel(final String logName, final LogLevel level) {
    try {
      Logger logger = Logger.getLogger(logName);
      logger.setLevel(Level.toLevel(level.getLog4Jname()));
      return true;
    } catch (Exception ignored) {
      // ignored.
      return false;
    }
  }
}
