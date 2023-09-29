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

package org.apache.hadoop.fs.store;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

/**
 * Log exactly once, even across threads.
 */
public class LogExactlyOnce {

  private final AtomicBoolean logged = new AtomicBoolean(false);
  private final Logger log;

  public LogExactlyOnce(final Logger log) {
    this.log = log;
  }

  public void warn(String format, Object...args) {
    if (!logged.getAndSet(true)) {
      log.warn(format, args);
    }
  }
  public void info(String format, Object...args) {
    if (!logged.getAndSet(true)) {
      log.info(format, args);
    }
  }
  public void error(String format, Object...args) {
    if (!logged.getAndSet(true)) {
      log.error(format, args);
    }
  }

  /**
   * Log at DEBUG if nothing has been logged yet.
   * @param format format string
   * @param args arguments
   */
  public void debug(String format, Object...args) {
    if (!logged.getAndSet(true)) {
      log.debug(format, args);
    }
  }
}
