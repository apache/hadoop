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
package org.apache.hadoop.util;

import org.slf4j.Logger;

@Deprecated
class LogAdapter {

  private final Logger logger;

  private LogAdapter(Logger logger) {
    this.logger = logger;
  }

  public static LogAdapter create(Logger logger) {
    return new LogAdapter(logger);
  }

  public void info(String msg) {
    logger.info(msg);
  }

  public void warn(String msg, Throwable t) {
    logger.warn(msg, t);
  }

  public void debug(Throwable t) {
    logger.debug("", t);
  }

  public void error(String msg) {
    logger.error(msg);
  }
}
