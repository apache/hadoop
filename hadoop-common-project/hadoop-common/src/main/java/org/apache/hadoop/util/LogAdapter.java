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

import org.apache.commons.logging.Log;
import org.slf4j.Logger;

class LogAdapter {
  private Log LOG;
  private Logger LOGGER;

  private LogAdapter(Log LOG) {
    this.LOG = LOG;
  }

  private LogAdapter(Logger LOGGER) {
    this.LOGGER = LOGGER;
  }

  public static LogAdapter create(Log LOG) {
    return new LogAdapter(LOG);
  }

  public static LogAdapter create(Logger LOGGER) {
    return new LogAdapter(LOGGER);
  }

  public void info(String msg) {
    if (LOG != null) {
      LOG.info(msg);
    } else if (LOGGER != null) {
      LOGGER.info(msg);
    }
  }

  public void warn(String msg, Throwable t) {
    if (LOG != null) {
      LOG.warn(msg, t);
    } else if (LOGGER != null) {
      LOGGER.warn(msg, t);
    }
  }

  public void debug(Throwable t) {
    if (LOG != null) {
      LOG.debug(t);
    } else if (LOGGER != null) {
      LOGGER.debug("", t);
    }
  }

  public void error(String msg) {
    if (LOG != null) {
      LOG.error(msg);
    } else if (LOGGER != null) {
      LOGGER.error(msg);
    }
  }
}
