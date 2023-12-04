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

import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.LogExactlyOnce;

/**
 * Factory to create a log controller; this uses reflection.
 */
public final class LogControllerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LogControllerFactory.class);

  /**
   * Log once if a controller can't be created.
   */
  private static final LogExactlyOnce WARN_CONTROLLER_NOT_CREATED =
      new LogExactlyOnce(LOG);

  /**
   * The log4j controller class: {@value}.
   */
  static final String LOG4J1CONTROLLER =
      "org.apache.hadoop.fs.s3a.impl.logging.Log4J1Controller";


  /**
   * Create a controller.
   * @return the instantiated controller or empty of the class can't be instantiated.
   */
  public static Optional<LogControl> createController(String classname) {
    try {
      Class<?> clazz = Class.forName(classname);
      return Optional.of((LogControl) clazz.newInstance());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
             | ClassCastException e) {
      WARN_CONTROLLER_NOT_CREATED.warn("Failed to create controller {}: {}", classname, e, e);
      return Optional.empty();
    }
  }

  /**
   * Enable logging through reflection.
   * @param level desired level
   * @param logs loggers to enable
   * @return true if logging was enabled.
   */
  public static boolean enableLogging(LogControl.LogLevel level,
      String... logs) {

    final Optional<LogControl> control =
        LogControllerFactory.createController(LOG4J1CONTROLLER);
    if (control.isPresent()) {
      final LogControl logControl = control.get();
      Arrays.stream(logs).forEach(
          log -> {
            LOG.debug("Setting log level of {} to {}", log, level);
            logControl.setLogLevel(log, level);
          });
      return true;
    } else {
      return false;
    }

  }


}
