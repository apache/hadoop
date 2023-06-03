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

package org.apache.hadoop.logging;

import java.io.StringWriter;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

public class LogCapturer {
  private final StringWriter sw = new StringWriter();
  private final Appender appender;
  private final Logger logger;

  public static LogCapturer captureLogs(org.slf4j.Logger logger) {
    if (logger.getName().equals("root")) {
      return new LogCapturer(Logger.getRootLogger());
    }
    return new LogCapturer(LogManager.getLogger(logger.getName()));
  }

  private LogCapturer(Logger logger) {
    this.logger = logger;
    Appender defaultAppender = Logger.getRootLogger().getAppender("stdout");
    if (defaultAppender == null) {
      defaultAppender = Logger.getRootLogger().getAppender("console");
    }
    final Layout layout =
        (defaultAppender == null) ? new PatternLayout() : defaultAppender.getLayout();
    this.appender = new WriterAppender(layout, sw);
    logger.addAppender(this.appender);
  }

  public String getOutput() {
    return sw.toString();
  }

  public void stopCapturing() {
    logger.removeAppender(appender);
  }

  public void clearOutput() {
    sw.getBuffer().setLength(0);
  }
}
