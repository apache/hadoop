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

package org.apache.hadoop.yarn.util;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AdHocLogDumper {

  private static final Logger LOG =
      LogManager.getLogger(AdHocLogDumper.class);

  private String name;
  private String targetFilename;
  private Map<String, Priority> appenderLevels;
  private Level currentLogLevel;
  public static final String AD_HOC_DUMPER_APPENDER = "ad-hoc-dumper-appender";
  private static volatile boolean logFlag = false;
  private static final Object lock = new Object();

  public AdHocLogDumper(String name, String targetFilename) {
    this.name = name;
    this.targetFilename = targetFilename;
    appenderLevels = new HashMap<>();
  }

  public void dumpLogs(String level, int timePeriod)
      throws YarnRuntimeException, IOException {
    synchronized (lock){
      if (logFlag) {
        LOG.info("Attempt to dump logs when appender is already running");
        throw new YarnRuntimeException("Appender is already dumping logs");
      }
      Level targetLevel = Level.toLevel(level);
      Logger logger = LogManager.getLogger(name);
      appenderLevels.clear();
      currentLogLevel = logger.getLevel();
      Level currentEffectiveLevel = logger.getEffectiveLevel();

      // make sure we can create the appender first
      Layout layout = new PatternLayout("%d{ISO8601} %p %c: %m%n");
      FileAppender fApp;
      File file =
          new File(System.getProperty("yarn.log.dir"), targetFilename);
      try {
        fApp = new FileAppender(layout, file.getAbsolutePath(), false);
      } catch (IOException ie) {
        LOG.warn("Error creating file, can't dump logs to "
            + file.getAbsolutePath(), ie);
        throw ie;
      }
      fApp.setName(AdHocLogDumper.AD_HOC_DUMPER_APPENDER);
      fApp.setThreshold(targetLevel);

      // get current threshold of all appenders and set it to the effective
      // level
      for (Enumeration appenders = Logger.getRootLogger().getAllAppenders();
          appenders.hasMoreElements();) {
        Object obj = appenders.nextElement();
        if (obj instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton) obj;
          appenderLevels.put(appender.getName(), appender.getThreshold());
          appender.setThreshold(currentEffectiveLevel);
        }
      }

      logger.addAppender(fApp);
      LOG.info("Dumping adhoc logs for " + name + " to "
          + file.getAbsolutePath() + " for " + timePeriod + " milliseconds");
      logger.setLevel(targetLevel);
      logFlag = true;

      TimerTask restoreLogLevel = new RestoreLogLevel();
      Timer restoreLogLevelTimer = new Timer();
      restoreLogLevelTimer.schedule(restoreLogLevel, timePeriod);
    }
  }

  @VisibleForTesting
  public static boolean getState() {
    return logFlag;
  }

  class RestoreLogLevel extends TimerTask {
    @Override
    public void run() {
      Logger logger = LogManager.getLogger(name);
      logger.removeAppender(AD_HOC_DUMPER_APPENDER);
      logger.setLevel(currentLogLevel);
      for (Enumeration appenders = Logger.getRootLogger().getAllAppenders();
          appenders.hasMoreElements();) {
        Object obj = appenders.nextElement();
        if (obj instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton) obj;
          appender.setThreshold(appenderLevels.get(appender.getName()));
        }
      }
      logFlag = false;
      LOG.info("Done dumping adhoc logs for " + name);
    }
  }
}
