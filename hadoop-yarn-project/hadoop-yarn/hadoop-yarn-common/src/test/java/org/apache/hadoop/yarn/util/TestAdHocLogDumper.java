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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Priority;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.util.GenericsUtil.isLog4jLogger;

public class TestAdHocLogDumper {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestAdHocLogDumper.class);

  @Test
  public void testDumpingSchedulerLogs() throws Exception {

    Map<Appender, Priority> levels = new HashMap<>();
    String logFilename = "test.log";
    Logger logger = LoggerFactory.getLogger(TestAdHocLogDumper.class);
    if (isLog4jLogger(this.getClass())) {
      for (Enumeration appenders = LogManager.getRootLogger().
          getAllAppenders(); appenders.hasMoreElements();) {
        Object obj = appenders.nextElement();
        if (obj instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton) obj;
          levels.put(appender, appender.getThreshold());
        }
      }
    }

    AdHocLogDumper dumper = new AdHocLogDumper(this.getClass().getName(),
        logFilename);
    dumper.dumpLogs("DEBUG", 1000);
    LOG.debug("test message 1");
    LOG.info("test message 2");
    File logFile = new File(logFilename);
    Assert.assertTrue(logFile.exists());
    Thread.sleep(2000);
    long lastWrite = logFile.lastModified();
    Assert.assertTrue(lastWrite < Time.now());
    Assert.assertTrue(logFile.length() != 0);

    // make sure levels are set back to their original values
    if (isLog4jLogger(this.getClass())) {
      for (Enumeration appenders = LogManager.getRootLogger().
          getAllAppenders(); appenders.hasMoreElements();) {
        Object obj = appenders.nextElement();
        if (obj instanceof AppenderSkeleton) {
          AppenderSkeleton appender = (AppenderSkeleton) obj;
          Assert.assertEquals(levels.get(appender), appender.getThreshold());
        }
      }
    }
    boolean del = logFile.delete();
    if(!del) {
      LOG.info("Couldn't clean up after test");
    }
  }
}
