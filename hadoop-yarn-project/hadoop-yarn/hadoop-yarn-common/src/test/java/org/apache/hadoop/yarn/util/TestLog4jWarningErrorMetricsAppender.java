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
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Level;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestLog4jWarningErrorMetricsAppender {

  Log4jWarningErrorMetricsAppender appender;
  private static final Logger LOG = LoggerFactory.
      getLogger(TestLog4jWarningErrorMetricsAppender.class);
  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");
  List<Long> cutoff = new ArrayList<>();

  void setupAppender(int cleanupIntervalSeconds, long messageAgeLimitSeconds,
      int maxUniqueMessages) {
    removeAppender();
    appender =
        new Log4jWarningErrorMetricsAppender(cleanupIntervalSeconds,
          messageAgeLimitSeconds, maxUniqueMessages);
    LogManager.getRootLogger().addAppender(appender);
  }

  void removeAppender() {
    LogManager.getRootLogger().removeAppender(appender);
  }

  void logMessages(Level level, String message, int count) {
    for (int i = 0; i < count; ++i) {
      switch (level.toInt()) {
      case Level.FATAL_INT:
        LOG.error(FATAL, message);
        break;
      case Level.ERROR_INT:
        LOG.error(message);
        break;
      case Level.WARN_INT:
        LOG.warn(message);
        break;
      case Level.INFO_INT:
        LOG.info(message);
        break;
      case Level.DEBUG_INT:
        LOG.debug(message);
        break;
      case Level.TRACE_INT:
        LOG.trace(message);
        break;
      }
    }
  }

  @Test
  public void testPurge() throws Exception {
    setupAppender(2, 1, 1);
    logMessages(Level.ERROR, "test message 1", 1);
    cutoff.clear();
    cutoff.add(0L);
    Assert.assertEquals(1, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(1, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert.assertEquals(1, appender.getErrorMessagesAndCounts(cutoff).get(0)
      .size());
    Thread.sleep(3000);
    Assert.assertEquals(1, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(0, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert.assertEquals(0, appender.getErrorMessagesAndCounts(cutoff).get(0)
      .size());

    setupAppender(2, 1000, 2);

    logMessages(Level.ERROR, "test message 1", 3);
    logMessages(Level.ERROR, "test message 2", 2);

    Assert.assertEquals(1, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(5, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert.assertEquals(2, appender.getErrorMessagesAndCounts(cutoff).get(0)
      .size());
    logMessages(Level.ERROR, "test message 3", 3);
    Thread.sleep(2000);
    Assert.assertEquals(8, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert.assertEquals(2, appender.getErrorMessagesAndCounts(cutoff).get(0)
      .size());
  }

  @Test
  public void testErrorCounts() throws Exception {
    cutoff.clear();
    setupAppender(100, 100, 100);
    cutoff.add(0L);
    logMessages(Level.ERROR, "test message 1", 2);
    logMessages(Level.ERROR, "test message 2", 3);
    Assert.assertEquals(1, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(1, appender.getWarningCounts(cutoff).size());
    Assert.assertEquals(5, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert
      .assertEquals(0, appender.getWarningCounts(cutoff).get(0).longValue());
    Thread.sleep(1000);
    cutoff.add(Time.now() / 1000);
    logMessages(Level.ERROR, "test message 3", 2);
    Assert.assertEquals(2, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(2, appender.getWarningCounts(cutoff).size());
    Assert.assertEquals(7, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert.assertEquals(2, appender.getErrorCounts(cutoff).get(1).longValue());
    Assert
      .assertEquals(0, appender.getWarningCounts(cutoff).get(0).longValue());
    Assert
      .assertEquals(0, appender.getWarningCounts(cutoff).get(1).longValue());
  }

  @Test
  public void testWarningCounts() throws Exception {
    cutoff.clear();
    setupAppender(100, 100, 100);
    cutoff.add(0L);
    logMessages(Level.WARN, "test message 1", 2);
    logMessages(Level.WARN, "test message 2", 3);
    Assert.assertEquals(1, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(1, appender.getWarningCounts(cutoff).size());
    Assert.assertEquals(0, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert
      .assertEquals(5, appender.getWarningCounts(cutoff).get(0).longValue());
    Thread.sleep(1000);
    cutoff.add(Time.now() / 1000);
    logMessages(Level.WARN, "test message 3", 2);
    Assert.assertEquals(2, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(2, appender.getWarningCounts(cutoff).size());
    Assert.assertEquals(0, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert.assertEquals(0, appender.getErrorCounts(cutoff).get(1).longValue());
    Assert
      .assertEquals(7, appender.getWarningCounts(cutoff).get(0).longValue());
    Assert
      .assertEquals(2, appender.getWarningCounts(cutoff).get(1).longValue());
  }

  @Test
  public void testWarningMessages() throws Exception {
    cutoff.clear();
    setupAppender(100, 100, 100);
    cutoff.add(0L);
    logMessages(Level.WARN, "test message 1", 2);
    logMessages(Level.WARN, "test message 2", 3);
    Assert.assertEquals(1, appender.getErrorMessagesAndCounts(cutoff).size());
    Assert.assertEquals(1, appender.getWarningMessagesAndCounts(cutoff).size());
    Map<String, Log4jWarningErrorMetricsAppender.Element> errorsMap =
        appender.getErrorMessagesAndCounts(cutoff).get(0);
    Map<String, Log4jWarningErrorMetricsAppender.Element> warningsMap =
        appender.getWarningMessagesAndCounts(cutoff).get(0);
    Assert.assertEquals(0, errorsMap.size());
    Assert.assertEquals(2, warningsMap.size());
    Assert.assertTrue(warningsMap.containsKey("test message 1"));
    Assert.assertTrue(warningsMap.containsKey("test message 2"));
    Log4jWarningErrorMetricsAppender.Element msg1Info = warningsMap.get("test message 1");
    Log4jWarningErrorMetricsAppender.Element msg2Info = warningsMap.get("test message 2");
    Assert.assertEquals(2, msg1Info.count.intValue());
    Assert.assertEquals(3, msg2Info.count.intValue());
    Thread.sleep(1000);
    cutoff.add(Time.now() / 1000);
    logMessages(Level.WARN, "test message 3", 2);
    Assert.assertEquals(2, appender.getErrorMessagesAndCounts(cutoff).size());
    Assert.assertEquals(2, appender.getWarningMessagesAndCounts(cutoff).size());
    errorsMap = appender.getErrorMessagesAndCounts(cutoff).get(0);
    warningsMap = appender.getWarningMessagesAndCounts(cutoff).get(0);
    Assert.assertEquals(0, errorsMap.size());
    Assert.assertEquals(3, warningsMap.size());
    Assert.assertTrue(warningsMap.containsKey("test message 3"));
    errorsMap = appender.getErrorMessagesAndCounts(cutoff).get(1);
    warningsMap = appender.getWarningMessagesAndCounts(cutoff).get(1);
    Assert.assertEquals(0, errorsMap.size());
    Assert.assertEquals(1, warningsMap.size());
    Assert.assertTrue(warningsMap.containsKey("test message 3"));
    Log4jWarningErrorMetricsAppender.Element msg3Info = warningsMap.get("test message 3");
    Assert.assertEquals(2, msg3Info.count.intValue());
  }

  @Test
  public void testErrorMessages() throws Exception {
    cutoff.clear();
    setupAppender(100, 100, 100);
    cutoff.add(0L);
    logMessages(Level.ERROR, "test message 1", 2);
    logMessages(Level.ERROR, "test message 2", 3);
    Assert.assertEquals(1, appender.getErrorMessagesAndCounts(cutoff).size());
    Assert.assertEquals(1, appender.getWarningMessagesAndCounts(cutoff).size());
    Map<String, Log4jWarningErrorMetricsAppender.Element> errorsMap =
        appender.getErrorMessagesAndCounts(cutoff).get(0);
    Map<String, Log4jWarningErrorMetricsAppender.Element> warningsMap =
        appender.getWarningMessagesAndCounts(cutoff).get(0);
    Assert.assertEquals(2, errorsMap.size());
    Assert.assertEquals(0, warningsMap.size());
    Assert.assertTrue(errorsMap.containsKey("test message 1"));
    Assert.assertTrue(errorsMap.containsKey("test message 2"));
    Log4jWarningErrorMetricsAppender.Element msg1Info = errorsMap.get("test message 1");
    Log4jWarningErrorMetricsAppender.Element msg2Info = errorsMap.get("test message 2");
    Assert.assertEquals(2, msg1Info.count.intValue());
    Assert.assertEquals(3, msg2Info.count.intValue());
    Thread.sleep(1000);
    cutoff.add(Time.now() / 1000);
    logMessages(Level.ERROR, "test message 3", 2);
    Assert.assertEquals(2, appender.getErrorMessagesAndCounts(cutoff).size());
    Assert.assertEquals(2, appender.getWarningMessagesAndCounts(cutoff).size());
    errorsMap = appender.getErrorMessagesAndCounts(cutoff).get(0);
    warningsMap = appender.getWarningMessagesAndCounts(cutoff).get(0);
    Assert.assertEquals(3, errorsMap.size());
    Assert.assertEquals(0, warningsMap.size());
    Assert.assertTrue(errorsMap.containsKey("test message 3"));
    errorsMap = appender.getErrorMessagesAndCounts(cutoff).get(1);
    warningsMap = appender.getWarningMessagesAndCounts(cutoff).get(1);
    Assert.assertEquals(1, errorsMap.size());
    Assert.assertEquals(0, warningsMap.size());
    Assert.assertTrue(errorsMap.containsKey("test message 3"));
    Log4jWarningErrorMetricsAppender.Element msg3Info = errorsMap.get("test message 3");
    Assert.assertEquals(2, msg3Info.count.intValue());
  }

  @Test
  public void testInfoDebugTrace() {
    cutoff.clear();
    setupAppender(100, 100, 100);
    cutoff.add(0L);
    logMessages(Level.INFO, "test message 1", 2);
    logMessages(Level.DEBUG, "test message 2", 2);
    logMessages(Level.TRACE, "test message 3", 2);
    Assert.assertEquals(1, appender.getErrorMessagesAndCounts(cutoff).size());
    Assert.assertEquals(1, appender.getWarningMessagesAndCounts(cutoff).size());
    Assert.assertEquals(1, appender.getErrorCounts(cutoff).size());
    Assert.assertEquals(1, appender.getWarningCounts(cutoff).size());
    Assert.assertEquals(0, appender.getErrorCounts(cutoff).get(0).longValue());
    Assert
      .assertEquals(0, appender.getWarningCounts(cutoff).get(0).longValue());
    Assert.assertEquals(0, appender.getErrorMessagesAndCounts(cutoff).get(0)
      .size());
    Assert.assertEquals(0, appender.getWarningMessagesAndCounts(cutoff).get(0)
      .size());
  }

}
