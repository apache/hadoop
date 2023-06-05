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

import java.io.FileInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Hadoop's internal class that access log4j APIs directly.
 * <p/>
 * This class will depend on log4j directly, so callers should not use this class directly to avoid
 * introducing log4j dependencies to downstream users. Please call the methods in
 * {@link HadoopLoggerUtils}, as they will call the methods here through reflection.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
final class HadoopInternalLog4jUtils {

  private HadoopInternalLog4jUtils() {
  }

  static void setLogLevel(String loggerName, String levelName) {
    if (loggerName == null) {
      throw new IllegalArgumentException("logger name cannot be null");
    }
    Logger logger = loggerName.equalsIgnoreCase("root") ?
        LogManager.getRootLogger() :
        LogManager.getLogger(loggerName);
    Level level = Level.toLevel(levelName.toUpperCase());
    if (!level.toString().equalsIgnoreCase(levelName)) {
      throw new IllegalArgumentException("Unsupported log level " + levelName);
    }
    logger.setLevel(level);
  }

  static void shutdownLogManager() {
    LogManager.shutdown();
  }

  static String getEffectiveLevel(String loggerName) {
    Logger logger = loggerName.equalsIgnoreCase("root") ?
        LogManager.getRootLogger() :
        LogManager.getLogger(loggerName);
    return logger.getEffectiveLevel().toString();
  }

  static void resetConfiguration() {
    LogManager.resetConfiguration();
  }

  static void updateLog4jConfiguration(Class<?> targetClass, String log4jPath) throws Exception {
    Properties customProperties = new Properties();
    try (FileInputStream fs = new FileInputStream(log4jPath);
        InputStream is = targetClass.getResourceAsStream("/log4j.properties")) {
      customProperties.load(fs);
      Properties originalProperties = new Properties();
      originalProperties.load(is);
      for (Map.Entry<Object, Object> entry : customProperties.entrySet()) {
        originalProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    }
  }

  static boolean hasAppenders(String logger) {
    return Logger.getLogger(logger)
        .getAllAppenders()
        .hasMoreElements();
  }

  @SuppressWarnings("unchecked")
  static void syncLogs() {
    // flush standard streams
    //
    System.out.flush();
    System.err.flush();

    // flush flushable appenders
    //
    final Logger rootLogger = Logger.getRootLogger();
    flushAppenders(rootLogger);
    final Enumeration<Logger> allLoggers = rootLogger.getLoggerRepository().
        getCurrentLoggers();
    while (allLoggers.hasMoreElements()) {
      final Logger l = allLoggers.nextElement();
      flushAppenders(l);
    }
  }

  @SuppressWarnings("unchecked")
  private static void flushAppenders(Logger l) {
    final Enumeration<Appender> allAppenders = l.getAllAppenders();
    while (allAppenders.hasMoreElements()) {
      final Appender a = allAppenders.nextElement();
      if (a instanceof Flushable) {
        try {
          ((Flushable) a).flush();
        } catch (IOException ioe) {
          System.err.println(a + ": Failed to flush!"
              + stringifyException(ioe));
        }
      }
    }
  }

  private static String stringifyException(Throwable e) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    e.printStackTrace(printWriter);
    printWriter.close();
    return stringWriter.toString();
  }

}
