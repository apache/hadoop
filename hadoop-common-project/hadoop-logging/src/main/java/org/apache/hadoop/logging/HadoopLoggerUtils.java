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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A bridge class for operating on logging framework, such as changing log4j log level, etc.
 * Will call the methods in {@link HadoopInternalLog4jUtils} to perform operations on log4j level.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HadoopLoggerUtils {

  private static final String INTERNAL_UTILS_CLASS =
      "org.apache.hadoop.logging.HadoopInternalLog4jUtils";

  private HadoopLoggerUtils() {
  }

  private static Method getMethod(String methodName, Class<?>... args) {
    try {
      Class<?> clazz = Class.forName(INTERNAL_UTILS_CLASS);
      return clazz.getDeclaredMethod(methodName, args);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new AssertionError("should not happen", e);
    }
  }

  private static void throwUnchecked(Throwable throwable) {
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    }
    if (throwable instanceof Error) {
      throw (Error) throwable;
    }
  }

  public static void shutdownLogManager() {
    Method method = getMethod("shutdownLogManager");
    try {
      method.invoke(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

  public static void setLogLevel(String loggerName, String levelName) {
    Method method = getMethod("setLogLevel", String.class, String.class);
    try {
      method.invoke(null, loggerName, levelName);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

  public static String getEffectiveLevel(String loggerName) {
    Method method = getMethod("getEffectiveLevel", String.class);
    try {
      return (String) method.invoke(null, loggerName);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

  public static void resetConfiguration() {
    Method method = getMethod("resetConfiguration");
    try {
      method.invoke(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

  public static void updateLog4jConfiguration(Class<?> targetClass, String log4jPath) {
    Method method = getMethod("updateLog4jConfiguration", Class.class, String.class);
    try {
      method.invoke(null, targetClass, log4jPath);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

  public static boolean hasAppenders(String logger) {
    Method method = getMethod("hasAppenders", String.class);
    try {
      return (Boolean) method.invoke(null, logger);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

  public synchronized static void syncLogs() {
    Method method = getMethod("syncLogs");
    try {
      method.invoke(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("Failed to execute, should not happen", e.getCause());
    }
  }

}
