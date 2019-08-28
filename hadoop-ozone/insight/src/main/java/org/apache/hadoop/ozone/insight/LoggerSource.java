/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.insight;

import org.apache.hadoop.ozone.insight.Component.Type;

/**
 * Definition of a log source.
 */
public class LoggerSource {

  /**
   * Id of the component where the log is generated.
   */
  private Component component;

  /**
   * Log4j/slf4j logger name.
   */
  private String loggerName;

  /**
   * Log level.
   */
  private Level level;

  public LoggerSource(Component component, String loggerName, Level level) {
    this.component = component;
    this.loggerName = loggerName;
    this.level = level;
  }

  public LoggerSource(Type componentType, Class<?> loggerClass,
      Level level) {
    this(new Component(componentType), loggerClass.getCanonicalName(), level);
  }

  public Component getComponent() {
    return component;
  }

  public String getLoggerName() {
    return loggerName;
  }

  public Level getLevel() {
    return level;
  }

  /**
   * Log level definition.
   */
  public enum Level {
    TRACE, DEBUG, INFO, WARN, ERROR
  }

}
