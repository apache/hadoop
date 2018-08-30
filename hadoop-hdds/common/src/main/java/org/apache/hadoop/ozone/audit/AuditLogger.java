/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.audit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.spi.ExtendedLogger;


/**
 * Class to define Audit Logger for Ozone.
 */
public class AuditLogger {

  private ExtendedLogger logger;
  private static final String FQCN = AuditLogger.class.getName();
  private static final Marker WRITE_MARKER = AuditMarker.WRITE.getMarker();
  private static final Marker READ_MARKER = AuditMarker.READ.getMarker();

  /**
   * Parametrized Constructor to initialize logger.
   * @param type Audit Logger Type
   */
  public AuditLogger(AuditLoggerType type){
    initializeLogger(type);
  }

  /**
   * Initializes the logger with specific type.
   * @param loggerType specified one of the values from enum AuditLoggerType.
   */
  private void initializeLogger(AuditLoggerType loggerType){
    this.logger = LogManager.getContext(false).getLogger(loggerType.getType());
  }

  @VisibleForTesting
  public ExtendedLogger getLogger() {
    return logger;
  }

  public void logWriteSuccess(AuditMessage msg) {
    logWriteSuccess(Level.INFO, msg);
  }

  public void logWriteSuccess(Level level, AuditMessage msg) {
    this.logger.logIfEnabled(FQCN, level, WRITE_MARKER, msg, null);
  }

  public void logWriteFailure(AuditMessage msg) {
    logWriteFailure(Level.ERROR, msg);
  }

  public void logWriteFailure(Level level, AuditMessage msg) {
    logWriteFailure(level, msg, null);
  }

  public void logWriteFailure(AuditMessage msg, Throwable exception) {
    logWriteFailure(Level.ERROR, msg, exception);
  }

  public void logWriteFailure(Level level, AuditMessage msg,
      Throwable exception) {
    this.logger.logIfEnabled(FQCN, level, WRITE_MARKER, msg, exception);
  }

  public void logReadSuccess(AuditMessage msg) {
    logReadSuccess(Level.INFO, msg);
  }

  public void logReadSuccess(Level level, AuditMessage msg) {
    this.logger.logIfEnabled(FQCN, level, READ_MARKER, msg, null);
  }

  public void logReadFailure(AuditMessage msg) {
    logReadFailure(Level.ERROR, msg);
  }

  public void logReadFailure(Level level, AuditMessage msg) {
    logReadFailure(level, msg, null);
  }

  public void logReadFailure(AuditMessage msg, Throwable exception) {
    logReadFailure(Level.ERROR, msg, exception);
  }

  public void logReadFailure(Level level, AuditMessage msg,
      Throwable exception) {
    this.logger.logIfEnabled(FQCN, level, READ_MARKER, msg, exception);
  }

}
