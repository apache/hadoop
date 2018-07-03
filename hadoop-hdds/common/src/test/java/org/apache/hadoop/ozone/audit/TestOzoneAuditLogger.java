/**
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
package org.apache.hadoop.ozone.audit;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Test Ozone Audit Logger.
 */
public class TestOzoneAuditLogger {

  private static final Logger LOG = LoggerFactory.getLogger
      (TestOzoneAuditLogger.class.getName());
  private static AuditLogger AUDIT = new AuditLogger(AuditLoggerType.OMLOGGER);
  public DummyEntity auditableObj = new DummyEntity();

  @BeforeClass
  public static void setUp(){
    System.setProperty("log4j.configurationFile", "log4j2.properties");
  }

  @AfterClass
  public static void tearDown() {
      File file = new File("audit.log");
      if (FileUtils.deleteQuietly(file)) {
        LOG.info(file.getName() +
            " has been deleted as all tests have completed.");
      } else {
        LOG.info("audit.log could not be deleted.");
      }
  }

  /**
   * Ensures WriteSuccess events are logged @ INFO and above.
   */
  @Test
  public void logInfoWriteSuccess() throws IOException {
    AUDIT.logWriteSuccess(DummyAction.CREATE_VOLUME, auditableObj.toAuditMap(), Level.INFO);
    String expected = "[INFO ] OMAudit - CREATE_VOLUME [ key1=\"value1\" " +
        "key2=\"value2\"] SUCCESS";
    verifyLog(expected);
  }

  /**
   * Test to verify default log level is INFO
   */
  @Test
  public void verifyDefaultLogLevel() throws IOException {
    AUDIT.logWriteSuccess(DummyAction.CREATE_VOLUME, auditableObj.toAuditMap());
    String expected = "[INFO ] OMAudit - CREATE_VOLUME [ key1=\"value1\" " +
        "key2=\"value2\"] SUCCESS";
    verifyLog(expected);
  }

  /**
   * Test to verify WriteFailure events are logged as ERROR.
   */
  @Test
  public void logErrorWriteFailure() throws IOException {
    AUDIT.logWriteFailure(DummyAction.CREATE_VOLUME, auditableObj.toAuditMap(), Level.ERROR);
    String expected = "[ERROR] OMAudit - CREATE_VOLUME [ key1=\"value1\" " +
        "key2=\"value2\"] FAILURE";
    verifyLog(expected);
  }

  /**
   * Test to verify no READ event is logged.
   */
  @Test
  public void notLogReadEvents() throws IOException {
    AUDIT.logReadSuccess(DummyAction.READ_VOLUME, auditableObj.toAuditMap(), Level.INFO);
    AUDIT.logReadFailure(DummyAction.READ_VOLUME, auditableObj.toAuditMap(), Level.INFO);
    AUDIT.logReadFailure(DummyAction.READ_VOLUME, auditableObj.toAuditMap(), Level.ERROR);
    AUDIT.logReadFailure(DummyAction.READ_VOLUME, auditableObj.toAuditMap(), Level.ERROR,
        new Exception("test"));
    verifyLog(null);
  }

  /**
   * Test to ensure DEBUG level messages are not logged when INFO is enabled.
   */
  @Test
  public void notLogDebugEvents() throws IOException {
    AUDIT.logWriteSuccess(DummyAction.CREATE_VOLUME, auditableObj.toAuditMap(), Level.DEBUG);
    AUDIT.logReadSuccess(DummyAction.READ_VOLUME, auditableObj.toAuditMap(), Level.DEBUG);
    verifyLog(null);
  }

  public void verifyLog(String expected) throws IOException {
      File file = new File("audit.log");
      List<String> lines = FileUtils.readLines(file, (String)null);
      if(expected == null){
        // When no log entry is expected, the log file must be empty
        assertTrue(lines.size() == 0);
      } else {
        // When log entry is expected, the log file will contain one line and
        // that must be equal to the expected string
        assertTrue(expected.equalsIgnoreCase(lines.get(0)));
        //empty the file
        lines.remove(0);
        FileUtils.writeLines(file, lines, false);
      }
  }
}
