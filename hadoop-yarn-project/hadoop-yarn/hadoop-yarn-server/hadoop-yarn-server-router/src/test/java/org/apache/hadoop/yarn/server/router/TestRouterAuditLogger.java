/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.router;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link RouterAuditLogger}.
 */
public class TestRouterAuditLogger {
  private static final String USER = "test";
  private static final String OPERATION = "oper";
  private static final String TARGET = "tgt";
  private static final String DESC = "description of an audit log";

  private static final ApplicationId APPID = mock(ApplicationId.class);
  private static final SubClusterId SUBCLUSTERID = mock(SubClusterId.class);

  @Before public void setUp() throws Exception {
    when(APPID.toString()).thenReturn("app_1");
    when(SUBCLUSTERID.toString()).thenReturn("sc0");
  }

  /**
   * Test the AuditLog format with key-val pair.
   */
  @Test
  public void testKeyValLogFormat() throws Exception {
    StringBuilder actLog = new StringBuilder();
    StringBuilder expLog = new StringBuilder();

    // add the first k=v pair and check
    RouterAuditLogger.start(RouterAuditLogger.Keys.USER, USER, actLog);
    expLog.append("USER=test");
    assertEquals(expLog.toString(), actLog.toString());

    // append another k1=v1 pair to already added k=v and test
    RouterAuditLogger.add(RouterAuditLogger.Keys.OPERATION, OPERATION, actLog);
    expLog.append("\tOPERATION=oper");
    assertEquals(expLog.toString(), actLog.toString());

    // append another k1=null pair and test
    RouterAuditLogger.add(RouterAuditLogger.Keys.APPID, (String) null, actLog);
    expLog.append("\tAPPID=null");
    assertEquals(expLog.toString(), actLog.toString());

    // now add the target and check of the final string
    RouterAuditLogger.add(RouterAuditLogger.Keys.TARGET, TARGET, actLog);
    expLog.append("\tTARGET=tgt");
    assertEquals(expLog.toString(), actLog.toString());
  }

  /**
   * Test the AuditLog format for successful events.
   */
  private void testSuccessLogFormatHelper(ApplicationId appId,
      SubClusterId subClusterId) {
    // check without the IP
    String sLog = RouterAuditLogger
        .createSuccessLog(USER, OPERATION, TARGET, appId, subClusterId);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=SUCCESS");
    if (appId != null) {
      expLog.append("\tAPPID=app_1");
    }
    if (subClusterId != null) {
      expLog.append("\tSUBCLUSTERID=sc0");
    }
    assertEquals(expLog.toString(), sLog);
  }

  /**
   * Test the AuditLog format for successful events passing nulls.
   */
  private void testSuccessLogNulls() {
    String sLog =
        RouterAuditLogger.createSuccessLog(null, null, null, null, null);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=null\t");
    expLog.append("OPERATION=null\tTARGET=null\tRESULT=SUCCESS");
    assertEquals(expLog.toString(), sLog);
  }

  /**
   * Test the AuditLog format for successful events with the various
   * parameters.
   */
  private void testSuccessLogFormat() {
    testSuccessLogFormatHelper(null, null);
    testSuccessLogFormatHelper(APPID, null);
    testSuccessLogFormatHelper(null, SUBCLUSTERID);
    testSuccessLogFormatHelper(APPID, SUBCLUSTERID);
  }

  /**
   *  Test the AuditLog format for failure events.
   */
  private void testFailureLogFormatHelper(ApplicationId appId,
      SubClusterId subClusterId) {
    String fLog = RouterAuditLogger
        .createFailureLog(USER, OPERATION, "UNKNOWN", TARGET, DESC, appId,
            subClusterId);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=FAILURE\t");
    expLog.append("DESCRIPTION=description of an audit log");
    expLog.append("\tPERMISSIONS=UNKNOWN");

    if (appId != null) {
      expLog.append("\tAPPID=app_1");
    }
    if (subClusterId != null) {
      expLog.append("\tSUBCLUSTERID=sc0");
    }
    assertEquals(expLog.toString(), fLog);
  }

  /**
   * Test the AuditLog format for failure events with the various
   * parameters.
   */
  private void testFailureLogFormat() {
    testFailureLogFormatHelper(null, null);
    testFailureLogFormatHelper(APPID, null);
    testFailureLogFormatHelper(null, SUBCLUSTERID);
    testFailureLogFormatHelper(APPID, SUBCLUSTERID);
  }

  /**
   *  Test {@link RouterAuditLogger}.
   */
  @Test public void testRouterAuditLogger() throws Exception {
    testSuccessLogFormat();
    testFailureLogFormat();
  }
}
