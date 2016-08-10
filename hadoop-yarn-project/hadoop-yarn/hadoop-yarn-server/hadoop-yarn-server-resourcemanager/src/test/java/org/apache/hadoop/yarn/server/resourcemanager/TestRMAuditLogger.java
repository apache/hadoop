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
package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRPC.TestImpl;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.Keys;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

/**
 * Tests {@link RMAuditLogger}.
 */
public class TestRMAuditLogger {
  private static final String USER = "test";
  private static final String OPERATION = "oper";
  private static final String TARGET = "tgt";
  private static final String PERM = "admin group";
  private static final String DESC = "description of an audit log";
  private static final ApplicationId APPID = mock(ApplicationId.class);
  private static final ApplicationAttemptId ATTEMPTID = mock(ApplicationAttemptId.class);
  private static final ContainerId CONTAINERID = mock(ContainerId.class);

  @Before
  public void setUp() throws Exception {
    when(APPID.toString()).thenReturn("app_1");
    when(ATTEMPTID.toString()).thenReturn("app_attempt_1");
    when(CONTAINERID.toString()).thenReturn("container_1");
  }


  /**
   * Test the AuditLog format with key-val pair.
   */
  @Test  
  public void testKeyValLogFormat() throws Exception {
    StringBuilder actLog = new StringBuilder();
    StringBuilder expLog = new StringBuilder();
    // add the first k=v pair and check
    RMAuditLogger.start(Keys.USER, USER, actLog);
    expLog.append("USER=test");
    assertEquals(expLog.toString(), actLog.toString());

    // append another k1=v1 pair to already added k=v and test
    RMAuditLogger.add(Keys.OPERATION, OPERATION, actLog);
    expLog.append("\tOPERATION=oper");
    assertEquals(expLog.toString(), actLog.toString());

    // append another k1=null pair and test
    RMAuditLogger.add(Keys.APPID, (String)null, actLog);
    expLog.append("\tAPPID=null");
    assertEquals(expLog.toString(), actLog.toString());

    // now add the target and check of the final string
    RMAuditLogger.add(Keys.TARGET, TARGET, actLog);
    expLog.append("\tTARGET=tgt");
    assertEquals(expLog.toString(), actLog.toString());
  }


  /**
   * Test the AuditLog format for successful events.
   */
  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId,
      InetAddress remoteIp) {
    InetAddress ip;
    if (remoteIp != null) {
      ip = remoteIp;
    } else {
      ip = Server.getRemoteIp();
    }

    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");

    String sLog;
    if (checkIP) {
      sLog = RMAuditLogger.createSuccessLog(USER, OPERATION, TARGET, appId,
          attemptId, containerId, ip);
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    } else {
      sLog = RMAuditLogger.createSuccessLog(USER, OPERATION, TARGET, appId,
          attemptId, containerId);
    }
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=SUCCESS");

    if (appId != null) {
      expLog.append("\tAPPID=app_1");
    }
    if (attemptId != null) {
      expLog.append("\tAPPATTEMPTID=app_attempt_1");
    }
    if (containerId != null) {
      expLog.append("\tCONTAINERID=container_1");
    }
    assertEquals(expLog.toString(), sLog);
  }

  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId, null);
  }

  /**
   * Test the AuditLog format for successful events passing nulls.
   */
  private void testSuccessLogNulls(boolean checkIP) {
    String sLog = RMAuditLogger.createSuccessLog(null, null, null, null, 
        null, null);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=null\t");
    if (checkIP) {
      InetAddress ip = Server.getRemoteIp();
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    }
    expLog.append("OPERATION=null\tTARGET=null\tRESULT=SUCCESS");
    assertEquals(expLog.toString(), sLog);
  }

  private void testSuccessLogFormatHelperWithIP(boolean checkIP,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, InetAddress ip) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId, ip);
  }

  /**
   * Tests the SuccessLog with two IP addresses.
   *
   * @param checkIP
   * @param appId
   * @param attemptId
   * @param containerId
   */
  private void testSuccessLogFormatHelperWithIP(boolean checkIP,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId) {
    testSuccessLogFormatHelperWithIP(checkIP, APPID, ATTEMPTID, CONTAINERID,
        InetAddress.getLoopbackAddress());
    byte[] ipAddr = new byte[] { 100, 10, 10, 1 };

    InetAddress addr = null;
    try {
      addr = InetAddress.getByAddress(ipAddr);
    } catch (UnknownHostException uhe) {
      // should not happen as long as IP address format
      // stays the same
      Assert.fail("Check ip address being constructed");
    }
    testSuccessLogFormatHelperWithIP(checkIP, APPID, ATTEMPTID, CONTAINERID,
        addr);
  }

  /**
   * Test the AuditLog format for successful events with the various
   * parameters.
   */
  private void testSuccessLogFormat(boolean checkIP) {
    testSuccessLogFormatHelper(checkIP, null, null, null);
    testSuccessLogFormatHelper(checkIP, APPID, null, null);
    testSuccessLogFormatHelper(checkIP, null, null, CONTAINERID);
    testSuccessLogFormatHelper(checkIP, null, ATTEMPTID, null);
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, null);
    testSuccessLogFormatHelper(checkIP, APPID, null, CONTAINERID);
    testSuccessLogFormatHelper(checkIP, null, ATTEMPTID, CONTAINERID);
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID);
    testSuccessLogFormatHelperWithIP(checkIP, APPID, ATTEMPTID, CONTAINERID);
    testSuccessLogNulls(checkIP);
  }


  /**
   * Test the AuditLog format for failure events.
   */
  private void testFailureLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId) {
    String fLog =
      RMAuditLogger.createFailureLog(USER, OPERATION, PERM, TARGET, DESC,
      appId, attemptId, containerId);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    if (checkIP) {
      InetAddress ip = Server.getRemoteIp();
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    }
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=FAILURE\t");
    expLog.append("DESCRIPTION=description of an audit log");
    expLog.append("\tPERMISSIONS=admin group");
    if (appId != null) {
      expLog.append("\tAPPID=app_1");
    }
    if (attemptId != null) {
      expLog.append("\tAPPATTEMPTID=app_attempt_1");
    }
    if (containerId != null) {
      expLog.append("\tCONTAINERID=container_1");
    }
    assertEquals(expLog.toString(), fLog);
  }

  /**
   * Test the AuditLog format for failure events with the various
   * parameters.
   */
  private void testFailureLogFormat(boolean checkIP) {
    testFailureLogFormatHelper(checkIP, null, null, null);
    testFailureLogFormatHelper(checkIP, APPID, null, null);
    testFailureLogFormatHelper(checkIP, null, null, CONTAINERID);
    testFailureLogFormatHelper(checkIP, null, ATTEMPTID, null);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, null);
    testFailureLogFormatHelper(checkIP, APPID, null, CONTAINERID);
    testFailureLogFormatHelper(checkIP, null, ATTEMPTID, CONTAINERID);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID);
  }

  /**
   * Test {@link RMAuditLogger} without IP set.
   */
  @Test  
  public void testRMAuditLoggerWithoutIP() throws Exception {
    // test without ip
    testSuccessLogFormat(false);
    testFailureLogFormat(false);
  }

  /**
   * A special extension of {@link TestImpl} RPC server with 
   * {@link TestImpl#ping()} testing the audit logs.
   */
  private class MyTestRPCServer extends TestImpl {
    @Override
    public void ping() {
      // test with ip set
      testSuccessLogFormat(true);
      testFailureLogFormat(true);
    }
  }

  /**
   * Test {@link RMAuditLogger} with IP set.
   */
  @Test  
  public void testRMAuditLoggerWithIP() throws Exception {
    Configuration conf = new Configuration();
    // start the IPC server
    Server server = new RPC.Builder(conf).setProtocol(TestProtocol.class)
        .setInstance(new MyTestRPCServer()).setBindAddress("0.0.0.0")
        .setPort(0).setNumHandlers(5).setVerbose(true).build();
    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);

    // Make a client connection and test the audit log
    TestProtocol proxy = (TestProtocol)RPC.getProxy(TestProtocol.class,
                           TestProtocol.versionID, addr, conf);
    // Start the testcase
    proxy.ping();

    server.stop();
  }
}
