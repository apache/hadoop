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
package org.apache.hadoop.yarn.server.nodemanager;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRPC.TestImpl;
import org.apache.hadoop.ipc.TestRpcBase.TestRpcService;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.Keys;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link NMAuditLogger}.
 */
public class TestNMAuditLogger {
  private static final String USER = "test";
  private static final String OPERATION = "oper";
  private static final String TARGET = "tgt";
  private static final String DESC = "description of an audit log";

  private static final ApplicationId APPID = mock(ApplicationId.class);
  private static final ContainerId CONTAINERID = mock(ContainerId.class);

  @Before
  public void setUp() throws Exception {
    when(APPID.toString()).thenReturn("app_1");
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
    NMAuditLogger.start(Keys.USER, USER, actLog);
    expLog.append("USER=test");
    assertEquals(expLog.toString(), actLog.toString());

    // append another k1=v1 pair to already added k=v and test
    NMAuditLogger.add(Keys.OPERATION, OPERATION, actLog);
    expLog.append("\tOPERATION=oper");
    assertEquals(expLog.toString(), actLog.toString());

    // append another k1=null pair and test
    NMAuditLogger.add(Keys.APPID, (String)null, actLog);
    expLog.append("\tAPPID=null");
    assertEquals(expLog.toString(), actLog.toString());

    // now add the target and check of the final string
    NMAuditLogger.add(Keys.TARGET, TARGET, actLog);
    expLog.append("\tTARGET=tgt");
    assertEquals(expLog.toString(), actLog.toString());
  }


  /**
   * Test the AuditLog format for successful events.
   */
  private void testSuccessLogFormatHelper(boolean checkIP, 
      ApplicationId appId, ContainerId containerId) {
    // check without the IP
    String sLog = NMAuditLogger.createSuccessLog(USER, OPERATION, TARGET,
        appId, containerId);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    if (checkIP) {
      InetAddress ip = Server.getRemoteIp();
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    }
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=SUCCESS");
    if (appId != null) {
      expLog.append("\tAPPID=app_1");
    }
    if (containerId != null) {
      expLog.append("\tCONTAINERID=container_1");
    }
    assertEquals(expLog.toString(), sLog);
  }

  /**
   * Test the AuditLog format for successful events passing nulls.
   */
  private void testSuccessLogNulls(boolean checkIP) {
    String sLog = NMAuditLogger.createSuccessLog(null, null, null,
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

  /**
   * Test the AuditLog format for successful events with the various
   * parameters.
   */
  private void testSuccessLogFormat(boolean checkIP) {
    testSuccessLogFormatHelper(checkIP, null, null);
    testSuccessLogFormatHelper(checkIP, APPID, null);
    testSuccessLogFormatHelper(checkIP, null, CONTAINERID);
    testSuccessLogFormatHelper(checkIP, APPID, CONTAINERID);
    testSuccessLogNulls(checkIP);
  }


  /**
   * Test the AuditLog format for failure events.
   */
  private void testFailureLogFormatHelper(boolean checkIP, ApplicationId appId,
      ContainerId containerId) {
    String fLog =
      NMAuditLogger.createFailureLog(USER, OPERATION, TARGET, DESC, appId,
      containerId);
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    if (checkIP) {
      InetAddress ip = Server.getRemoteIp();
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
    }
    expLog.append("OPERATION=oper\tTARGET=tgt\tRESULT=FAILURE\t");
    expLog.append("DESCRIPTION=description of an audit log");

    if (appId != null) {
      expLog.append("\tAPPID=app_1");
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
    testFailureLogFormatHelper(checkIP, null, null);
    testFailureLogFormatHelper(checkIP, APPID, null);
    testFailureLogFormatHelper(checkIP, null, CONTAINERID);
    testFailureLogFormatHelper(checkIP, APPID, CONTAINERID);
  }

  /**
   * Test {@link NMAuditLogger} without IP set.
   */
  @Test  
  public void testNMAuditLoggerWithoutIP() throws Exception {
    // test without ip
    testSuccessLogFormat(false);
    testFailureLogFormat(false);
  }

  /**
   * A special extension of {@link TestImpl} RPC server with 
   * {@link TestImpl#ping()} testing the audit logs.
   */
  private class MyTestRPCServer extends TestRpcBase.PBServerImpl {
    @Override
    public TestProtos.EmptyResponseProto ping(
        RpcController unused, TestProtos.EmptyRequestProto request)
        throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(clientId);
      Assert.assertEquals(ClientId.BYTE_LENGTH, clientId.length);
      // test with ip set
      testSuccessLogFormat(true);
      testFailureLogFormat(true);
      return TestProtos.EmptyResponseProto.newBuilder().build();
    }
  }

  /**
   * Test {@link NMAuditLogger} with IP set.
   */
  @Test  
  public void testNMAuditLoggerWithIP() throws Exception {
    Configuration conf = new Configuration();
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);

    // Create server side implementation
    MyTestRPCServer serverImpl = new MyTestRPCServer();
    BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // start the IPC server
    Server server = new RPC.Builder(conf)
        .setProtocol(TestRpcBase.TestRpcService.class)
        .setInstance(service).setBindAddress("0.0.0.0")
        .setPort(0).setNumHandlers(5).setVerbose(true).build();

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);

    // Make a client connection and test the audit log
    TestRpcService proxy = RPC.getProxy(TestRpcService.class,
                           TestProtocol.versionID, addr, conf);
    // Start the testcase
    TestProtos.EmptyRequestProto pingRequest =
        TestProtos.EmptyRequestProto.newBuilder().build();
    proxy.ping(null, pingRequest);

    server.stop();
    RPC.stopProxy(proxy);
  }
}
