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

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRPC.TestImpl;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.TestRpcBase.TestRpcService;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.Keys;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests {@link RMAuditLogger}.
 */
public class TestRMAuditLogger {
  private static final String USER = "test";
  private static final String OPERATION = "oper";
  private static final String TARGET = "tgt";
  private static final String PERM = "admin group";
  private static final String DESC = "description of an audit log";
  private static final String QUEUE = "root";
  private static final ApplicationId APPID = mock(ApplicationId.class);
  private static final ApplicationAttemptId ATTEMPTID = mock(ApplicationAttemptId.class);
  private static final ContainerId CONTAINERID = mock(ContainerId.class);
  private static final Resource RESOURCE = mock(Resource.class);
  private static final String CALLER_CONTEXT = "context";
  private static final byte[] CALLER_SIGNATURE = "signature".getBytes();
  private static final String PARTITION = "label1";

  @Before
  public void setUp() throws Exception {
    when(APPID.toString()).thenReturn("app_1");
    when(ATTEMPTID.toString()).thenReturn("app_attempt_1");
    when(CONTAINERID.toString()).thenReturn("container_1");
    when(RESOURCE.toString()).thenReturn("<memory:1536, vcores:1>");
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

  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId, null,
        null);
  }

  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId,
      CallerContext callerContext, Resource resource) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId,
        callerContext, resource, Server.getRemoteIp());
  }

  /**
   * Test the AuditLog format for successful events.
   */
  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId,
      CallerContext callerContext, Resource resource, InetAddress remoteIp) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId,
        callerContext, resource, remoteIp, null);
  }

  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
        ApplicationAttemptId attemptId, ContainerId containerId,
        CallerContext callerContext, Resource resource, InetAddress remoteIp,
        RMAuditLogger.ArgsBuilder args) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId,
        callerContext, resource, remoteIp, args, null, null);
  }

  private void testSuccessLogFormatHelper(boolean checkIP, ApplicationId appId,
        ApplicationAttemptId attemptId, ContainerId containerId,
        CallerContext callerContext, Resource resource, InetAddress remoteIp,
        RMAuditLogger.ArgsBuilder args, String queueName, String partition) {
    String sLog;
    InetAddress tmpIp = checkIP ? remoteIp : null;
    if (args != null) {
      sLog = RMAuditLogger.createSuccessLog(USER, OPERATION, TARGET,
          tmpIp, args);
    } else {
      sLog = RMAuditLogger.createSuccessLog(USER, OPERATION, TARGET, appId,
          attemptId, containerId, resource, callerContext, tmpIp, queueName,
          partition);
    }
    StringBuilder expLog = new StringBuilder();
    expLog.append("USER=test\t");
    if (checkIP) {
      InetAddress ip;
      if(remoteIp != null) {
        ip = remoteIp;
      } else {
        ip = Server.getRemoteIp();
      }
      expLog.append(Keys.IP.name() + "=" + ip.getHostAddress() + "\t");
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
    if (resource != null) {
      expLog.append("\tRESOURCE=<memory:1536, vcores:1>");
    }
    if (callerContext != null) {
      if (callerContext.getContext() != null) {
        expLog.append("\tCALLERCONTEXT=context");
      }
      if (callerContext.getSignature() != null) {
        expLog.append("\tCALLERSIGNATURE=signature");
      }
    }
    if (args != null) {
      expLog.append("\tQUEUENAME=root");
      expLog.append("\tRECURSIVE=true");
    } else {
      if (queueName != null) {
        expLog.append("\tQUEUENAME=" + QUEUE);
      }
    }
    if (partition != null) {
      expLog.append("\tNODELABEL=" + PARTITION);
    }
    assertEquals(expLog.toString(), sLog);
  }

  private void testSuccessLogFormatHelperWithIP(boolean checkIP,
      ApplicationId appId, ApplicationAttemptId attemptId,
      ContainerId containerId, InetAddress ip) {
    testSuccessLogFormatHelper(checkIP, appId, attemptId, containerId, null,
        null, ip);
  }

  /**
   * Test the AuditLog format for successful events passing nulls.
   */
  private void testSuccessLogNulls(boolean checkIP) {
    String sLog = RMAuditLogger.createSuccessLog(null, null, null, null, 
        null, null, null);
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
    testSuccessLogFormatHelperWithIP(checkIP, appId, attemptId, containerId,
        InetAddress.getLoopbackAddress());
    byte[] ipAddr = new byte[] {100, 10, 10, 1};

    InetAddress addr = null;
    try {
      addr = InetAddress.getByAddress(ipAddr);
    } catch (UnknownHostException uhe) {
      // should not happen as long as IP address format
      // stays the same
      Assert.fail("Check ip address being constructed");
    }
    testSuccessLogFormatHelperWithIP(checkIP, appId, attemptId, containerId,
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
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID, null, null);
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(null).setSignature(null).build(), RESOURCE);
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(CALLER_CONTEXT).setSignature(null).build(), RESOURCE);
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(null).setSignature(CALLER_SIGNATURE).build(), RESOURCE);
    testSuccessLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(CALLER_CONTEXT).setSignature(CALLER_SIGNATURE)
            .build(), RESOURCE);
    RMAuditLogger.ArgsBuilder args = new RMAuditLogger.ArgsBuilder()
        .append(Keys.QUEUENAME, QUEUE).append(Keys.RECURSIVE, "true");
    testSuccessLogFormatHelper(checkIP, null, null, null, null, null,
        Server.getRemoteIp(), args);
    testSuccessLogFormatHelper(checkIP, null, null, null, null, null,
        Server.getRemoteIp(), null, QUEUE, PARTITION);
    testSuccessLogFormatHelperWithIP(checkIP, APPID, ATTEMPTID, CONTAINERID);
    testSuccessLogNulls(checkIP);
  }

  private void testFailureLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId) {
    testFailureLogFormatHelper(checkIP, appId, attemptId, containerId, null, null);
  }
 
  /**
   * Test the AuditLog format for failure events.
   */
  private void testFailureLogFormatHelper(boolean checkIP, ApplicationId appId,
      ApplicationAttemptId attemptId, ContainerId containerId,
      CallerContext callerContext, Resource resource) {
    testFailureLogFormatHelper(checkIP, appId, attemptId, containerId,
        callerContext, resource, null, null, null);
  }

  private void testFailureLogFormatHelper(boolean checkIP, ApplicationId appId,
        ApplicationAttemptId attemptId, ContainerId containerId,
        CallerContext callerContext, Resource resource,
        String queueName, String partition, RMAuditLogger.ArgsBuilder args) {
    String fLog = args == null ?
      RMAuditLogger.createFailureLog(USER, OPERATION, PERM, TARGET, DESC,
          appId, attemptId, containerId, resource, callerContext,
          queueName, partition) :
        RMAuditLogger.createFailureLog(USER, OPERATION, PERM, TARGET, DESC,
            args);
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
    if (resource != null) {
      expLog.append("\tRESOURCE=<memory:1536, vcores:1>");
    }
    if (callerContext != null) {
      if (callerContext.getContext() != null) {
        expLog.append("\tCALLERCONTEXT=context");
      }
      if (callerContext.getSignature() != null) {
        expLog.append("\tCALLERSIGNATURE=signature");
      }
    }
    if (queueName != null) {
      expLog.append("\tQUEUENAME=" + QUEUE);
    }
    if (partition != null) {
      expLog.append("\tNODELABEL=" + PARTITION);
    }
    if (args != null) {
      expLog.append("\tQUEUENAME=root");
      expLog.append("\tRECURSIVE=true");
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
    
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(null).setSignature(null).build(), RESOURCE);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(CALLER_CONTEXT).setSignature(null).build(), RESOURCE);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(null).setSignature(CALLER_SIGNATURE).build(), RESOURCE);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(CALLER_CONTEXT).setSignature(CALLER_SIGNATURE)
            .build(), RESOURCE);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(CALLER_CONTEXT).setSignature(CALLER_SIGNATURE)
            .build(), RESOURCE, QUEUE, null, null);
    testFailureLogFormatHelper(checkIP, APPID, ATTEMPTID, CONTAINERID,
        new CallerContext.Builder(CALLER_CONTEXT).setSignature(CALLER_SIGNATURE)
            .build(), RESOURCE, QUEUE, PARTITION, null);
    RMAuditLogger.ArgsBuilder args = new RMAuditLogger.ArgsBuilder()
        .append(Keys.QUEUENAME, QUEUE).append(Keys.RECURSIVE, "true");
    testFailureLogFormatHelper(checkIP, null, null, null, null, null,
        null, null, args);
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
   * Test {@link RMAuditLogger} with IP set.
   */
  @Test  
  public void testRMAuditLoggerWithIP() throws Exception {
    Configuration conf = new Configuration();
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine2.class);

    // Create server side implementation
    MyTestRPCServer serverImpl = new MyTestRPCServer();
    BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // start the IPC server
    Server server = new RPC.Builder(conf)
        .setProtocol(TestRpcService.class)
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
