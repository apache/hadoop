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

package org.apache.hadoop.mapreduce.security;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.hs.JHSDelegationTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

public class TestJHSSecurity {

  private static final Log LOG = LogFactory.getLog(TestJHSSecurity.class);
  
  @Test
  public void testDelegationToken() throws IOException, InterruptedException {

    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);

    final YarnConfiguration conf = new YarnConfiguration(new JobConf());
    // Just a random principle
    conf.set(JHAdminConfig.MR_HISTORY_PRINCIPAL,
      "RandomOrc/localhost@apache.org");

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);
    
    final long initialInterval = 10000l;
    final long maxLifetime= 20000l;
    final long renewInterval = 10000l;

    JobHistoryServer jobHistoryServer = null;
    MRClientProtocol clientUsingDT = null;
    long tokenFetchTime;
    try {
      jobHistoryServer = new JobHistoryServer() {
        protected void doSecureLogin(Configuration conf) throws IOException {
          // no keytab based login
        };

        protected JHSDelegationTokenSecretManager createJHSSecretManager(
            Configuration conf) {
          return new JHSDelegationTokenSecretManager(initialInterval, 
              maxLifetime, renewInterval, 3600000);
        }
      };
//      final JobHistoryServer jobHistoryServer = jhServer;
      jobHistoryServer.init(conf);
      jobHistoryServer.start();
      final MRClientProtocol hsService = jobHistoryServer.getClientService()
          .getClientHandler();

      // Fake the authentication-method
      UserGroupInformation loggedInUser = UserGroupInformation
          .createRemoteUser("testrenewer@APACHE.ORG");
      Assert.assertEquals("testrenewer", loggedInUser.getShortUserName());
   // Default realm is APACHE.ORG
      loggedInUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);


      DelegationToken token = getDelegationToken(loggedInUser, hsService,
          loggedInUser.getShortUserName());
      tokenFetchTime = System.currentTimeMillis();
      LOG.info("Got delegation token at: " + tokenFetchTime);

      // Now try talking to JHS using the delegation token
      clientUsingDT = getMRClientProtocol(token, jobHistoryServer
          .getClientService().getBindAddress(), "TheDarkLord", conf);

      GetJobReportRequest jobReportRequest =
          Records.newRecord(GetJobReportRequest.class);
      jobReportRequest.setJobId(MRBuilderUtils.newJobId(123456, 1, 1));
      try {
        clientUsingDT.getJobReport(jobReportRequest);
      } catch (YarnRemoteException e) {
        Assert.assertEquals("Unknown job job_123456_0001", e.getMessage());
      }
      
   // Renew after 50% of token age.
      while(System.currentTimeMillis() < tokenFetchTime + initialInterval / 2) {
        Thread.sleep(500l);
      }
      long nextExpTime = renewDelegationToken(loggedInUser, hsService, token);
      long renewalTime = System.currentTimeMillis();
      LOG.info("Renewed token at: " + renewalTime + ", NextExpiryTime: "
          + nextExpTime);
      
      // Wait for first expiry, but before renewed expiry.
      while (System.currentTimeMillis() > tokenFetchTime + initialInterval
          && System.currentTimeMillis() < nextExpTime) {
        Thread.sleep(500l);
      }
      Thread.sleep(50l);
      
      // Valid token because of renewal.
      try {
        clientUsingDT.getJobReport(jobReportRequest);
      } catch (UndeclaredThrowableException e) {
        Assert.assertEquals("Unknown job job_123456_0001", e.getMessage());
      }
      
      // Wait for expiry.
      while(System.currentTimeMillis() < renewalTime + renewInterval) {
        Thread.sleep(500l);
      }
      Thread.sleep(50l);
      LOG.info("At time: " + System.currentTimeMillis() + ", token should be invalid");
      // Token should have expired.      
      try {
        clientUsingDT.getJobReport(jobReportRequest);
        fail("Should not have succeeded with an expired token");
      } catch (UndeclaredThrowableException e) {
        assertTrue(e.getCause().getMessage().contains("is expired"));
      }
      
      // Test cancellation
      // Stop the existing proxy, start another.
      if (clientUsingDT != null) {
//        RPC.stopProxy(clientUsingDT);
        clientUsingDT = null;
      }
      token = getDelegationToken(loggedInUser, hsService,
          loggedInUser.getShortUserName());
      tokenFetchTime = System.currentTimeMillis();
      LOG.info("Got delegation token at: " + tokenFetchTime);
 
      // Now try talking to HSService using the delegation token
      clientUsingDT = getMRClientProtocol(token, jobHistoryServer
          .getClientService().getBindAddress(), "loginuser2", conf);

      
      try {
        clientUsingDT.getJobReport(jobReportRequest);
      } catch (UndeclaredThrowableException e) {
        fail("Unexpected exception" + e);
      }
      cancelDelegationToken(loggedInUser, hsService, token);
      if (clientUsingDT != null) {
//        RPC.stopProxy(clientUsingDT);
        clientUsingDT = null;
      } 
      
      // Creating a new connection.
      clientUsingDT = getMRClientProtocol(token, jobHistoryServer
          .getClientService().getBindAddress(), "loginuser2", conf);
      LOG.info("Cancelled delegation token at: " + System.currentTimeMillis());
      // Verify cancellation worked.
      try {
        clientUsingDT.getJobReport(jobReportRequest);
        fail("Should not have succeeded with a cancelled delegation token");
      } catch (UndeclaredThrowableException e) {
      }


      
    } finally {
      jobHistoryServer.stop();
    }
  }

  private DelegationToken getDelegationToken(
      final UserGroupInformation loggedInUser,
      final MRClientProtocol hsService, final String renewerString)
      throws IOException, InterruptedException {
    // Get the delegation token directly as it is a little difficult to setup
    // the kerberos based rpc.
    DelegationToken token = loggedInUser
        .doAs(new PrivilegedExceptionAction<DelegationToken>() {
          @Override
          public DelegationToken run() throws YarnRemoteException {
            GetDelegationTokenRequest request = Records
                .newRecord(GetDelegationTokenRequest.class);
            request.setRenewer(renewerString);
            return hsService.getDelegationToken(request).getDelegationToken();
          }

        });
    return token;
  }

  private long renewDelegationToken(final UserGroupInformation loggedInUser,
      final MRClientProtocol hsService, final DelegationToken dToken)
      throws IOException, InterruptedException {
    long nextExpTime = loggedInUser.doAs(new PrivilegedExceptionAction<Long>() {

      @Override
      public Long run() throws YarnRemoteException {
        RenewDelegationTokenRequest request = Records
            .newRecord(RenewDelegationTokenRequest.class);
        request.setDelegationToken(dToken);
        return hsService.renewDelegationToken(request).getNextExpirationTime();
      }
    });
    return nextExpTime;
  }

  private void cancelDelegationToken(final UserGroupInformation loggedInUser,
      final MRClientProtocol hsService, final DelegationToken dToken)
      throws IOException, InterruptedException {

    loggedInUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws YarnRemoteException {
        CancelDelegationTokenRequest request = Records
            .newRecord(CancelDelegationTokenRequest.class);
        request.setDelegationToken(dToken);
        hsService.cancelDelegationToken(request);
        return null;
      }
    });
  }

  private MRClientProtocol getMRClientProtocol(DelegationToken token,
      final InetSocketAddress hsAddress, String user, final Configuration conf) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.addToken(ProtoUtils.convertFromProtoFormat(token, hsAddress));

    final YarnRPC rpc = YarnRPC.create(conf);
    MRClientProtocol hsWithDT = ugi
        .doAs(new PrivilegedAction<MRClientProtocol>() {

          @Override
          public MRClientProtocol run() {
            return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
                hsAddress, conf);
          }
        });
    return hsWithDT;
  }

}
