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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
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

    final JobHistoryServer jobHistoryServer = new JobHistoryServer() {
      protected void doSecureLogin(Configuration conf) throws IOException {
        // no keytab based login
      };
    };
    jobHistoryServer.init(conf);
    jobHistoryServer.start();

    // Fake the authentication-method
    UserGroupInformation loggedInUser = UserGroupInformation.getCurrentUser();
    loggedInUser.setAuthenticationMethod(AuthenticationMethod.KERBEROS);

    // Get the delegation token directly as it is a little difficult to setup
    // the kerberos based rpc.
    DelegationToken token =
        loggedInUser.doAs(new PrivilegedExceptionAction<DelegationToken>() {
          @Override
          public DelegationToken run() throws YarnRemoteException {
            GetDelegationTokenRequest request =
                Records.newRecord(GetDelegationTokenRequest.class);
            request.setRenewer("OneRenewerToRuleThemAll");
            return jobHistoryServer.getClientService().getClientHandler()
              .getDelegationToken(request).getDelegationToken();
          }
        });

    // Now try talking to JHS using the delegation token
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("TheDarkLord");
    ugi.addToken(ProtoUtils.convertFromProtoFormat(
        token, jobHistoryServer.getClientService().getBindAddress()));
    final YarnRPC rpc = YarnRPC.create(conf);
    MRClientProtocol userUsingDT =
        ugi.doAs(new PrivilegedAction<MRClientProtocol>() {
          @Override
          public MRClientProtocol run() {
            return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
              jobHistoryServer.getClientService().getBindAddress(), conf);
          }
        });
    GetJobReportRequest jobReportRequest =
        Records.newRecord(GetJobReportRequest.class);
    jobReportRequest.setJobId(MRBuilderUtils.newJobId(123456, 1, 1));
    try {
      userUsingDT.getJobReport(jobReportRequest);
    } catch (YarnRemoteException e) {
      Assert.assertEquals("Unknown job job_123456_0001", e.getMessage());
    }
  }

}
