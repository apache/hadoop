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

package org.apache.hadoop.yarn.client;

import java.io.IOException;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.junit.After;

/**
 * Test Base for Application Master Service Protocol.
 */
public abstract class ApplicationMasterServiceProtoTestBase
    extends ProtocolHATestBase {

  private ApplicationMasterProtocol amClient;
  private ApplicationAttemptId attemptId;

  protected void startupHAAndSetupClient() throws Exception {
    attemptId = this.cluster.createFakeApplicationAttemptId();

    Token<AMRMTokenIdentifier> appToken =
        this.cluster.getResourceManager().getRMContext()
          .getAMRMTokenSecretManager().createAndGetAMRMToken(attemptId);
    appToken.setService(ClientRMProxy.getAMRMTokenService(this.conf));
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appToken);
    syncToken(appToken);
    amClient = ClientRMProxy
        .createRMProxy(this.conf, ApplicationMasterProtocol.class);
  }

  @After
  public void shutDown() {
    if(this.amClient != null) {
      RPC.stopProxy(this.amClient);
    }
  }

  protected ApplicationMasterProtocol getAMClient() {
    return amClient;
  }

  private void syncToken(Token<AMRMTokenIdentifier> token) throws IOException {
    for (int i = 0; i < this.cluster.getNumOfResourceManager(); i++) {
      this.cluster.getResourceManager(i).getRMContext()
          .getAMRMTokenSecretManager().addPersistedPassword(token);
    }
  }
}
