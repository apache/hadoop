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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.junit.Before;

public abstract class ACLsTestBase {

  protected static final String COMMON_USER = "common_user";
  protected static final String QUEUE_A_USER = "queueA_user";
  protected static final String QUEUE_B_USER = "queueB_user";
  protected static final String QUEUE_A_GROUP = "queueA_group";
  protected static final String QUEUE_B_GROUP = "queueB_group";
  protected static final String ROOT_ADMIN = "root_admin";
  protected static final String QUEUE_A_ADMIN = "queueA_admin";
  protected static final String QUEUE_B_ADMIN = "queueB_admin";

  protected static final String QUEUEA = "queueA";
  protected static final String QUEUEB = "queueB";
  protected static final String QUEUEC = "queueC";

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestApplicationACLs.class);

  protected MockRM resourceManager;
  Configuration conf;
  YarnRPC rpc;
  InetSocketAddress rmAddress;

  @Before
  public void setup() throws InterruptedException, IOException {
    conf = createConfiguration();
    rpc = YarnRPC.create(conf);
    rmAddress = conf.getSocketAddr(
      YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_PORT);

    AccessControlList adminACL = new AccessControlList("");
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACL.getAclString());
    conf.setInt(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, 10);

    resourceManager = new MockRM(conf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(getRMContext(), this.scheduler,
          this.rmAppManager, this.applicationACLsManager,
          this.queueACLsManager, getRMContext()
                .getRMDelegationTokenSecretManager());
      }

      @Override
      protected void doSecureLogin() throws IOException {
      }
    };
    new Thread() {
      public void run() {
        resourceManager.start();
      };
    }.start();
    int waitCount = 0;
    while (resourceManager.getServiceState() == STATE.INITED
        && waitCount++ < 60) {
      LOG.info("Waiting for RM to start...");
      Thread.sleep(1500);
    }
    if (resourceManager.getServiceState() != STATE.STARTED) {
      // RM could have failed.
      throw new IOException("ResourceManager failed to start. Final state is "
          + resourceManager.getServiceState());
    }
  }

  protected ApplicationClientProtocol getRMClientForUser(String user)
      throws IOException, InterruptedException {
    UserGroupInformation userUGI = UserGroupInformation.createRemoteUser(user);
    ApplicationClientProtocol userClient =
        userUGI
          .doAs(new PrivilegedExceptionAction<ApplicationClientProtocol>() {
            @Override
            public ApplicationClientProtocol run() throws Exception {
              return (ApplicationClientProtocol) rpc.getProxy(
                ApplicationClientProtocol.class, rmAddress, conf);
            }
          });
    return userClient;
  }

  protected abstract Configuration createConfiguration() throws IOException;
}
