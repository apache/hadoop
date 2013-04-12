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

package org.apache.hadoop.yarn.server;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.RegistrationResponse;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Test;

public class TestRMNMSecretKeys {

  @Test
  public void testNMUpdation() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      "kerberos");
    UserGroupInformation.setConfiguration(conf);
    // Default rolling and activation intervals are large enough, no need to
    // intervene

    final DrainDispatcher dispatcher = new DrainDispatcher();
    ResourceManager rm = new ResourceManager() {
      @Override
      protected void doSecureLogin() throws IOException {
        // Do nothing.
      }

      @Override
      protected Dispatcher createDispatcher() {
        return dispatcher;
      }
    };
    rm.init(conf);
    rm.start();

    MockNM nm = new MockNM("host:1234", 3072, rm.getResourceTrackerService());
    RegistrationResponse registrationResponse = nm.registerNode();
    MasterKey masterKey = registrationResponse.getMasterKey();
    Assert.assertNotNull("Registration should cause a key-update!", masterKey);
    dispatcher.await();

    HeartbeatResponse response = nm.nodeHeartbeat(true);
    Assert.assertNull(
      "First heartbeat after registration shouldn't get any key updates!",
      response.getMasterKey());
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    Assert
      .assertNull(
        "Even second heartbeat after registration shouldn't get any key updates!",
        response.getMasterKey());
    dispatcher.await();

    // Let's force a roll-over
    RMContainerTokenSecretManager secretManager =
        rm.getRMContainerTokenSecretManager();
    secretManager.rollMasterKey();

    // Heartbeats after roll-over and before activation should be fine.
    response = nm.nodeHeartbeat(true);
    Assert.assertNotNull(
      "Heartbeats after roll-over and before activation should not err out.",
      response.getMasterKey());
    Assert.assertEquals(
      "Roll-over should have incremented the key-id only by one!",
      masterKey.getKeyId() + 1, response.getMasterKey().getKeyId());
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull(
      "Second heartbeat after roll-over shouldn't get any key updates!",
      response.getMasterKey());
    dispatcher.await();

    // Let's force activation
    secretManager.activateNextMasterKey();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull("Activation shouldn't cause any key updates!",
      response.getMasterKey());
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull(
      "Even second heartbeat after activation shouldn't get any key updates!",
      response.getMasterKey());
    dispatcher.await();

    rm.stop();
  }
}
