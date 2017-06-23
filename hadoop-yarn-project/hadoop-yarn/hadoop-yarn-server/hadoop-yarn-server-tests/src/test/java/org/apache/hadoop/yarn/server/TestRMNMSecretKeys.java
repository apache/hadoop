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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.kerby.util.IOUtil;
import org.junit.Test;

public class TestRMNMSecretKeys {
  private static final String KRB5_CONF = "java.security.krb5.conf";
  private static final File KRB5_CONF_ROOT_DIR = new File(
      System.getProperty("test.build.dir", "target/test-dir"),
          UUID.randomUUID().toString());

  @BeforeClass
  public static void setup() throws IOException {
    KRB5_CONF_ROOT_DIR.mkdir();
    File krb5ConfFile = new File(KRB5_CONF_ROOT_DIR, "krb5.conf");
    krb5ConfFile.createNewFile();
    String content = "[libdefaults]\n" +
        "    default_realm = APACHE.ORG\n" +
        "    udp_preference_limit = 1\n"+
        "    extra_addresses = 127.0.0.1\n" +
        "[realms]\n" +
        "    APACHE.ORG = {\n" +
        "        admin_server = localhost:88\n" +
        "        kdc = localhost:88\n}\n" +
        "[domain_realm]\n" +
        "    localhost = APACHE.ORG";
    IOUtil.writeFile(content, krb5ConfFile);
    System.setProperty(KRB5_CONF, krb5ConfFile.getAbsolutePath());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    KRB5_CONF_ROOT_DIR.delete();
  }

  @Test(timeout = 1000000)
  public void testNMUpdation() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    // validating RM NM keys for Unsecured environment
    validateRMNMKeyExchange(conf);
    
    // validating RM NM keys for secured environment
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    validateRMNMKeyExchange(conf);
  }

  private void validateRMNMKeyExchange(YarnConfiguration conf) throws Exception {
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
      @Override
      protected void startWepApp() {
        // Don't need it, skip.
      }
    };
    rm.init(conf);
    rm.start();

    // Testing ContainerToken and NMToken
    String containerToken = "Container Token : ";
    String nmToken = "NM Token : ";
    
    MockNM nm = new MockNM("host:1234", 3072, rm.getResourceTrackerService());
    RegisterNodeManagerResponse registrationResponse = nm.registerNode();
    
    MasterKey containerTokenMasterKey =
        registrationResponse.getContainerTokenMasterKey();
    Assert.assertNotNull(containerToken
        + "Registration should cause a key-update!", containerTokenMasterKey);
    MasterKey nmTokenMasterKey = registrationResponse.getNMTokenMasterKey();
    Assert.assertNotNull(nmToken
        + "Registration should cause a key-update!", nmTokenMasterKey);
    
    dispatcher.await();

    NodeHeartbeatResponse response = nm.nodeHeartbeat(true);
    Assert.assertNull(containerToken +
        "First heartbeat after registration shouldn't get any key updates!",
        response.getContainerTokenMasterKey());
    Assert.assertNull(nmToken +
        "First heartbeat after registration shouldn't get any key updates!",
        response.getNMTokenMasterKey());
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull(containerToken +
        "Even second heartbeat after registration shouldn't get any key updates!",
        response.getContainerTokenMasterKey());
    Assert.assertNull(nmToken +
        "Even second heartbeat after registration shouldn't get any key updates!",
        response.getContainerTokenMasterKey());
    
    dispatcher.await();

    // Let's force a roll-over
    rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    rm.getRMContext().getNMTokenSecretManager().rollMasterKey();

    // Heartbeats after roll-over and before activation should be fine.
    response = nm.nodeHeartbeat(true);
    Assert.assertNotNull(containerToken +
        "Heartbeats after roll-over and before activation should not err out.",
        response.getContainerTokenMasterKey());
    Assert.assertNotNull(nmToken +
        "Heartbeats after roll-over and before activation should not err out.",
        response.getNMTokenMasterKey());
    
    Assert.assertEquals(containerToken +
        "Roll-over should have incremented the key-id only by one!",
        containerTokenMasterKey.getKeyId() + 1,
        response.getContainerTokenMasterKey().getKeyId());
    Assert.assertEquals(nmToken +
        "Roll-over should have incremented the key-id only by one!",
        nmTokenMasterKey.getKeyId() + 1,
        response.getNMTokenMasterKey().getKeyId());
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull(containerToken +
        "Second heartbeat after roll-over shouldn't get any key updates!",
        response.getContainerTokenMasterKey());
    Assert.assertNull(nmToken +
        "Second heartbeat after roll-over shouldn't get any key updates!",
        response.getNMTokenMasterKey());
    dispatcher.await();

    // Let's force activation
    rm.getRMContext().getContainerTokenSecretManager().activateNextMasterKey();
    rm.getRMContext().getNMTokenSecretManager().activateNextMasterKey();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull(containerToken
        + "Activation shouldn't cause any key updates!",
        response.getContainerTokenMasterKey());
    Assert.assertNull(nmToken
        + "Activation shouldn't cause any key updates!",
        response.getNMTokenMasterKey());
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    Assert.assertNull(containerToken +
        "Even second heartbeat after activation shouldn't get any key updates!",
        response.getContainerTokenMasterKey());
    Assert.assertNull(nmToken +
        "Even second heartbeat after activation shouldn't get any key updates!",
        response.getNMTokenMasterKey());
    dispatcher.await();

    rm.stop();
  }
}
