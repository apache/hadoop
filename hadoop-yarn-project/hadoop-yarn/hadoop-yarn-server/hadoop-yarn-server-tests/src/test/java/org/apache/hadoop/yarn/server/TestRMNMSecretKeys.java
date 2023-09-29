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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestRMNMSecretKeys {
  private static final String KRB5_CONF = "java.security.krb5.conf";
  private static final File KRB5_CONF_ROOT_DIR = new File(
      System.getProperty("test.build.dir", "target/test-dir"),
          UUID.randomUUID().toString());

  @BeforeAll
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

  @AfterAll
  public static void tearDown() throws IOException {
    KRB5_CONF_ROOT_DIR.delete();
  }

  @Test
  @Timeout(1000000)
  void testNMUpdation() throws Exception {
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
    assertNotNull(containerTokenMasterKey, containerToken
        + "Registration should cause a key-update!");
    MasterKey nmTokenMasterKey = registrationResponse.getNMTokenMasterKey();
    assertNotNull(nmTokenMasterKey, nmToken
        + "Registration should cause a key-update!");
    
    dispatcher.await();

    NodeHeartbeatResponse response = nm.nodeHeartbeat(true);
    assertNull(response.getContainerTokenMasterKey(),
        containerToken +
        "First heartbeat after registration shouldn't get any key updates!");
    assertNull(response.getNMTokenMasterKey(),
        nmToken +
        "First heartbeat after registration shouldn't get any key updates!");
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    assertNull(response.getContainerTokenMasterKey(),
        containerToken +
        "Even second heartbeat after registration shouldn't get any key updates!");
    assertNull(response.getContainerTokenMasterKey(),
        nmToken +
        "Even second heartbeat after registration shouldn't get any key updates!");
    
    dispatcher.await();

    // Let's force a roll-over
    rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    rm.getRMContext().getNMTokenSecretManager().rollMasterKey();

    // Heartbeats after roll-over and before activation should be fine.
    response = nm.nodeHeartbeat(true);
    assertNotNull(response.getContainerTokenMasterKey(),
        containerToken +
        "Heartbeats after roll-over and before activation should not err out.");
    assertNotNull(response.getNMTokenMasterKey(),
        nmToken +
        "Heartbeats after roll-over and before activation should not err out.");
    
    assertEquals(containerTokenMasterKey.getKeyId() + 1,
        response.getContainerTokenMasterKey().getKeyId(),
        containerToken +
        "Roll-over should have incremented the key-id only by one!");
    assertEquals(nmTokenMasterKey.getKeyId() + 1,
        response.getNMTokenMasterKey().getKeyId(),
        nmToken +
        "Roll-over should have incremented the key-id only by one!");
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    assertNull(response.getContainerTokenMasterKey(),
        containerToken +
        "Second heartbeat after roll-over shouldn't get any key updates!");
    assertNull(response.getNMTokenMasterKey(),
        nmToken +
        "Second heartbeat after roll-over shouldn't get any key updates!");
    dispatcher.await();

    // Let's force activation
    rm.getRMContext().getContainerTokenSecretManager().activateNextMasterKey();
    rm.getRMContext().getNMTokenSecretManager().activateNextMasterKey();

    response = nm.nodeHeartbeat(true);
    assertNull(response.getContainerTokenMasterKey(),
        containerToken
        + "Activation shouldn't cause any key updates!");
    assertNull(response.getNMTokenMasterKey(),
        nmToken
        + "Activation shouldn't cause any key updates!");
    dispatcher.await();

    response = nm.nodeHeartbeat(true);
    assertNull(response.getContainerTokenMasterKey(),
        containerToken +
        "Even second heartbeat after activation shouldn't get any key updates!");
    assertNull(response.getNMTokenMasterKey(),
        nmToken +
        "Even second heartbeat after activation shouldn't get any key updates!");
    dispatcher.await();

    rm.stop();
  }
}
