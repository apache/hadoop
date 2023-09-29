/*
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

package org.apache.hadoop.registry.secure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.registry.client.impl.zk.ZKPathDumper;
import org.apache.hadoop.registry.client.impl.zk.CuratorService;
import org.apache.hadoop.registry.client.impl.zk.RegistrySecurity;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginContext;

import static org.apache.hadoop.registry.client.api.RegistryConstants.*;
import static org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions.PROP_ZK_SASL_CLIENT_CONTEXT;
import static org.apache.hadoop.registry.client.impl.zk.ZookeeperConfigOptions.PROP_ZK_SASL_CLIENT_USERNAME;

/**
 * Verify that the Mini ZK service can be started up securely
 */
public class TestSecureRegistry extends AbstractSecureRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSecureRegistry.class);

  @Before
  public void beforeTestSecureZKService() throws Throwable {
      enableKerberosDebugging();
  }

  @After
  public void afterTestSecureZKService() throws Throwable {
    disableKerberosDebugging();
    RegistrySecurity.clearZKSaslClientProperties();
  }

  @Test
  public void testCreateSecureZK() throws Throwable {
    startSecureZK();
    secureZK.stop();
  }

  @Test
  public void testInsecureClientToZK() throws Throwable {
    startSecureZK();
    userZookeeperToCreateRoot();
    RegistrySecurity.clearZKSaslClientProperties();

    CuratorService curatorService =
        startCuratorServiceInstance("insecure client", false);

    curatorService.zkList("/");
    curatorService.zkMkPath("", CreateMode.PERSISTENT, false,
        RegistrySecurity.WorldReadWriteACL);
  }

  /**
   * test that ZK can write as itself
   * @throws Throwable
   */
  @Test
  public void testZookeeperCanWrite() throws Throwable {

    System.setProperty("curator-log-events", "true");
    startSecureZK();
    CuratorService curator = null;
    LoginContext login = login(ZOOKEEPER_LOCALHOST,
        ZOOKEEPER_CLIENT_CONTEXT,
        keytab_zk);
    try {
      logLoginDetails(ZOOKEEPER, login);
      RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER,
                                                ZOOKEEPER_CLIENT_CONTEXT);
      curator = startCuratorServiceInstance("ZK", true);
      LOG.info(curator.toString());

      addToTeardown(curator);
      curator.zkMkPath("/", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
      curator.zkList("/");
      curator.zkMkPath("/zookeeper", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
    } finally {
      logout(login);
      ServiceOperations.stop(curator);
    }
  }

  @Test
  public void testSystemPropertyOverwrite() {
    System.setProperty(PROP_ZK_SASL_CLIENT_USERNAME, "");
    System.setProperty(PROP_ZK_SASL_CLIENT_CONTEXT, "");
    RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER,
        ZOOKEEPER_CLIENT_CONTEXT);
    assertEquals(ZOOKEEPER, System.getProperty(PROP_ZK_SASL_CLIENT_USERNAME));
    assertEquals(ZOOKEEPER_CLIENT_CONTEXT,
        System.getProperty(PROP_ZK_SASL_CLIENT_CONTEXT));

    String userName = "user1";
    String context = "context1";
    System.setProperty(PROP_ZK_SASL_CLIENT_USERNAME, userName);
    System.setProperty(PROP_ZK_SASL_CLIENT_CONTEXT, context);
    RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER,
        ZOOKEEPER_CLIENT_CONTEXT);
    assertEquals(userName, System.getProperty(PROP_ZK_SASL_CLIENT_USERNAME));
    assertEquals(context, System.getProperty(PROP_ZK_SASL_CLIENT_CONTEXT));
  }

  /**
   * Start a curator service instance
   * @param name name
   * @param secure flag to indicate the cluster is secure
   * @return an inited and started curator service
   */
  protected CuratorService startCuratorServiceInstance(String name,
      boolean secure) {
    Configuration clientConf = new Configuration();
    clientConf.set(KEY_REGISTRY_ZK_ROOT, "/");
    clientConf.setBoolean(KEY_REGISTRY_SECURE, secure);
    describe(LOG, "Starting Curator service");
    CuratorService curatorService = new CuratorService(name, secureZK);
    curatorService.init(clientConf);
    curatorService.start();
    LOG.info("Curator Binding {}",
        curatorService.bindingDiagnosticDetails());
    return curatorService;
  }

  /**
   * have the ZK user create the root dir.
   * This logs out the ZK user after and stops its curator instance,
   * to avoid contamination
   * @throws Throwable
   */
  public void userZookeeperToCreateRoot() throws Throwable {

    System.setProperty("curator-log-events", "true");
    CuratorService curator = null;
    LoginContext login = login(ZOOKEEPER_LOCALHOST,
        ZOOKEEPER_CLIENT_CONTEXT,
        keytab_zk);
    try {
      logLoginDetails(ZOOKEEPER, login);
      RegistrySecurity.setZKSaslClientProperties(ZOOKEEPER,
          ZOOKEEPER_CLIENT_CONTEXT);
      curator = startCuratorServiceInstance("ZK", true);
      LOG.info(curator.toString());

      addToTeardown(curator);
      curator.zkMkPath("/", CreateMode.PERSISTENT, false,
          RegistrySecurity.WorldReadWriteACL);
      ZKPathDumper pathDumper = curator.dumpPath(true);
      LOG.info(pathDumper.toString());
    } finally {
      logout(login);
      ServiceOperations.stop(curator);
    }
  }
}
