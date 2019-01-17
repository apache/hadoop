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

package org.apache.hadoop.registry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.conf.RegistryConfiguration;
import org.apache.hadoop.registry.server.services.AddingCompositeService;
import org.apache.hadoop.registry.server.services.MicroZookeeperService;
import org.apache.hadoop.registry.server.services.MicroZookeeperServiceKeys;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AbstractZKRegistryTest extends RegistryTestHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractZKRegistryTest.class);

  private static final AddingCompositeService servicesToTeardown =
      new AddingCompositeService("teardown");
  // static initializer guarantees it is always started
  // ahead of any @BeforeClass methods
  static {
    servicesToTeardown.init(new Configuration());
    servicesToTeardown.start();
  }

  @Rule
  public final Timeout testTimeout = new Timeout(10000);

  @Rule
  public TestName methodName = new TestName();

  protected static void addToTeardown(Service svc) {
    servicesToTeardown.addService(svc);
  }

  @AfterClass
  public static void teardownServices() throws IOException {
    describe(LOG, "teardown of static services");
    servicesToTeardown.close();
  }

  protected static MicroZookeeperService zookeeper;


  @BeforeClass
  public static void createZKServer() throws Exception {
    File zkDir = new File("target/zookeeper");
    FileUtils.deleteDirectory(zkDir);
    assertTrue(zkDir.mkdirs());
    zookeeper = new MicroZookeeperService("InMemoryZKService");
    Configuration conf = new RegistryConfiguration();
    conf.set(MicroZookeeperServiceKeys.KEY_ZKSERVICE_DIR, zkDir.getAbsolutePath());
    zookeeper.init(conf);
    zookeeper.start();
    addToTeardown(zookeeper);
  }

  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  /**
   * Returns the connection string to use
   *
   * @return connection string
   */
  public String getConnectString() {
    return zookeeper.getConnectionString();
  }

  public Configuration createRegistryConfiguration() {
    Configuration conf = new RegistryConfiguration();
    conf.setInt(RegistryConstants.KEY_REGISTRY_ZK_CONNECTION_TIMEOUT, 1000);
    conf.setInt(RegistryConstants.KEY_REGISTRY_ZK_RETRY_INTERVAL, 500);
    conf.setInt(RegistryConstants.KEY_REGISTRY_ZK_RETRY_TIMES, 10);
    conf.setInt(RegistryConstants.KEY_REGISTRY_ZK_RETRY_CEILING, 10);
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_QUORUM,
        zookeeper.getConnectionString());
    return conf;
  }
}
