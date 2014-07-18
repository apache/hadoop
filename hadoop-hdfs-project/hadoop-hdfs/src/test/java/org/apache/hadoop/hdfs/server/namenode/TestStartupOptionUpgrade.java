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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This class tests various upgrade cases from earlier versions to current
 * version with and without clusterid.
 */
@RunWith(value = Parameterized.class)
public class TestStartupOptionUpgrade {

  private Configuration conf;
  private StartupOption startOpt;
  private int layoutVersion;
  NNStorage storage;

  @Parameters
  public static Collection<Object[]> startOption() {
    Object[][] params = new Object[][] { { StartupOption.UPGRADE },
        { StartupOption.UPGRADEONLY } };
    return Arrays.asList(params);
  }

  public TestStartupOptionUpgrade(StartupOption startOption) {
    super();
    this.startOpt = startOption;
  }
  
  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    startOpt.setClusterId(null);
    storage = new NNStorage(conf,
      Collections.<URI>emptyList(),
      Collections.<URI>emptyList());
  }

  @After
  public void tearDown() throws Exception {
    conf = null;
    startOpt = null;
  }

  /**
   * Tests the upgrade from version 0.20.204 to Federation version Test without
   * clusterid the case: -upgrade 
   * Expected to generate clusterid
   * 
   * @throws Exception
   */
  @Test
  public void testStartupOptUpgradeFrom204() throws Exception {
    layoutVersion = Feature.RESERVED_REL20_204.getInfo().getLayoutVersion();
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);
    assertTrue("Clusterid should start with CID", storage.getClusterID()
        .startsWith("CID"));
  }

  /**
   * Tests the upgrade from version 0.22 to Federation version Test with
   * clusterid case: -upgrade -clusterid <cid> 
   * Expected to reuse user given clusterid
   * 
   * @throws Exception
   */
  @Test
  public void testStartupOptUpgradeFrom22WithCID() throws Exception {
    startOpt.setClusterId("cid");
    layoutVersion = Feature.RESERVED_REL22.getInfo().getLayoutVersion();
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);
    assertEquals("Clusterid should match with the given clusterid",
        "cid", storage.getClusterID());
  }

  /**
   * Tests the upgrade from one version of Federation to another Federation
   * version Test without clusterid case: -upgrade
   * Expected to reuse existing clusterid
   * 
   * @throws Exception
   */
  @Test
  public void testStartupOptUpgradeFromFederation()
      throws Exception {
    // Test assumes clusterid already exists, set the clusterid
    storage.setClusterID("currentcid");
    layoutVersion = Feature.FEDERATION.getInfo().getLayoutVersion();
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);
    assertEquals("Clusterid should match with the existing one",
        "currentcid", storage.getClusterID());
  }

  /**
   * Tests the upgrade from one version of Federation to another Federation
   * version Test with wrong clusterid case: -upgrade -clusterid <cid> 
   * Expected to reuse existing clusterid and ignore user given clusterid
   * 
   * @throws Exception
   */
  @Test
  public void testStartupOptUpgradeFromFederationWithWrongCID()
      throws Exception {
    startOpt.setClusterId("wrong-cid");
    storage.setClusterID("currentcid");
    layoutVersion = Feature.FEDERATION.getInfo().getLayoutVersion();
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);
    assertEquals("Clusterid should match with the existing one",
        "currentcid", storage.getClusterID());
  }

  /**
   * Tests the upgrade from one version of Federation to another Federation
   * version Test with correct clusterid case: -upgrade -clusterid <cid>
   * Expected to reuse existing clusterid and ignore user given clusterid
   * 
   * @throws Exception
   */
  @Test
  public void testStartupOptUpgradeFromFederationWithCID()
      throws Exception {
    startOpt.setClusterId("currentcid");
    storage.setClusterID("currentcid");
    layoutVersion = Feature.FEDERATION.getInfo().getLayoutVersion();
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);
    assertEquals("Clusterid should match with the existing one",
        "currentcid", storage.getClusterID());
  }
}
