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
package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.junit.Test;

/**
 * Test the Router State records.
 */
public class TestRouterState {

  private static final String ADDRESS = "address";
  private static final String VERSION = "version";
  private static final String COMPILE_INFO = "compileInfo";
  private static final long START_TIME = 100;
  private static final long DATE_MODIFIED = 200;
  private static final long DATE_CREATED = 300;
  private static final long FILE_RESOLVER_VERSION = 500;
  private static final RouterServiceState STATE = RouterServiceState.RUNNING;


  private RouterState generateRecord() {
    RouterState record = RouterState.newInstance(ADDRESS, START_TIME, STATE);
    record.setVersion(VERSION);
    record.setCompileInfo(COMPILE_INFO);
    record.setDateCreated(DATE_CREATED);
    record.setDateModified(DATE_MODIFIED);

    StateStoreVersion version = StateStoreVersion.newInstance();
    version.setMountTableVersion(FILE_RESOLVER_VERSION);
    record.setStateStoreVersion(version);
    return record;
  }

  private void validateRecord(RouterState record) throws IOException {
    assertEquals(ADDRESS, record.getAddress());
    assertEquals(START_TIME, record.getDateStarted());
    assertEquals(STATE, record.getStatus());
    assertEquals(COMPILE_INFO, record.getCompileInfo());
    assertEquals(VERSION, record.getVersion());

    StateStoreVersion version = record.getStateStoreVersion();
    assertEquals(FILE_RESOLVER_VERSION, version.getMountTableVersion());
  }

  @Test
  public void testGetterSetter() throws IOException {
    RouterState record = generateRecord();
    validateRecord(record);
  }

  @Test
  public void testSerialization() throws IOException {

    RouterState record = generateRecord();

    StateStoreSerializer serializer = StateStoreSerializer.getSerializer();
    String serializedString = serializer.serializeString(record);
    RouterState newRecord =
        serializer.deserialize(serializedString, RouterState.class);

    validateRecord(newRecord);
  }

  @Test
  public void testStateStoreResilience() throws Exception {
    StateStoreService service = new StateStoreService();
    Configuration conf = new Configuration();
    conf.setClass(RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        MockStateStoreDriver.class,
        StateStoreDriver.class);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE, false);
    service.init(conf);
    MockStateStoreDriver driver = (MockStateStoreDriver) service.getDriver();
    driver.clearAll();
    // Add two records for block1
    driver.put(MembershipState.newInstance("routerId", "ns1",
        "ns1-ha1", "cluster1", "block1", "rpc1",
        "service1", "lifeline1", "https", "nn01",
        FederationNamenodeServiceState.ACTIVE, false), false, false);
    driver.put(MembershipState.newInstance("routerId", "ns1",
        "ns1-ha2", "cluster1", "block1", "rpc2",
        "service2", "lifeline2", "https", "nn02",
        FederationNamenodeServiceState.STANDBY, false), false, false);
    // load the cache
    service.loadDriver();
    MembershipNamenodeResolver resolver = new MembershipNamenodeResolver(conf, service);
    service.refreshCaches(true);

    // look up block1
    List<? extends FederationNamenodeContext> result =
        resolver.getNamenodesForBlockPoolId("block1");
    assertEquals(2, result.size());

    // cause io errors and then reload the cache
    driver.setGiveErrors(true);
    long previousUpdate = service.getCacheUpdateTime();
    service.refreshCaches(true);
    assertEquals(previousUpdate, service.getCacheUpdateTime());

    // make sure the old cache is still there
    result = resolver.getNamenodesForBlockPoolId("block1");
    assertEquals(2, result.size());
    service.stop();
  }
}
