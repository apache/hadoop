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
package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.junit.Test;

/**
 * Test the Membership State records.
 */
public class TestMembershipState {

  private static final String ROUTER = "router";
  private static final String NAMESERVICE = "nameservice";
  private static final String NAMENODE = "namenode";
  private static final String CLUSTER_ID = "cluster";
  private static final String BLOCKPOOL_ID = "blockpool";
  private static final String RPC_ADDRESS = "rpcaddress";
  private static final String SERVICE_ADDRESS = "serviceaddress";
  private static final String LIFELINE_ADDRESS = "lifelineaddress";
  private static final String WEB_ADDRESS = "webaddress";
  private static final boolean SAFE_MODE = false;
  private static final String SCHEME = "http";

  private static final long DATE_CREATED = 100;
  private static final long DATE_MODIFIED = 200;

  private static final long NUM_BLOCKS = 300;
  private static final long NUM_FILES = 400;
  private static final int NUM_DEAD = 500;
  private static final int NUM_STALE = 550;
  private static final int NUM_ACTIVE = 600;
  private static final int NUM_DECOM = 700;
  private static final int NUM_DECOM_ACTIVE = 800;
  private static final int NUM_DECOM_DEAD = 900;
  private static final int NUM_MAIN_LIVE = 151;
  private static final int NUM_MAIN_DEAD = 303;
  private static final int NUM_ENTER_MAIN = 144;
  private static final long NUM_BLOCK_MISSING = 1000;

  private static final long TOTAL_SPACE = 1100;
  private static final long AVAILABLE_SPACE = 1200;

  private static final FederationNamenodeServiceState STATE =
      FederationNamenodeServiceState.ACTIVE;

  private MembershipState createRecord() throws IOException {

    MembershipState record = MembershipState.newInstance(
        ROUTER, NAMESERVICE, NAMENODE, CLUSTER_ID,
        BLOCKPOOL_ID, RPC_ADDRESS, SERVICE_ADDRESS, LIFELINE_ADDRESS,
        SCHEME, WEB_ADDRESS, STATE, SAFE_MODE);
    record.setDateCreated(DATE_CREATED);
    record.setDateModified(DATE_MODIFIED);

    MembershipStats stats = MembershipStats.newInstance();
    stats.setNumOfBlocks(NUM_BLOCKS);
    stats.setNumOfFiles(NUM_FILES);
    stats.setNumOfActiveDatanodes(NUM_ACTIVE);
    stats.setNumOfDeadDatanodes(NUM_DEAD);
    stats.setNumOfStaleDatanodes(NUM_STALE);
    stats.setNumOfDecommissioningDatanodes(NUM_DECOM);
    stats.setNumOfDecomActiveDatanodes(NUM_DECOM_ACTIVE);
    stats.setNumOfDecomDeadDatanodes(NUM_DECOM_DEAD);
    stats.setNumOfInMaintenanceLiveDataNodes(NUM_MAIN_LIVE);
    stats.setNumOfInMaintenanceDeadDataNodes(NUM_MAIN_DEAD);
    stats.setNumOfEnteringMaintenanceDataNodes(NUM_ENTER_MAIN);
    stats.setNumOfBlocksMissing(NUM_BLOCK_MISSING);
    stats.setTotalSpace(TOTAL_SPACE);
    stats.setAvailableSpace(AVAILABLE_SPACE);
    record.setStats(stats);
    return record;
  }

  private void validateRecord(MembershipState record) throws IOException {

    assertEquals(ROUTER, record.getRouterId());
    assertEquals(NAMESERVICE, record.getNameserviceId());
    assertEquals(CLUSTER_ID, record.getClusterId());
    assertEquals(BLOCKPOOL_ID, record.getBlockPoolId());
    assertEquals(RPC_ADDRESS, record.getRpcAddress());
    assertEquals(SCHEME, record.getWebScheme());
    assertEquals(WEB_ADDRESS, record.getWebAddress());
    assertEquals(STATE, record.getState());
    assertEquals(SAFE_MODE, record.getIsSafeMode());
    assertEquals(DATE_CREATED, record.getDateCreated());
    assertEquals(DATE_MODIFIED, record.getDateModified());

    MembershipStats stats = record.getStats();
    assertEquals(NUM_BLOCKS, stats.getNumOfBlocks());
    assertEquals(NUM_FILES, stats.getNumOfFiles());
    assertEquals(NUM_ACTIVE, stats.getNumOfActiveDatanodes());
    assertEquals(NUM_DEAD, stats.getNumOfDeadDatanodes());
    assertEquals(NUM_STALE, stats.getNumOfStaleDatanodes());
    assertEquals(NUM_DECOM, stats.getNumOfDecommissioningDatanodes());
    assertEquals(NUM_DECOM_ACTIVE, stats.getNumOfDecomActiveDatanodes());
    assertEquals(NUM_DECOM_DEAD, stats.getNumOfDecomDeadDatanodes());
    assertEquals(NUM_MAIN_LIVE, stats.getNumOfInMaintenanceLiveDataNodes());
    assertEquals(NUM_MAIN_DEAD, stats.getNumOfInMaintenanceDeadDataNodes());
    assertEquals(NUM_ENTER_MAIN, stats.getNumOfEnteringMaintenanceDataNodes());
    assertEquals(TOTAL_SPACE, stats.getTotalSpace());
    assertEquals(AVAILABLE_SPACE, stats.getAvailableSpace());
  }

  @Test
  public void testGetterSetter() throws IOException {
    MembershipState record = createRecord();
    validateRecord(record);
  }

  @Test
  public void testSerialization() throws IOException {

    MembershipState record = createRecord();

    StateStoreSerializer serializer = StateStoreSerializer.getSerializer();
    String serializedString = serializer.serializeString(record);
    MembershipState newRecord =
        serializer.deserialize(serializedString, MembershipState.class);

    validateRecord(newRecord);
  }
}