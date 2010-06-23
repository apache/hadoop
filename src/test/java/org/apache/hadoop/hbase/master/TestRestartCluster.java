/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRestartCluster {
  private static final Log LOG = LogFactory.getLog(TestRestartCluster.class);
  private static Configuration conf;
  private static HBaseTestingUtility utility;
  private static ZooKeeperWrapper zkWrapper;
  private static final byte[] TABLENAME = Bytes.toBytes("master_transitions");
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a")};
  
  @BeforeClass public static void beforeAllTests() throws Exception {
    conf = HBaseConfiguration.create();
    utility = new HBaseTestingUtility(conf);
  }

  @AfterClass public static void afterAllTests() throws IOException {
    utility.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
  }

  @Test (timeout=300000) public void testRestartClusterAfterKill()throws Exception {
    utility.startMiniZKCluster();
    zkWrapper = ZooKeeperWrapper.createInstance(conf, "cluster1");

    // create the unassigned region, throw up a region opened state for META
    String unassignedZNode = zkWrapper.getRegionInTransitionZNode();
    zkWrapper.createZNodeIfNotExists(unassignedZNode);
    byte[] data = null;
    HBaseEventType hbEventType = HBaseEventType.RS2ZK_REGION_OPENED;
    try {
      data = Writables.getBytes(new RegionTransitionEventData(hbEventType, HMaster.MASTER));
    } catch (IOException e) {
      LOG.error("Error creating event data for " + hbEventType, e);
    }
    zkWrapper.createUnassignedRegion(HRegionInfo.ROOT_REGIONINFO.getEncodedName(), data);
    zkWrapper.createUnassignedRegion(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName(), data);
    LOG.debug("Created UNASSIGNED zNode for ROOT and META regions in state " + HBaseEventType.M2ZK_REGION_OFFLINE);
    
    // start the HB cluster
    LOG.info("Starting HBase cluster...");
    utility.startMiniCluster(2);  
    
    utility.createTable(TABLENAME, FAMILIES);
    LOG.info("Created a table, waiting for table to be available...");
    utility.waitTableAvailable(TABLENAME, 60*1000);

    LOG.info("Master deleted unassgined region and started up successfully.");
  }
}
