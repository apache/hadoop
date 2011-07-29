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
package org.apache.hadoop.hbase.zookeeper;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZKTable {
  private static final Log LOG = LogFactory.getLog(TestZooKeeperNodeTracker.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testTableStates()
  throws ZooKeeperConnectionException, IOException, KeeperException {
    final String name = "testDisabled";
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.info(why, e);
      }
    };
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      name, abortable, true);
    ZKTable zkt = new ZKTable(zkw);
    assertTrue(zkt.isEnabledTable(name));
    assertFalse(zkt.isDisablingTable(name));
    assertFalse(zkt.isDisabledTable(name));
    assertFalse(zkt.isEnablingTable(name));
    assertFalse(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.isDisabledOrEnablingTable(name));
    zkt.setDisablingTable(name);
    assertTrue(zkt.isDisablingTable(name));
    assertTrue(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.getDisabledTables().contains(name));
    zkt.setDisabledTable(name);
    assertTrue(zkt.isDisabledTable(name));
    assertTrue(zkt.isDisablingOrDisabledTable(name));
    assertFalse(zkt.isDisablingTable(name));
    assertTrue(zkt.getDisabledTables().contains(name));
    zkt.setEnablingTable(name);
    assertTrue(zkt.isEnablingTable(name));
    assertTrue(zkt.isDisabledOrEnablingTable(name));
    assertFalse(zkt.isDisabledTable(name));
    assertFalse(zkt.getDisabledTables().contains(name));
    zkt.setEnabledTable(name);
    assertTrue(zkt.isEnabledTable(name));
    assertFalse(zkt.isEnablingTable(name));
  }
}
