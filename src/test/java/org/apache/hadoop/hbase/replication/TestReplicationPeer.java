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
package org.apache.hadoop.hbase.replication;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestReplicationPeer {

  private static final Log LOG = LogFactory.getLog(TestReplicationPeer.class);
  private static HBaseTestingUtility utility;
  private static Configuration conf;
  private static ReplicationPeer rp;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = HBaseConfiguration.create();
    utility = new HBaseTestingUtility(conf);
    conf = utility.getConfiguration();
    utility.startMiniZKCluster();

    rp = new ReplicationPeer(conf, "clusterKey", "clusterId");
  }

  @Test(timeout=300000)
  public void testResetZooKeeperSession() throws Exception {
    ZooKeeperWatcher zkw = rp.getZkw();
    zkw.getRecoverableZooKeeper().exists("/1/2", false);

    LOG.info("Expiring ReplicationPeer ZooKeeper session.");
    utility.expireSession(zkw, null, false);

    try {
      LOG.info("Attempting to use expired ReplicationPeer ZooKeeper session.");
      // Trying to use the expired session to assert that it is indeed closed
      zkw.getRecoverableZooKeeper().exists("/1/2", false);
    } catch (SessionExpiredException k) {
      rp.reloadZkWatcher();

      zkw = rp.getZkw();

      // Try to use the connection again
      LOG.info("Attempting to use refreshed "
          + "ReplicationPeer ZooKeeper session.");
      zkw.getRecoverableZooKeeper().exists("/1/2", false);

      return;
    }

    Assert.fail("ReplicationPeer ZooKeeper session was not properly expired.");
  }

}
