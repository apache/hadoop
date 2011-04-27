/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.catalog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;

/**
 * Do {@link CatalogTracker} tests on running cluster.
 */
public class TestCatalogTrackerOnCluster {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Log LOG =
    LogFactory.getLog(TestCatalogTrackerOnCluster.class);

  /**
   * @throws Exception 
   * @see https://issues.apache.org/jira/browse/HBASE-3445
   */
  @Test public void testBadOriginalRootLocation() throws Exception {
    UTIL.getConfiguration().setInt("ipc.socket.timeout", 3000);
    // Launch cluster so it does bootstrapping.
    UTIL.startMiniCluster();
    // Shutdown hbase.
    UTIL.shutdownMiniHBaseCluster();
    // Mess with the root location in the running zk.  Set it to be nonsense.
    ZooKeeperWatcher zookeeper = new ZooKeeperWatcher(UTIL.getConfiguration(),
      "Bad Root Location Writer", new Abortable() {
        @Override
        public void abort(String why, Throwable e) {
          LOG.error("Abort was called on 'bad root location writer'", e);
        }
    });
    ServerName nonsense =
      new ServerName("example.org", 1234, System.currentTimeMillis());
    RootLocationEditor.setRootLocation(zookeeper, nonsense);
    // Bring back up the hbase cluster.  See if it can deal with nonsense root
    // location.
    UTIL.startMiniHBaseCluster(1, 1);
    UTIL.shutdownMiniCluster();
  }
}
