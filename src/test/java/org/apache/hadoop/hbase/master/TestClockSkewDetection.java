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

import java.net.InetAddress;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;


public class TestClockSkewDetection {
  private static final Log LOG =
    LogFactory.getLog(TestClockSkewDetection.class);

  @Test
  public void testClockSkewDetection() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    ServerManager sm = new ServerManager(new Server() {
      @Override
      public CatalogTracker getCatalogTracker() {
        return null;
      }

      @Override
      public Configuration getConfiguration() {
        return conf;
      }

      @Override
      public ServerName getServerName() {
        return null;
      }

      @Override
      public ZooKeeperWatcher getZooKeeper() {
        return null;
      }

      @Override
      public void abort(String why, Throwable e) {}

      @Override
      public boolean isStopped() {
        return false;
      }

      @Override
      public void stop(String why) {
      }}, null);

    LOG.debug("regionServerStartup 1");
    InetAddress ia1 = InetAddress.getLocalHost();
    sm.regionServerStartup(ia1, 1234, -1, System.currentTimeMillis());

    long maxSkew = 30000;

    try {
      LOG.debug("regionServerStartup 2");
      InetAddress ia2 = InetAddress.getLocalHost();
      sm.regionServerStartup(ia2, 1235, -1, System.currentTimeMillis() - maxSkew * 2);
      Assert.assertTrue("HMaster should have thrown an ClockOutOfSyncException "
        + "but didn't.", false);
    } catch(ClockOutOfSyncException e) {
      //we want an exception
      LOG.info("Recieved expected exception: "+e);
    }
  }
}