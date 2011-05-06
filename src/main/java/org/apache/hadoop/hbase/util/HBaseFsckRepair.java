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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

public class HBaseFsckRepair {

  /**
   * Fix dupe assignment by doing silent closes on each RS hosting the region
   * and then force ZK unassigned node to OFFLINE to trigger assignment by
   * master.
   * @param conf
   * @param region
   * @param servers
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void fixDupeAssignment(Configuration conf, HRegionInfo region,
      List<ServerName> servers)
  throws IOException, KeeperException, InterruptedException {

    HRegionInfo actualRegion = new HRegionInfo(region);

    // Close region on the servers silently
    for(ServerName server : servers) {
      closeRegionSilentlyAndWait(conf, server, actualRegion);
    }

    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(conf, actualRegion);
  }

  /**
   * Fix unassigned by creating/transition the unassigned ZK node for this
   * region to OFFLINE state with a special flag to tell the master that this
   * is a forced operation by HBCK.
   * @param conf
   * @param region
   * @throws IOException
   * @throws KeeperException
   */
  public static void fixUnassigned(Configuration conf, HRegionInfo region)
  throws IOException, KeeperException {
    HRegionInfo actualRegion = new HRegionInfo(region);

    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(conf, actualRegion);
  }

  private static void forceOfflineInZK(Configuration conf, final HRegionInfo region)
  throws ZooKeeperConnectionException, KeeperException, IOException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        try {
          ZKAssign.createOrForceNodeOffline(
              connection.getZooKeeperWatcher(),
              region, HConstants.HBCK_CODE_SERVERNAME);
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
        return null;
      }
    });
  }

  private static void closeRegionSilentlyAndWait(Configuration conf,
      ServerName server, HRegionInfo region) throws IOException,
      InterruptedException {
    HConnection connection = HConnectionManager.getConnection(conf);
    boolean success = false;
    try {
      HRegionInterface rs =
        connection.getHRegionConnection(server.getHostname(), server.getPort());
      rs.closeRegion(region, false);
      long timeout = conf.getLong("hbase.hbck.close.timeout", 120000);
      long expiration = timeout + System.currentTimeMillis();
      while (System.currentTimeMillis() < expiration) {
        try {
          HRegionInfo rsRegion = rs.getRegionInfo(region.getRegionName());
          if (rsRegion == null)
            throw new NotServingRegionException();
        } catch (Exception e) {
          success = true;
          return;
        }
        Thread.sleep(1000);
      }
      throw new IOException("Region " + region + " failed to close within"
          + " timeout " + timeout);
    } finally {
      try {
        connection.close();
      } catch (IOException ioe) {
        if (success) {
          throw ioe;
        }
      }
    }
  }
}