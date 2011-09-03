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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.commons.logging.Log;

/**
 * Performs bulk reopen of the list of regions provided to it.
 */
public class BulkReOpen extends BulkAssigner {
  private final Map<ServerName, List<HRegionInfo>> rsToRegions;
  private final AssignmentManager assignmentManager;
  private static final Log LOG = LogFactory.getLog(BulkReOpen.class);

  public BulkReOpen(final Server server,
      final Map<ServerName, List<HRegionInfo>> serverToRegions,
    final AssignmentManager am) {
    super(server);
    this.assignmentManager = am;
    this.rsToRegions = serverToRegions;
  }

  /**
   * Unassign all regions, so that they go through the regular region
   * assignment flow (in assignment manager) and are re-opened.
   */
  @Override
  protected void populatePool(ExecutorService pool) {
    LOG.debug("Creating threads for each region server ");
    for (Map.Entry<ServerName, List<HRegionInfo>> e : rsToRegions
        .entrySet()) {
      final List<HRegionInfo> hris = e.getValue();
      // add a plan for each of the regions that needs to be reopened
      for (HRegionInfo hri : hris) {
        RegionPlan reOpenPlan = new RegionPlan(hri, null,
            assignmentManager.getRegionServerOfRegion(hri));
        assignmentManager.addPlan(hri.getEncodedName(), reOpenPlan);
      }
      pool.execute(new Runnable() {
        public void run() {
          assignmentManager.unassign(hris);
        }
      });
    }
  }

 /**
  * Reopen the regions asynchronously, so always returns true immediately.
  * @return true
  */
  @Override
  protected boolean waitUntilDone(long timeout) {
    return true;
  }

  /**
   * Configuration knobs "hbase.bulk.reopen.threadpool.size" number of regions
   * that can be reopened concurrently. The maximum number of threads the master
   * creates is never more than the number of region servers.
   * If configuration is not defined it defaults to 20
   */
  protected int getThreadCount() {
    int defaultThreadCount = super.getThreadCount();
    return this.server.getConfiguration().getInt(
        "hbase.bulk.reopen.threadpool.size", defaultThreadCount);
  }

  public boolean bulkReOpen() throws InterruptedException {
    return bulkAssign();
  }
}
