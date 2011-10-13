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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to run enable of a table.
 */
public class EnableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(EnableTableHandler.class);
  private final byte [] tableName;
  private final String tableNameStr;
  private final AssignmentManager assignmentManager;
  private final CatalogTracker ct;

  public EnableTableHandler(Server server, byte [] tableName,
      CatalogTracker catalogTracker, AssignmentManager assignmentManager,
      boolean skipTableStateCheck)
  throws TableNotFoundException, TableNotDisabledException, IOException {
    super(server, EventType.C_M_ENABLE_TABLE);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(tableName);
    this.ct = catalogTracker;
    this.assignmentManager = assignmentManager;
    // Check if table exists
    if (!MetaReader.tableExists(catalogTracker, this.tableNameStr)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }

    // There could be multiple client requests trying to disable or enable
    // the table at the same time. Ensure only the first request is honored
    // After that, no other requests can be accepted until the table reaches
    // DISABLED or ENABLED.
    if (!skipTableStateCheck)
    {
      try {
        if (!this.assignmentManager.getZKTable().checkDisabledAndSetEnablingTable
          (this.tableNameStr)) {
          LOG.info("Table " + tableNameStr + " isn't disabled; skipping enable");
          throw new TableNotDisabledException(this.tableNameStr);
        }
      } catch (KeeperException e) {
        throw new IOException("Unable to ensure that the table will be" +
          " enabling because of a ZooKeeper issue", e);
      }
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" +
      tableNameStr;
  }

  @Override
  public void process() {
    try {
      LOG.info("Attemping to enable the table " + this.tableNameStr);
      handleEnableTable();
    } catch (IOException e) {
      LOG.error("Error trying to enable the table " + this.tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to enable the table " + this.tableNameStr, e);
    }
  }

  private void handleEnableTable() throws IOException, KeeperException {
    // I could check table is disabling and if so, not enable but require
    // that user first finish disabling but that might be obnoxious.

    // Set table enabling flag up in zk.
    this.assignmentManager.getZKTable().setEnablingTable(this.tableNameStr);
    boolean done = false;
    // Get the regions of this table. We're done when all listed
    // tables are onlined.
    List<HRegionInfo> regionsInMeta;
    regionsInMeta = MetaReader.getTableRegions(this.ct, tableName, true);
    int countOfRegionsInTable = regionsInMeta.size();
    List<HRegionInfo> regions = regionsToAssign(regionsInMeta);
    int regionsCount = regions.size();
    if (regionsCount == 0) {
      done = true;
    }
    LOG.info("Table has " + countOfRegionsInTable + " regions of which " +
      regionsCount + " are offline.");
    BulkEnabler bd = new BulkEnabler(this.server, regions,
      countOfRegionsInTable);
    try {
      if (bd.bulkAssign()) {
        done = true;
      }
    } catch (InterruptedException e) {
      LOG.warn("Enable was interrupted");
      // Preserve the interrupt.
      Thread.currentThread().interrupt();
    }
    // Flip the table to enabled.
    if (done) this.assignmentManager.getZKTable().setEnabledTable(
      this.tableNameStr);
    LOG.info("Enabled table is done=" + done);
  }

  /**
   * @param regionsInMeta This datastructure is edited by this method.
   * @return The <code>regionsInMeta</code> list minus the regions that have
   * been onlined; i.e. List of regions that need onlining.
   * @throws IOException
   */
  private List<HRegionInfo> regionsToAssign(
    final List<HRegionInfo> regionsInMeta)
  throws IOException {
    final List<HRegionInfo> onlineRegions =
      this.assignmentManager.getRegionsOfTable(tableName);
    regionsInMeta.removeAll(onlineRegions);
    return regionsInMeta;
  }

  /**
   * Run bulk enable.
   */
  class BulkEnabler extends BulkAssigner {
    private final List<HRegionInfo> regions;
    // Count of regions in table at time this assign was launched.
    private final int countOfRegionsInTable;

    BulkEnabler(final Server server, final List<HRegionInfo> regions,
        final int countOfRegionsInTable) {
      super(server);
      this.regions = regions;
      this.countOfRegionsInTable = countOfRegionsInTable;
    }

    @Override
    protected void populatePool(ExecutorService pool) {
      for (HRegionInfo region: regions) {
        if (assignmentManager.isRegionInTransition(region) != null) continue;
        final HRegionInfo hri = region;
        pool.execute(new Runnable() {
          public void run() {
            assignmentManager.assign(hri, true);
          }
        });
      }
    }

    @Override
    protected boolean waitUntilDone(long timeout)
    throws InterruptedException {
      long startTime = System.currentTimeMillis();
      long remaining = timeout;
      List<HRegionInfo> regions = null;
      int lastNumberOfRegions = this.countOfRegionsInTable;
      while (!server.isStopped() && remaining > 0) {
        Thread.sleep(waitingTimeForEvents);
        regions = assignmentManager.getRegionsOfTable(tableName);
        if (isDone(regions)) break;

        // Punt on the timeout as long we make progress
        if (regions.size() > lastNumberOfRegions) {
          lastNumberOfRegions = regions.size();
          timeout += waitingTimeForEvents;
        }
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
      return isDone(regions);
    }

    private boolean isDone(final List<HRegionInfo> regions) {
      return regions != null && regions.size() >= this.countOfRegionsInTable;
    }
  }
}
