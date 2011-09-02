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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to run disable of a table.
 */
public class DisableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(DisableTableHandler.class);
  private final byte [] tableName;
  private final String tableNameStr;
  private final AssignmentManager assignmentManager;

  public DisableTableHandler(Server server, byte [] tableName,
      CatalogTracker catalogTracker, AssignmentManager assignmentManager,
      boolean skipTableStateCheck)
  throws TableNotFoundException, TableNotEnabledException, IOException {
    super(server, EventType.C_M_DISABLE_TABLE);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(this.tableName);
    this.assignmentManager = assignmentManager;
    // Check if table exists
    // TODO: do we want to keep this in-memory as well?  i guess this is
    //       part of old master rewrite, schema to zk to check for table
    //       existence and such
    if (!MetaReader.tableExists(catalogTracker, this.tableNameStr)) {
      throw new TableNotFoundException(this.tableNameStr);
    }

    // There could be multiple client requests trying to disable or enable
    // the table at the same time. Ensure only the first request is honored
    // After that, no other requests can be accepted until the table reaches
    // DISABLED or ENABLED.
    if (!skipTableStateCheck)
    {
      try {
        if (!this.assignmentManager.getZKTable().checkEnabledAndSetDisablingTable
          (this.tableNameStr)) {
          LOG.info("Table " + tableNameStr + " isn't enabled; skipping disable");
          throw new TableNotEnabledException(this.tableNameStr);
        }
      } catch (KeeperException e) {
        throw new IOException("Unable to ensure that the table will be" +
          " disabling because of a ZooKeeper issue", e);
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
      LOG.info("Attemping to disable table " + this.tableNameStr);
      handleDisableTable();
    } catch (IOException e) {
      LOG.error("Error trying to disable table " + this.tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to disable table " + this.tableNameStr, e);
    }
  }

  private void handleDisableTable() throws IOException, KeeperException {
    // Set table disabling flag up in zk.
    this.assignmentManager.getZKTable().setDisablingTable(this.tableNameStr);
    boolean done = false;
    while (true) {
      // Get list of online regions that are of this table.  Regions that are
      // already closed will not be included in this list; i.e. the returned
      // list is not ALL regions in a table, its all online regions according
      // to the in-memory state on this master.
      final List<HRegionInfo> regions =
        this.assignmentManager.getRegionsOfTable(tableName);
      if (regions.size() == 0) {
        done = true;
        break;
      }
      LOG.info("Offlining " + regions.size() + " regions.");
      BulkDisabler bd = new BulkDisabler(this.server, regions);
      try {
        if (bd.bulkAssign()) {
          done = true;
          break;
        }
      } catch (InterruptedException e) {
        LOG.warn("Disable was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        break;
      }
    }
    // Flip the table to disabled if success.
    if (done) this.assignmentManager.getZKTable().setDisabledTable(this.tableNameStr);
    LOG.info("Disabled table is done=" + done);
  }

  /**
   * Run bulk disable.
   */
  class BulkDisabler extends BulkAssigner {
    private final List<HRegionInfo> regions;

    BulkDisabler(final Server server, final List<HRegionInfo> regions) {
      super(server);
      this.regions = regions;
    }

    @Override
    protected void populatePool(ExecutorService pool) {
      for (HRegionInfo region: regions) {
        if (assignmentManager.isRegionInTransition(region) != null) continue;
        final HRegionInfo hri = region;
        pool.execute(new Runnable() {
          public void run() {
            assignmentManager.unassign(hri);
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
      while (!server.isStopped() && remaining > 0) {
        Thread.sleep(waitingTimeForEvents);
        regions = assignmentManager.getRegionsOfTable(tableName);
        if (regions.isEmpty()) break;
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
      return regions != null && regions.isEmpty();
    }
  }
}
