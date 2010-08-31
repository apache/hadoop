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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.util.Bytes;


public class DisableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(DisableTableHandler.class);

  private final byte [] tableName;
  private final String tableNameStr;
  private final AssignmentManager assignmentManager;

  public DisableTableHandler(Server server, byte [] tableName,
      CatalogTracker catalogTracker, AssignmentManager assignmentManager)
  throws TableNotFoundException, IOException {
    super(server, EventType.C2M_DISABLE_TABLE);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(this.tableName);
    this.assignmentManager = assignmentManager;
    // Check if table exists
    // TODO: do we want to keep this in-memory as well?  i guess this is
    //       part of old master rewrite, schema to zk to check for table
    //       existence and such
    if(!MetaReader.tableExists(catalogTracker, this.tableNameStr)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
  }

  @Override
  public void process() {
    try {
      LOG.info("Attemping to disable the table " + this.tableNameStr);
      handleDisableTable();
    } catch (IOException e) {
      LOG.error("Error trying to disable the table " + this.tableNameStr, e);
    }
  }

  private void handleDisableTable() throws IOException {
    // Set the table as disabled so it doesn't get re-onlined
    assignmentManager.disableTable(this.tableNameStr);
    // Get the online regions of this table.
    // TODO: What if region splitting at the time we get this listing?
    // TODO: Remove offline flag from HRI
    // TODO: Confirm we have parallel closing going on.
    List<HRegionInfo> regions = assignmentManager.getRegionsOfTable(tableName);
    // Unassign the online regions
    for(HRegionInfo region : regions) {
      assignmentManager.unassign(region);
    }
  }
}