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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to create a table.
 */
public class CreateTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(CreateTableHandler.class);
  private MasterFileSystem fileSystemManager;
  private final HTableDescriptor hTableDescriptor;
  private Configuration conf;
  private final AssignmentManager assignmentManager;
  private final CatalogTracker catalogTracker;
  private final ServerManager serverManager;
  private final HRegionInfo [] newRegions;

  public CreateTableHandler(Server server, MasterFileSystem fileSystemManager,
    ServerManager serverManager, HTableDescriptor hTableDescriptor,
    Configuration conf, HRegionInfo [] newRegions,
    CatalogTracker catalogTracker, AssignmentManager assignmentManager)
    throws NotAllMetaRegionsOnlineException, TableExistsException,
    IOException {
    super(server, EventType.C_M_CREATE_TABLE);

    this.fileSystemManager = fileSystemManager;
    this.serverManager = serverManager;
    this.hTableDescriptor = hTableDescriptor;
    this.conf = conf;
    this.newRegions = newRegions;
    this.catalogTracker = catalogTracker;
    this.assignmentManager = assignmentManager;

    int timeout = conf.getInt("hbase.client.catalog.timeout", 10000);
    // Need META availability to create a table
    try {
      if(catalogTracker.waitForMeta(timeout) == null) {
        throw new NotAllMetaRegionsOnlineException();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for meta availability", e);
      throw new IOException(e);
    }

    String tableName = this.hTableDescriptor.getNameAsString();
    if (MetaReader.tableExists(catalogTracker, tableName)) {
      throw new TableExistsException(tableName);
    }

    // If we have multiple client threads trying to create the table at the
    // same time, given the async nature of the operation, the table
    // could be in a state where .META. table hasn't been updated yet in
    // the process() function.
    // Use enabling state to tell if there is already a request for the same
    // table in progress. This will introduce a new zookeeper call. Given
    // createTable isn't a frequent operation, that should be ok.
    try {
      if (!this.assignmentManager.getZKTable().checkAndSetEnablingTable(
        tableName))
        throw new TableExistsException(tableName);
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be" +
        " enabling because of a ZooKeeper issue", e);
    }
  }


  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" +
      this.hTableDescriptor.getNameAsString();
  }

  @Override
  public void process() {
    String tableName = this.hTableDescriptor.getNameAsString();
    try {
      LOG.info("Attemping to create the table " + tableName);
      handleCreateTable();
    } catch (IOException e) {
      LOG.error("Error trying to create the table " + tableName, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to create the table " + tableName, e);
    }
  }

  private void handleCreateTable() throws IOException, KeeperException {

    // TODO: Currently we make the table descriptor and as side-effect the
    // tableDir is created.  Should we change below method to be createTable
    // where we create table in tmp dir with its table descriptor file and then
    // do rename to move it into place?
    FSTableDescriptors.createTableDescriptor(this.hTableDescriptor, this.conf);

    List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
    final int batchSize =
      this.conf.getInt("hbase.master.createtable.batchsize", 100);
    HLog hlog = null;
    for (int regionIdx = 0; regionIdx < this.newRegions.length; regionIdx++) {
      HRegionInfo newRegion = this.newRegions[regionIdx];
      // 1. Create HRegion
      HRegion region = HRegion.createHRegion(newRegion,
        this.fileSystemManager.getRootDir(), this.conf,
        this.hTableDescriptor, hlog);
      if (hlog == null) {
        hlog = region.getLog();
      }

      regionInfos.add(region.getRegionInfo());
      if (regionIdx % batchSize == 0) {
        // 2. Insert into META
        MetaEditor.addRegionsToMeta(this.catalogTracker, regionInfos);
        regionInfos.clear();
      }

      // 3. Close the new region to flush to disk.  Close log file too.
      region.close();
    }
    hlog.closeAndDelete();
    if (regionInfos.size() > 0) {
      MetaEditor.addRegionsToMeta(this.catalogTracker, regionInfos);
    }

    // 4. Trigger immediate assignment of the regions in round-robin fashion
    List<ServerName> servers = serverManager.getOnlineServersList();
    try {
      this.assignmentManager.assignUserRegions(Arrays.asList(newRegions),
        servers);
    } catch (InterruptedException ie) {
      LOG.error("Caught " + ie + " during round-robin assignment");
      throw new IOException(ie);
    }

    // 5. Set table enabled flag up in zk.
    try {
      assignmentManager.getZKTable().
        setEnabledTable(this.hTableDescriptor.getNameAsString());
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be" +
        " enabled because of a ZooKeeper issue", e);
    }
  }
}