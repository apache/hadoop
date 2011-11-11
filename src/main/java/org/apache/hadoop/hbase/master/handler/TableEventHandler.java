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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.BulkReOpen;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for performing operations against tables.
 * Checks on whether the process can go forward are done in constructor rather
 * than later on in {@link #process()}.  The idea is to fail fast rather than
 * later down in an async invocation of {@link #process()} (which currently has
 * no means of reporting back issues once started).
 */
public abstract class TableEventHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(TableEventHandler.class);
  protected final MasterServices masterServices;
  protected final byte [] tableName;
  protected final String tableNameStr;

  public TableEventHandler(EventType eventType, byte [] tableName, Server server,
      MasterServices masterServices)
  throws IOException {
    super(server, eventType);
    this.masterServices = masterServices;
    this.tableName = tableName;
    try {
      this.masterServices.checkTableModifiable(tableName);
    } catch (TableNotDisabledException ex)  {
      if (eventType.isOnlineSchemaChangeSupported()) {
        LOG.debug("Ignoring table not disabled exception " +
            "for supporting online schema changes.");
      }	else {
        throw ex;
      }
    }
    this.tableNameStr = Bytes.toString(this.tableName);
  }

  @Override
  public void process() {
    try {
      LOG.info("Handling table operation " + eventType + " on table " +
          Bytes.toString(tableName));
      List<HRegionInfo> hris =
        MetaReader.getTableRegions(this.server.getCatalogTracker(),
          tableName);
      handleTableOperation(hris);
      if (eventType.isOnlineSchemaChangeSupported() && this.masterServices.
          getAssignmentManager().getZKTable().
          isEnabledTable(Bytes.toString(tableName))) {
        if (reOpenAllRegions(hris)) {
          LOG.info("Completed table operation " + eventType + " on table " +
              Bytes.toString(tableName));
        } else {
          LOG.warn("Error on reopening the regions");
        }
      }
    } catch (IOException e) {
      LOG.error("Error manipulating table " + Bytes.toString(tableName), e);
    } catch (KeeperException e) {
      LOG.error("Error manipulating table " + Bytes.toString(tableName), e);
    }
  }

  public boolean reOpenAllRegions(List<HRegionInfo> regions) throws IOException {
    boolean done = false;
    LOG.info("Bucketing regions by region server...");
    HTable table = new HTable(masterServices.getConfiguration(), tableName);
    TreeMap<ServerName, List<HRegionInfo>> serverToRegions = Maps
        .newTreeMap();
    NavigableMap<HRegionInfo, ServerName> hriHserverMapping = table.getRegionLocations();
    List<HRegionInfo> reRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo hri : regions) {
      ServerName rsLocation = hriHserverMapping.get(hri);

      // Skip the offlined split parent region
      // See HBASE-4578 for more information.
      if (null == rsLocation) {
        LOG.info("Skip " + hri);
        continue;
      }
      if (!serverToRegions.containsKey(rsLocation)) {
        LinkedList<HRegionInfo> hriList = Lists.newLinkedList();
        serverToRegions.put(rsLocation, hriList);
      }
      reRegions.add(hri);
      serverToRegions.get(rsLocation).add(hri);
    }
    
    LOG.info("Reopening " + reRegions.size() + " regions on "
        + serverToRegions.size() + " region servers.");
    this.masterServices.getAssignmentManager().setRegionsToReopen(reRegions);
    BulkReOpen bulkReopen = new BulkReOpen(this.server, serverToRegions,
        this.masterServices.getAssignmentManager());
    while (true) {
      try {
        if (bulkReopen.bulkReOpen()) {
          done = true;
          break;
        } else {
          LOG.warn("Timeout before reopening all regions");
        }
      } catch (InterruptedException e) {
        LOG.warn("Reopen was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        break;
      }
    }
    return done;
  }

  /**
   * @return Table descriptor for this table
   * @throws TableExistsException
   * @throws FileNotFoundException
   * @throws IOException
   */
  HTableDescriptor getTableDescriptor()
  throws TableExistsException, FileNotFoundException, IOException {
    final String name = Bytes.toString(tableName);
    HTableDescriptor htd =
      this.masterServices.getTableDescriptors().get(name);
    if (htd == null) {
      throw new IOException("HTableDescriptor missing for " + name);
    }
    return htd;
  }

  byte [] hasColumnFamily(final HTableDescriptor htd, final byte [] cf)
  throws InvalidFamilyOperationException {
    if (!htd.hasFamily(cf)) {
      throw new InvalidFamilyOperationException("Column family '" +
        Bytes.toString(cf) + "' does not exist");
    }
    return cf;
  }

  protected abstract void handleTableOperation(List<HRegionInfo> regions)
  throws IOException, KeeperException;
}
