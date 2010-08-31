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
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;

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

  public TableEventHandler(EventType eventType, byte [] tableName, Server server,
      MasterServices masterServices)
  throws IOException {
    super(server, eventType);
    this.masterServices = masterServices;
    this.tableName = tableName;
    this.masterServices.checkTableModifiable(tableName);
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
    } catch (IOException e) {
      LOG.error("Error trying to delete the table " + Bytes.toString(tableName),
          e);
    }
  }

  protected abstract void handleTableOperation(List<HRegionInfo> regions)
  throws IOException;
}