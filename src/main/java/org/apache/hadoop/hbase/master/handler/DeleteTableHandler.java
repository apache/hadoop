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
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.master.MasterServices;

public class DeleteTableHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(DeleteTableHandler.class);

  public DeleteTableHandler(byte [] tableName, Server server,
      final MasterServices masterServices) throws IOException {
    super(EventType.C_M_DELETE_TABLE, tableName, server, masterServices);
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions)
  throws IOException {
    for(HRegionInfo region : regions) {
      LOG.debug("Deleting region " + region + " from META and FS");
      // Remove region from META
      MetaEditor.deleteRegion(this.server.getCatalogTracker(), region);
      // Delete region from FS
      this.masterServices.getMasterFileSystem().deleteRegion(region);
    }
    // Delete table from FS
    this.masterServices.getMasterFileSystem().deleteTable(tableName);
  }
}