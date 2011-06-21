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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Handles adding a new family to an existing table.
 */
public class TableDeleteFamilyHandler extends TableEventHandler {

  private final byte [] familyName;

  public TableDeleteFamilyHandler(byte[] tableName, byte [] familyName,
      Server server, final MasterServices masterServices) throws IOException {
    super(EventType.C_M_ADD_FAMILY, tableName, server, masterServices);
    this.familyName = familyName;
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> hris) throws IOException {
    AssignmentManager am = this.masterServices.getAssignmentManager();
    HTableDescriptor htd = this.masterServices.getTableDescriptors().get(Bytes.toString(tableName));
    if (htd == null) {
      throw new IOException("Add Family operation could not be completed as " +
          "HTableDescritor is missing for table = "
          + Bytes.toString(tableName));
    }
    if(!htd.hasFamily(familyName)) {
      throw new InvalidFamilyOperationException(
          "Family '" + Bytes.toString(familyName) + "' does not exist so " +
          "cannot be deleted");
    }
    // Update table descriptor in HDFS
    htd = this.masterServices.getMasterFileSystem()
        .deleteColumn(tableName, familyName);
    // Update in-memory descriptor cache
    this.masterServices.getTableDescriptors().add(htd);
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    String family = "UnknownFamily";
    if(familyName != null) {
      family = Bytes.toString(familyName);
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" + tableNameStr + "-" + family;
  }
}
