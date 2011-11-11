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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Handles adding a new family to an existing table.
 */
public class TableModifyFamilyHandler extends TableEventHandler {
  private final HColumnDescriptor familyDesc;

  public TableModifyFamilyHandler(byte[] tableName,
      HColumnDescriptor familyDesc, Server server,
      final MasterServices masterServices) throws IOException {
    super(EventType.C_M_MODIFY_FAMILY, tableName, server, masterServices);
    HTableDescriptor htd = getTableDescriptor();
    hasColumnFamily(htd, familyDesc.getName());
    this.familyDesc = familyDesc;
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions) throws IOException {
    // Update table descriptor in HDFS
    HTableDescriptor htd =
      this.masterServices.getMasterFileSystem().modifyColumn(tableName, familyDesc);
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
    if(familyDesc != null) {
      family = familyDesc.getNameAsString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" + tableNameStr + "-" + family;
  }

}
