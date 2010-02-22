/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;

/** Instantiated to add a column family to a table */
class AddColumn extends ColumnOperation {
  private final HColumnDescriptor newColumn;

  AddColumn(final HMaster master, final byte [] tableName, 
    final HColumnDescriptor newColumn) 
  throws IOException {
    super(master, tableName);
    this.newColumn = newColumn;
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    for (HRegionInfo i: unservedRegions) {
      // All we need to do to add a column is add it to the table descriptor.
      // When the region is brought on-line, it will find the column missing
      // and create it.
      i.getTableDesc().addFamily(newColumn);
      updateRegionInfo(server, m.getRegionName(), i);
    }
  }
}