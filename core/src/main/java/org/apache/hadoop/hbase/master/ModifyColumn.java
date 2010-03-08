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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/** Instantiated to modify an existing column family on a table */
class ModifyColumn extends ColumnOperation {
  private final HColumnDescriptor descriptor;
  private final byte [] columnName;
  
  ModifyColumn(final HMaster master, final byte [] tableName, 
    final byte [] columnName, HColumnDescriptor descriptor) 
  throws IOException {
    super(master, tableName);
    this.descriptor = descriptor;
    this.columnName = columnName;
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    for (HRegionInfo i: unservedRegions) {
      if (i.getTableDesc().hasFamily(columnName)) {
        i.getTableDesc().addFamily(descriptor);
        updateRegionInfo(server, m.getRegionName(), i);
      } else { // otherwise, we have an error.
        throw new InvalidColumnNameException("Column family '" +
          Bytes.toString(columnName) + 
          "' doesn't exist, so cannot be modified.");
      }
    }
  }
}
