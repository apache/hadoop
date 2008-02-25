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

import java.util.Map;
import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.io.Text;

/** Instantiated to modify an existing column family on a table */
class ModifyColumn extends ColumnOperation {
  private final HColumnDescriptor descriptor;
  private final Text columnName;
  
  ModifyColumn(final HMaster master, final Text tableName, 
    final Text columnName, HColumnDescriptor descriptor) 
  throws IOException {
    super(master, tableName);
    this.descriptor = descriptor;
    this.columnName = columnName;
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    for (HRegionInfo i: unservedRegions) {
      // get the column families map from the table descriptor
      Map<Text, HColumnDescriptor> families = i.getTableDesc().families();
      
      // if the table already has this column, then put the new descriptor 
      // version.
      if (families.get(columnName) != null){
        families.put(columnName, descriptor);
        updateRegionInfo(server, m.getRegionName(), i);          
      }
      else{ // otherwise, we have an error.
        throw new IOException("Column family '" + columnName + 
          "' doesn't exist, so cannot be modified.");
      }
    }
  }
}
