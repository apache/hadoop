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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.Store;

import java.io.IOException;

/** Instantiated to remove a column family from a table */
class DeleteColumn extends ColumnOperation {
  private final byte [] columnName;

  DeleteColumn(final HMaster master, final byte [] tableName,
    final byte [] columnName)
  throws IOException {
    super(master, tableName);
    this.columnName = columnName;
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    for (HRegionInfo i: unservedRegions) {
      i.getTableDesc().removeFamily(columnName);
      updateRegionInfo(server, m.getRegionName(), i);
      // Delete the directories used by the column
      Path tabledir =
        new Path(this.master.getRootDir(), i.getTableDesc().getNameAsString());
      this.master.getFileSystem().
        delete(Store.getStoreHomedir(tabledir, i.getEncodedName(),
        this.columnName), true);
    }
  }
}