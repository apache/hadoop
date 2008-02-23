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
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegion;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionInterface;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.Text;

/** 
 * Instantiated to delete a table
 * Note that it extends ChangeTableState, which takes care of disabling
 * the table.
 */
class TableDelete extends ChangeTableState {

  TableDelete(final HMaster master, final Text tableName) throws IOException {
    super(master, tableName, false);
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    // For regions that are being served, mark them for deletion          
    for (HashSet<HRegionInfo> s: servedRegions.values()) {
      for (HRegionInfo i: s) {
        master.regionManager.markRegionForDeletion(i.getRegionName());
      }
    }

    // Unserved regions we can delete now
    for (HRegionInfo i: unservedRegions) {
      // Delete the region
      try {
        HRegion.deleteRegion(this.master.fs, this.master.rootdir, i);
      
      } catch (IOException e) {
        LOG.error("failed to delete region " + i.getRegionName(),
          RemoteExceptionHandler.checkIOException(e));
      }
    }
    super.postProcessMeta(m, server);
    
    // delete the table's folder from fs.
    master.fs.delete(new Path(master.rootdir, tableName.toString()));
  }

  @Override
  protected void updateRegionInfo(BatchUpdate b,
    @SuppressWarnings("unused") HRegionInfo info) {
    for (int i = 0; i < ALL_META_COLUMNS.length; i++) {
      // Be sure to clean all cells
      b.delete(ALL_META_COLUMNS[i]);
    }
  }
}