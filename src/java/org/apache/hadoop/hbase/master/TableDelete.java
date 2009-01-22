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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;

/** 
 * Instantiated to delete a table. Table must be offline.
 */
class TableDelete extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());

  TableDelete(final HMaster master, final byte [] tableName) throws IOException {
    super(master, tableName);
  }

  @Override
  protected void processScanItem(String serverName,
      long startCode, final HRegionInfo info) throws IOException {
    
    if (isEnabled(info)) {
      throw new TableNotDisabledException(tableName);
    }
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    for (HRegionInfo i: unservedRegions) {
      // Delete the region
      try {
        HRegion.removeRegionFromMETA(server, m.getRegionName(), i.getRegionName());
        HRegion.deleteRegion(this.master.fs, this.master.rootdir, i);
      
      } catch (IOException e) {
        LOG.error("failed to delete region " + i.getRegionName(),
          RemoteExceptionHandler.checkIOException(e));
      }
    }
    
    // delete the table's folder from fs.
    master.fs.delete(new Path(master.rootdir, tableName.toString()), true);
  }
}
