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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/** Instantiated to modify table descriptor metadata */
class ModifyTableMeta extends TableOperation {

  private static Log LOG = LogFactory.getLog(ModifyTableMeta.class);

  private HTableDescriptor desc;

  ModifyTableMeta(final HMaster master, final byte [] tableName, 
    HTableDescriptor desc) 
  throws IOException {
    super(master, tableName);
    this.desc = desc;
    LOG.debug("modifying " + Bytes.toString(tableName) + ": " +
        desc.toString());
  }

  protected void updateRegionInfo(HRegionInterface server, byte [] regionName,
    HRegionInfo i)
  throws IOException {
    BatchUpdate b = new BatchUpdate(i.getRegionName());
    b.put(COL_REGIONINFO, Writables.getBytes(i));
    server.batchUpdate(regionName, b, -1L);
    LOG.debug("updated HTableDescriptor for region " + i.getRegionNameAsString());
  }

  @Override
  protected void processScanItem(String serverName,
      final HRegionInfo info) throws IOException {
    if (isEnabled(info)) {
      throw new TableNotDisabledException(Bytes.toString(tableName));
    }
  }

  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
  throws IOException {
    for (HRegionInfo i: unservedRegions) {
      i.setTableDesc(desc);
      updateRegionInfo(server, m.getRegionName(), i);
    }
    // kick off a meta scan right away
    master.regionManager.metaScannerThread.interrupt();
  }
}
