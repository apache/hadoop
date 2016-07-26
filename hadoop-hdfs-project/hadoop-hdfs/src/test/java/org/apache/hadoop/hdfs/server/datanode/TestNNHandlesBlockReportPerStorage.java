/**
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

package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.util.Time;


/**
 * Runs all tests in BlockReportTestBase, sending one block per storage.
 * This is the default DataNode behavior post HDFS-2832.
 */
public class TestNNHandlesBlockReportPerStorage extends BlockReportTestBase {

  @Override
  protected void sendBlockReports(DatanodeRegistration dnR, String poolId,
      StorageBlockReport[] reports) throws IOException {
    for (int r = 0; r < reports.length; r++) {
      LOG.info("Sending block report for storage " +
          reports[r].getStorage().getStorageID());
      StorageBlockReport[] singletonReport = {reports[r]};
      if (r != reports.length - 1) {
        cluster.getNameNodeRpc().blockReport(dnR, poolId, singletonReport,
            new BlockReportContext(reports.length, r, System.nanoTime(),
                0L));
      } else {
        StorageBlockReport[] lastSplitReport =
            new StorageBlockReport[reports.length];
        // When block reports are split, send a dummy storage report for all
        // other storages in the blockreport along with the last storage report
        for (int i = 0; i <= r; i++) {
          if (i == r) {
            lastSplitReport[i] = reports[r];
            break;
          }
          lastSplitReport[i] =
              new StorageBlockReport(reports[i].getStorage(),
                  BlockListAsLongs.STORAGE_REPORT);
        }
        cluster.getNameNodeRpc().blockReport(dnR, poolId, lastSplitReport,
            new BlockReportContext(reports.length, r, System.nanoTime(),
                0L));
      }
    }
  }
}
