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

import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;


/**
 * Runs all tests in BlockReportTestBase, sending one block per storage.
 * This is the default DataNode behavior post HDFS-2832.
 */
public class TestNNHandlesBlockReportPerStorage extends BlockReportTestBase {

  @Override
  protected void sendBlockReports(DatanodeRegistration dnR, String poolId,
      StorageBlockReport[] reports) throws IOException {
    for (StorageBlockReport report : reports) {
      LOG.info("Sending block report for storage " + report.getStorage().getStorageID());
      StorageBlockReport[] singletonReport = { report };
      cluster.getNameNodeRpc().blockReport(dnR, poolId, singletonReport);
    }
  }
}
