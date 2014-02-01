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
 * Runs all tests in BlockReportTestBase, sending one block report
 * per DataNode. This tests that the NN can handle the legacy DN
 * behavior where it presents itself as a single logical storage.
 */
public class TestNNHandlesCombinedBlockReport extends BlockReportTestBase {

  @Override
  protected void sendBlockReports(DatanodeRegistration dnR, String poolId,
                                  StorageBlockReport[] reports) throws IOException {
    LOG.info("Sending combined block reports for " + dnR);
    cluster.getNameNodeRpc().blockReport(dnR, poolId, reports);
  }
}
