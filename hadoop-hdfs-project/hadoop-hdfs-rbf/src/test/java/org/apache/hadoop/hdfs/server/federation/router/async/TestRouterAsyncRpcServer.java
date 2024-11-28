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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Used to test the async functionality of {@link RouterRpcServer}.
 */
public class TestRouterAsyncRpcServer extends RouterAsyncProtocolTestBase {
  private RouterRpcServer asyncRouterRpcServer;

  @Before
  public void setup() throws IOException {
    asyncRouterRpcServer = getRouterAsyncRpcServer();
  }

  /**
   * Test that the async RPC server can invoke a method at an available Namenode.
   */
  @Test
  public void testInvokeAtAvailableNsAsync() throws Exception {
    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    asyncRouterRpcServer.invokeAtAvailableNsAsync(method, BlockStoragePolicy[].class);
    BlockStoragePolicy[] storagePolicies = syncReturn(BlockStoragePolicy[].class);
    assertEquals(8, storagePolicies.length);
  }

  /**
   * Test get create location async.
   */
  @Test
  public void testGetCreateLocationAsync() throws Exception {
    final List<RemoteLocation> locations =
        asyncRouterRpcServer.getLocationsForPath("/testdir", true);
    asyncRouterRpcServer.getCreateLocationAsync("/testdir", locations);
    RemoteLocation remoteLocation = syncReturn(RemoteLocation.class);
    assertNotNull(remoteLocation);
    assertEquals(getNs0(), remoteLocation.getNameserviceId());
  }

  /**
   * Test get datanode report async.
   */
  @Test
  public void testGetDatanodeReportAsync() throws Exception {
    asyncRouterRpcServer.getDatanodeReportAsync(
        HdfsConstants.DatanodeReportType.ALL, true, 0);
    DatanodeInfo[] datanodeInfos = syncReturn(DatanodeInfo[].class);
    assertEquals(3, datanodeInfos.length);

    // Get the namespace where the datanode is located
    asyncRouterRpcServer.getDatanodeStorageReportMapAsync(HdfsConstants.DatanodeReportType.ALL);
    Map<String, DatanodeStorageReport[]> map = syncReturn(Map.class);
    assertEquals(1, map.size());
    assertEquals(3, map.get(getNs0()).length);

    DatanodeInfo[] slowDatanodeReport1 =
        asyncRouterRpcServer.getSlowDatanodeReport(true, 0);

    asyncRouterRpcServer.getSlowDatanodeReportAsync(true, 0);
    DatanodeInfo[] slowDatanodeReport2 = syncReturn(DatanodeInfo[].class);
    assertEquals(slowDatanodeReport1, slowDatanodeReport2);
  }
}
