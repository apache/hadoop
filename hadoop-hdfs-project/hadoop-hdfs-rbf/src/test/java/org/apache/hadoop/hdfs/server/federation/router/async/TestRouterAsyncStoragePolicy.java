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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Used to test the functionality of {@link RouterAsyncStoragePolicy}.
 */
public class TestRouterAsyncStoragePolicy extends RouterAsyncProtocolTestBase {
  private final String testfilePath = "/testdir/testAsyncStoragePolicy.file";
  private RouterAsyncStoragePolicy asyncStoragePolicy;

  @Before
  public void setup() throws IOException {
    asyncStoragePolicy = new RouterAsyncStoragePolicy(getRouterAsyncRpcServer());
    FSDataOutputStream fsDataOutputStream = getRouterFs().create(
        new Path(testfilePath), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
  }

  @Test
  public void testRouterAsyncStoragePolicy() throws Exception {
    BlockStoragePolicy[] storagePolicies = getCluster().getNamenodes().get(0)
        .getClient().getStoragePolicies();
    asyncStoragePolicy.getStoragePolicies();
    BlockStoragePolicy[] storagePoliciesAsync = syncReturn(BlockStoragePolicy[].class);
    assertArrayEquals(storagePolicies, storagePoliciesAsync);

    asyncStoragePolicy.getStoragePolicy(testfilePath);
    BlockStoragePolicy blockStoragePolicy1 = syncReturn(BlockStoragePolicy.class);

    asyncStoragePolicy.setStoragePolicy(testfilePath, "COLD");
    syncReturn(null);
    asyncStoragePolicy.getStoragePolicy(testfilePath);
    BlockStoragePolicy blockStoragePolicy2 = syncReturn(BlockStoragePolicy.class);
    assertNotEquals(blockStoragePolicy1, blockStoragePolicy2);
    assertEquals("COLD", blockStoragePolicy2.getName());
  }
}