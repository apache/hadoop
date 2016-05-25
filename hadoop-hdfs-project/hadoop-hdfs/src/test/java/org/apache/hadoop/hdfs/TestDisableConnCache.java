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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.BlockReaderTestUtil;
import org.junit.Test;

/**
 * This class tests disabling client connection caching in a single node
 * mini-cluster.
 */
public class TestDisableConnCache {
  static final Log LOG = LogFactory.getLog(TestDisableConnCache.class);

  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 3 * BLOCK_SIZE;
  
  /**
   * Test that the socket cache can be disabled by setting the capacity to
   * 0. Regression test for HDFS-3365.
   * @throws Exception 
   */
  @Test
  public void testDisableCache() throws Exception {
    HdfsConfiguration confWithoutCache = new HdfsConfiguration();
    // Configure a new instance with no peer caching, ensure that it doesn't
    // cache anything
    confWithoutCache.setInt(
        HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY, 0);
    BlockReaderTestUtil util = new BlockReaderTestUtil(1, confWithoutCache);
    final Path testFile = new Path("/testConnCache.dat");
    util.writeFile(testFile, FILE_SIZE / 1024);
    FileSystem fsWithoutCache = FileSystem.newInstance(util.getConf());
    try {
      DFSTestUtil.readFile(fsWithoutCache, testFile);
      assertEquals(0, ((DistributedFileSystem)fsWithoutCache).
          dfs.getClientContext().getPeerCache().size());
    } finally {
      fsWithoutCache.close();
      util.shutdown();
    }
  }
}