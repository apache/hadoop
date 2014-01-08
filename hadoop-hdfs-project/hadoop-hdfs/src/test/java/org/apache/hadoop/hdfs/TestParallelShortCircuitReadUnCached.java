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

import java.io.File;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.hamcrest.CoreMatchers.*;

/**
 * This class tests short-circuit local reads without any FileInputStream or
 * Socket caching.  This is a regression test for HDFS-4417.
 */
public class TestParallelShortCircuitReadUnCached extends TestParallelReadUtil {
  private static TemporarySocketDirectory sockDir;

  @BeforeClass
  static public void setupCluster() throws Exception {
    if (DomainSocket.getLoadingFailureReason() != null) return;
    sockDir = new TemporarySocketDirectory();
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), 
        "TestParallelShortCircuitReadUnCached._PORT.sock").getAbsolutePath());
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    // Enabling data transfer encryption should have no effect when using
    // short-circuit local reads.  This is a regression test for HDFS-5353.
    conf.setBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY, false);
    conf.setBoolean(DFSConfigKeys.
        DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, true);
    // We want to test reading from stale sockets.
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY, 1);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
        5 * 60 * 1000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY, 32);
    // Avoid using the FileInputStreamCache.
    conf.setInt(DFSConfigKeys.
        DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY, 0);
    DomainSocket.disableBindPathValidation();
    DFSInputStream.tcpReadsDisabledForTesting = true;
    setupCluster(1, conf);
  }

  @Before
  public void before() {
    Assume.assumeThat(DomainSocket.getLoadingFailureReason(), equalTo(null));
  }

  @AfterClass
  static public void teardownCluster() throws Exception {
    if (DomainSocket.getLoadingFailureReason() != null) return;
    sockDir.close();
    TestParallelReadUtil.teardownCluster();
  }
}