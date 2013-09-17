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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestParallelRead extends TestParallelReadUtil {
  @BeforeClass
  static public void setupCluster() throws Exception {
    // This is a test of the normal (TCP) read path.  For this reason, we turn
    // off both short-circuit local reads and UNIX domain socket data traffic.
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, false);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
                    false);
    // dfs.domain.socket.path should be ignored because the previous two keys
    // were set to false.  This is a regression test for HDFS-4473.
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "/will/not/be/created");

    setupCluster(DEFAULT_REPLICATION_FACTOR, conf);
  }

  @AfterClass
  static public void teardownCluster() throws Exception {
    TestParallelReadUtil.teardownCluster();
  }
}
