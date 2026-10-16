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

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;

import static org.assertj.core.api.Assumptions.assumeThat;

public class TestParallelShortCircuitReadNoChecksum extends TestParallelReadUtil {
  private static TemporarySocketDirectory sockDir;

  @BeforeAll
  static public void setupCluster() throws Exception {
    if (DomainSocket.getLoadingFailureReason() != null) return;
    DFSInputStream.tcpReadsDisabledForTesting = true;
    sockDir = new TemporarySocketDirectory();
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
      new File(sockDir.getDir(), "TestParallelLocalRead.%d.sock").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        true);
    DomainSocket.disableBindPathValidation();
    setupCluster(1, conf);
  }

  @BeforeEach
  public void before() {
    assumeThat(DomainSocket.getLoadingFailureReason()).isNull();
  }

  @AfterAll
  static public void teardownCluster() throws Exception {
    if (DomainSocket.getLoadingFailureReason() != null) return;
    sockDir.close();
    TestParallelReadUtil.teardownCluster();
  }
}
