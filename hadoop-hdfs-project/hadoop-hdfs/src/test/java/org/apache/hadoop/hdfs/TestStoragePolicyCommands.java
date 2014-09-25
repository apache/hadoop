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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test storage policy related DFSAdmin commands
 */
public class TestStoragePolicyCommands {
  private static final short REPL = 1;
  private static final int SIZE = 128;

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  
  @Before
  public void clusterSetUp() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void clusterShutdown() throws IOException{
    if(fs != null){
      fs.close();
    }
    if(cluster != null){
      cluster.shutdown();
    }
  }

  @Test
  public void testSetAndGetStoragePolicy() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(fs, bar, SIZE, REPL, 0);

    DFSTestUtil.DFSAdminRun("-getStoragePolicy /foo", 0,
        "The storage policy of " + foo.toString() + " is unspecified", conf);
    DFSTestUtil.DFSAdminRun("-getStoragePolicy /foo/bar", 0,
        "The storage policy of " + bar.toString() + " is unspecified", conf);

    DFSTestUtil.DFSAdminRun("-setStoragePolicy /foo WARM", 0,
        "Set storage policy WARM on " + foo.toString(), conf);
    DFSTestUtil.DFSAdminRun("-setStoragePolicy /foo/bar COLD", 0,
        "Set storage policy COLD on " + bar.toString(), conf);
    DFSTestUtil.DFSAdminRun("-setStoragePolicy /fooz WARM", -1,
        "File/Directory does not exist: /fooz", conf);

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    final BlockStoragePolicy cold = suite.getPolicy("COLD");
    DFSTestUtil.DFSAdminRun("-getStoragePolicy /foo", 0,
        "The storage policy of " + foo.toString() + ":\n" + warm, conf);
    DFSTestUtil.DFSAdminRun("-getStoragePolicy /foo/bar", 0,
        "The storage policy of " + bar.toString() + ":\n" + cold, conf);
    DFSTestUtil.DFSAdminRun("-getStoragePolicy /fooz", -1,
        "File/Directory does not exist: /fooz", conf);
  }
}
