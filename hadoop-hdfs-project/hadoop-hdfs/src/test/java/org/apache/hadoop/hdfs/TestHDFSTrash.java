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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TestTrash;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test trash using HDFS
 */
public class TestHDFSTrash {
  private static MiniDFSCluster cluster = null;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) { cluster.shutdown(); }
  }

  @Test
  public void testTrash() throws IOException {
    TestTrash.trashShell(cluster.getFileSystem(), new Path("/"));
  }

  @Test
  public void testNonDefaultFS() throws IOException {
    FileSystem fs = cluster.getFileSystem();
    Configuration conf = fs.getConf();
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, fs.getUri().toString());
    TestTrash.trashNonDefaultFS(conf);
  }
}
