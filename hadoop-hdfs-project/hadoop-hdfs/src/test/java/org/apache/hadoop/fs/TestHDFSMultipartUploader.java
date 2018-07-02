/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;

public class TestHDFSMultipartUploader
    extends AbstractSystemMultipartUploaderTest {

  private static MiniDFSCluster cluster;
  private Path tmp;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void init() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf,
          GenericTestUtils.getRandomizedTestDir())
        .numDataNodes(1)
        .build();
    cluster.waitClusterUp();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void setup() throws IOException {
    tmp = new Path(cluster.getFileSystem().getWorkingDirectory(),
        name.getMethodName());
    cluster.getFileSystem().mkdirs(tmp);
  }

  @Override
  public FileSystem getFS() throws IOException {
    return cluster.getFileSystem();
  }

  @Override
  public Path getBaseTestPath() {
    return tmp;
  }

}
