/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.example;

import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;

/**
 * Ensure that we can perform operations against the shaded minicluster
 * given the API and runtime jars by performing some simple smoke tests.
 */
public class ITUseMiniCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITUseMiniCluster.class);

  private MiniDFSCluster cluster;

  private static final String TEST_PATH = "/foo/bar/cats/dee";
  private static final String FILENAME = "test.file";

  private static final String TEXT = "Lorem ipsum dolor sit amet, consectetur "
      + "adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore "
      + "magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation "
      + "ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute "
      + "irure dolor in reprehenderit in voluptate velit esse cillum dolore eu "
      + "fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident,"
      + " sunt in culpa qui officia deserunt mollit anim id est laborum.";

  @Before
  public void clusterUp() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .build();
    cluster.waitActive();
  }

  @After
  public void clusterDown() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  public void useHdfsFileSystem() throws IOException {
    try (FileSystem fs = cluster.getFileSystem()) {
      simpleReadAfterWrite(fs);
    }
  }

  public void simpleReadAfterWrite(final FileSystem fs) throws IOException {
    LOG.info("Testing read-after-write with FS implementation: {}", fs);
    final Path path = new Path(TEST_PATH, FILENAME);
    if (!fs.mkdirs(path.getParent())) {
      throw new IOException("Mkdirs failed to create " +
          TEST_PATH);
    }
    try (FSDataOutputStream out = fs.create(path)) {
      out.writeUTF(TEXT);
    }
    try (FSDataInputStream in = fs.open(path)) {
      final String result = in.readUTF();
      Assert.assertEquals("Didn't read back text we wrote.", TEXT, result);
    }
  }

  @Test
  public void useWebHDFS() throws IOException, URISyntaxException {
    try (FileSystem fs = WebHdfsTestUtil.getWebHdfsFileSystem(
        cluster.getConfiguration(0), WebHdfsConstants.WEBHDFS_SCHEME)) {
      simpleReadAfterWrite(fs);
    }
  }
}
