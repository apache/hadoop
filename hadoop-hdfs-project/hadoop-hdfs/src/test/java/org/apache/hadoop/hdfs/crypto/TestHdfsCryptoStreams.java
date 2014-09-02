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
package org.apache.hadoop.hdfs.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoStreamsTestBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.crypto.CryptoFSDataInputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataOutputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestHdfsCryptoStreams extends CryptoStreamsTestBase {
  private static MiniDFSCluster dfsCluster;
  private static FileSystem fs;
  private static int pathCount = 0;
  private static Path path;
  private static Path file;

  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = new HdfsConfiguration();
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    dfsCluster.waitClusterUp();
    fs = dfsCluster.getFileSystem();
    codec = CryptoCodec.getInstance(conf);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Before
  @Override
  public void setUp() throws IOException {
    ++pathCount;
    path = new Path("/p" + pathCount);
    file = new Path(path, "file");
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short) 0700));

    super.setUp();
  }

  @After
  public void cleanUp() throws IOException {
    fs.delete(path, true);
  }

  @Override
  protected OutputStream getOutputStream(int bufferSize, byte[] key, byte[] iv)
      throws IOException {
    return new CryptoFSDataOutputStream(fs.create(file), codec, bufferSize,
        key, iv);
  }

  @Override
  protected InputStream getInputStream(int bufferSize, byte[] key, byte[] iv)
      throws IOException {
    return new CryptoFSDataInputStream(fs.open(file), codec, bufferSize, key,
        iv);
  }
}
