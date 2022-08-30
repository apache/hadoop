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
package org.apache.hadoop.hdfs.protocol.datatransfer.sasl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.HedgedRead;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Retry;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestSaslDataTransferExpiredBlockToken extends SaslDataTransferTestCase {
  private static final int BLOCK_SIZE = 4096;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private static final Path PATH = new Path("/file1");

  private final byte[] rawData = new byte[FILE_SIZE];
  private MiniDFSCluster cluster;

  @Rule
  public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

  @Before
  public void before() throws Exception {
    Random r = new Random();
    r.nextBytes(rawData);

    HdfsConfiguration conf = createSecureConfig("authentication,integrity,privacy");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    try (FileSystem fs = cluster.getFileSystem()) {
      createFile(fs);
    }

    // set a short token lifetime (1 second) initially
    SecurityTestUtil.setBlockTokenLifetime(
        cluster.getNameNode().getNamesystem().getBlockManager().getBlockTokenSecretManager(),
        1000L);
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void createFile(FileSystem fs) throws IOException {
    try (FSDataOutputStream out = fs.create(PATH)) {
      out.write(rawData);
    }
  }

  // read a file using blockSeekTo()
  private boolean checkFile1(FSDataInputStream in) {
    byte[] toRead = new byte[FILE_SIZE];
    int totalRead = 0;
    int nRead = 0;
    try {
      while ((nRead = in.read(toRead, totalRead, toRead.length - totalRead)) > 0) {
        totalRead += nRead;
      }
    } catch (IOException e) {
      return false;
    }
    assertEquals("Cannot read file.", toRead.length, totalRead);
    return checkFile(toRead);
  }

  // read a file using fetchBlockByteRange()/hedgedFetchBlockByteRange()
  private boolean checkFile2(FSDataInputStream in) {
    byte[] toRead = new byte[FILE_SIZE];
    try {
      assertEquals("Cannot read file", toRead.length, in.read(0, toRead, 0, toRead.length));
    } catch (IOException e) {
      return false;
    }
    return checkFile(toRead);
  }

  private boolean checkFile(byte[] fileToCheck) {
    if (fileToCheck.length != rawData.length) {
      return false;
    }
    for (int i = 0; i < fileToCheck.length; i++) {
      if (fileToCheck[i] != rawData[i]) {
        return false;
      }
    }
    return true;
  }

  private FileSystem newFileSystem() throws IOException {
    Configuration clientConf = new Configuration(cluster.getConfiguration(0));

    clientConf.setInt(Retry.WINDOW_BASE_KEY, Integer.MAX_VALUE);

    return FileSystem.newInstance(cluster.getURI(), clientConf);
  }

  private FileSystem newFileSystemHedgedRead() throws IOException {
    Configuration clientConf = new Configuration(cluster.getConfiguration(0));

    clientConf.setInt(Retry.WINDOW_BASE_KEY, 3000);
    clientConf.setInt(HedgedRead.THREADPOOL_SIZE_KEY, 5);

    return FileSystem.newInstance(cluster.getURI(), clientConf);
  }

  @Test
  public void testBlockSeekToWithExpiredToken() throws Exception {
    // read using blockSeekTo(). Acquired tokens are cached in in
    try (FileSystem fs = newFileSystem(); FSDataInputStream in = fs.open(PATH)) {
      waitBlockTokenExpired(in);
      assertTrue(checkFile1(in));
    }
  }

  @Test
  public void testFetchBlockByteRangeWithExpiredToken() throws Exception {
    // read using fetchBlockByteRange(). Acquired tokens are cached in in
    try (FileSystem fs = newFileSystem(); FSDataInputStream in = fs.open(PATH)) {
      waitBlockTokenExpired(in);
      assertTrue(checkFile2(in));
    }
  }

  @Test
  public void testHedgedFetchBlockByteRangeWithExpiredToken() throws Exception {
    // read using hedgedFetchBlockByteRange(). Acquired tokens are cached in in
    try (FileSystem fs = newFileSystemHedgedRead(); FSDataInputStream in = fs.open(PATH)) {
      waitBlockTokenExpired(in);
      assertTrue(checkFile2(in));
    }
  }

  private void waitBlockTokenExpired(FSDataInputStream in1) throws Exception {
    DFSInputStream innerStream = (DFSInputStream) in1.getWrappedStream();
    for (LocatedBlock block : innerStream.getAllBlocks()) {
      while (!SecurityTestUtil.isBlockTokenExpired(block.getBlockToken())) {
        Thread.sleep(100);
      }
    }
  }
}
