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

import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDFSStripedInputStreamReadFailures {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestDFSStripedInputStreamReadFailures.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private ErasureCodingPolicy ecPolicy;
  private short dataBlocks;
  private short parityBlocks;
  private int cellSize;
  private final int stripesPerBlock = 2;
  private int blockSize;

  @TempDir
  private java.nio.file.Path baseDir;

  @BeforeEach
  public void setup() throws IOException {
    ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    blockSize = stripesPerBlock * cellSize;

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }

    cluster = new MiniDFSCluster.Builder(conf, baseDir.toFile()).numDataNodes(
        dataBlocks + parityBlocks).build();
    cluster.waitActive();
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
    }
    fs = cluster.getFileSystem();
    DFSTestUtil.enableAllECPolicies(fs);
    fs.getClient()
        .setErasureCodingPolicy("/", ecPolicy.getName());
  }

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private Path writeFile(String name, byte[] bytes) throws Exception {
    Path path = new Path(name);

    DFSTestUtil.writeFile(fs, path, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, name);

    StripedFileTestUtil.checkData(fs, path, bytes.length,
        new ArrayList<DatanodeInfo>(), null, blockSize * dataBlocks);

    return path;
  }

  @Test
  public void testReadWithXceiverExhaustion() throws Exception {

    // Write a little more than 1 stripe size
    // worth of data to 10 files
    int numBytes = cellSize * dataBlocks + 123;
    int numFiles = 10;

    byte[] content = StripedFileTestUtil.generateBytes(numBytes);
    Path[] files = new Path[numFiles];
    for (int i = 0; i < numFiles; i++) {
      files[i] = writeFile("/file_"+ i, content);
    }

    // reconfigure DNs with xceivers set to 2
    for (DataNode dn : cluster.getDataNodes()) {
      dn.reconfigureProperty(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, "2");
    }
    boolean reconfigurationComplete = false;
    while (!reconfigurationComplete) {
      Thread.sleep(100);
      for (DataNode dn : cluster.getDataNodes()) {
        if (dn.getXceiverCount() != 2) {
          break;
        }
      }
      reconfigurationComplete = true;
    }

    // Start a thread for each file that we created
    // and use StripedFileTestUtil.verifyStatefulRead
    // to read from the file.
    final List<Throwable> exceptions = new ArrayList<>();
    final List<Thread> threads = new ArrayList<>(numFiles);
    final CyclicBarrier barrier = new CyclicBarrier(numFiles);
    final CountDownLatch completed = new CountDownLatch(numFiles);
    ThreadGroup testGroup = new ThreadGroup("xceiverTestThreads") {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        exceptions.add(e);
        super.uncaughtException(t, e);
      }
    };
    for (int i = 0; i < numFiles; i++) {
      int fileNum = i;
      threads.add(new Thread(testGroup, () -> {
        byte[] buffer = new byte[numBytes];
        try {
          barrier.await();
          StripedFileTestUtil.verifyStatefulRead(fs, files[fileNum], numBytes, content, buffer);
        } catch (Exception e1) {
          exceptions.add(e1);
        } finally {
          completed.countDown();
        }
      }));
    }
    threads.forEach(t -> t.start());
    completed.await();
    threads.forEach(t -> {
      try {
        t.join();
      } catch (InterruptedException e1) {
        throw new RuntimeException("Interrupted while trying to join thread");
      }
    });
    if (exceptions.size() > 0) {
      LOG.info("{} exceptions occurred", exceptions.size());
      exceptions.forEach(t -> {
        LOG.error("Exception details:", t);
        if (!(t instanceof IOException &&
            t.getMessage().contains("missing blocks, the stripe is"))) {
          fail("Unexpected exceptions occurred during test", t);
        }
      });
    }
  }
}
