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

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_READ_USE_CACHE_PRIORITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Retry;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDFSInputStream {
  private void testSkipInner(MiniDFSCluster cluster) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    Path file = new Path("/testfile");
    int fileLength = 1 << 22;
    byte[] fileContent = new byte[fileLength];
    for (int i = 0; i < fileLength; i++) {
      fileContent[i] = (byte) (i % 133);
    }
    FSDataOutputStream fout = fs.create(file);
    fout.write(fileContent);
    fout.close();
    Random random = new Random();
    for (int i = 3; i < 18; i++) {
      DFSInputStream fin = client.open("/testfile");
      for (long pos = 0; pos < fileLength;) {
        long skip = random.nextInt(1 << i) + 1;
        long skipped = fin.skip(skip);
        if (pos + skip >= fileLength) {
          assertEquals(fileLength, pos + skipped);
          break;
        } else {
          assertEquals(skip, skipped);
          pos += skipped;
          int data = fin.read();
          assertEquals(pos % 133, data);
          pos += 1;
        }
      }
      fin.close();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testSkipWithRemoteBlockReader() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      testSkipInner(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testSkipWithRemoteBlockReader2() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      testSkipInner(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testSkipWithLocalBlockReader() throws IOException {
    assumeTrue(DomainSocket.getLoadingFailureReason() == null);
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    DomainSocket.disableBindPathValidation();
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(sockDir.getDir(),
          "TestShortCircuitLocalRead._PORT.sock").getAbsolutePath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      DFSInputStream.tcpReadsDisabledForTesting = true;
      testSkipInner(cluster);
    } finally {
      DFSInputStream.tcpReadsDisabledForTesting = false;
      cluster.shutdown();
      sockDir.close();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testSeekToNewSource() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    Path path = new Path("/testfile");
    DFSTestUtil.createFile(fs, path, 1024, (short) 3, 0);
    DFSInputStream fin = fs.dfs.open("/testfile");
    try {
      fin.seekToNewSource(100);
      assertEquals(100, fin.getPos());
      DatanodeInfo firstNode = fin.getCurrentDatanode();
      assertNotNull(firstNode);
      fin.seekToNewSource(100);
      assertEquals(100, fin.getPos());
      assertFalse(firstNode.equals(fin.getCurrentDatanode()));
    } finally {
      fin.close();
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testOpenInfo() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY, 0);
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      int chunkSize = 512;
      Random r = new Random(12345L);
      byte[] data = new byte[chunkSize];
      r.nextBytes(data);

      Path file = new Path("/testfile");
      try(FSDataOutputStream fout = fs.create(file)) {
        fout.write(data);
      }

      DfsClientConf dcconf = new DfsClientConf(conf);
      int retryTimesForGetLastBlockLength =
              dcconf.getRetryTimesForGetLastBlockLength();
      assertEquals(0, retryTimesForGetLastBlockLength);

      try(DFSInputStream fin = fs.dfs.open("/testfile")) {
        long flen = fin.getFileLength();
        assertEquals(chunkSize, flen);

        long lastBlockBeingWrittenLength =
                fin.getlastBlockBeingWrittenLengthForTesting();
        assertEquals(0, lastBlockBeingWrittenLength);
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testNullCheckSumWhenDNRestarted()
      throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(HdfsClientConfigKeys.DFS_CHECKSUM_TYPE_KEY, "NULL");
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    cluster.waitActive();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      int chunkSize = 512;
      Random r = new Random(12345L);
      byte[] data = new byte[chunkSize];
      r.nextBytes(data);

      Path file = new Path("/testfile");
      try (FSDataOutputStream fout = fs.create(file)) {
        fout.write(data);
        fout.hflush();
        cluster.restartDataNode(0, true, true);
      }

      // wait for block to load
      Thread.sleep(1000);

      // fetch live DN
      final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      cluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().fetchDatanodes(live, null, false);
      assertTrue(live.size() == 2,
          "DN start should be success and live dn should be 2");
      assertTrue(fs.getFileStatus(file).getLen() == chunkSize,
          "File size should be " + chunkSize);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadWithPreferredCachingReplica() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_CLIENT_READ_USE_CACHE_PRIORITY, true);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = null;
    Path filePath = new Path("/testReadPreferredCachingReplica");
    try {
      fs = cluster.getFileSystem();
      FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);
      DFSInputStream dfsInputStream =
          (DFSInputStream) fs.open(filePath).getWrappedStream();
      LocatedBlock lb = mock(LocatedBlock.class);
      when(lb.getCachedLocations()).thenReturn(DatanodeInfo.EMPTY_ARRAY);
      DatanodeID nodeId = new DatanodeID("localhost", "localhost", "dn0", 1111,
          1112, 1113, 1114);
      DatanodeInfo dnInfo = new DatanodeDescriptor(nodeId);
      when(lb.getCachedLocations()).thenReturn(new DatanodeInfo[] {dnInfo});
      DatanodeInfo retDNInfo =
          dfsInputStream.getBestNodeDNAddrPair(lb, null).info;
      assertEquals(dnInfo, retDNInfo);
    } finally {
      fs.delete(filePath, true);
      cluster.shutdown();
    }
  }

  @Test
  public void testReadWithoutPreferredCachingReplica() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_CLIENT_READ_USE_CACHE_PRIORITY, false);
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = null;
    Path filePath = new Path("/testReadWithoutPreferredCachingReplica");
    try {
      fs = cluster.getFileSystem();
      FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);
      DFSInputStream dfsInputStream =
              (DFSInputStream) fs.open(filePath).getWrappedStream();
      LocatedBlock lb = mock(LocatedBlock.class);
      when(lb.getCachedLocations()).thenReturn(DatanodeInfo.EMPTY_ARRAY);
      DatanodeID nodeId = new DatanodeID("localhost", "localhost", "dn0", 1111,
              1112, 1113, 1114);
      DatanodeInfo dnInfo = new DatanodeDescriptor(nodeId);
      DatanodeInfoWithStorage dnInfoStorage =
          new DatanodeInfoWithStorage(dnInfo, "DISK", StorageType.DISK);
      when(lb.getLocations()).thenReturn(
          new DatanodeInfoWithStorage[] {dnInfoStorage});
      DatanodeInfo retDNInfo =
              dfsInputStream.getBestNodeDNAddrPair(lb, null).info;
      assertEquals(dnInfo, retDNInfo);
    } finally {
      fs.delete(filePath, true);
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateBlockReaderWhenInvalidBlockTokenException() throws
      IOException, InterruptedException, TimeoutException {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.DEBUG);
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 64 * 1024);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 516);
    DFSClientFaultInjector oldFaultInjector = DFSClientFaultInjector.get();
    FSDataOutputStream out = null;
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      // Create file which only contains one UC block.
      String file = "/testfile";
      Path path = new Path(file);
      out = fs.create(path, (short) 3);
      int bufferLen = 5120;
      byte[] toWrite = new byte[bufferLen];
      Random rb = new Random(0);
      rb.nextBytes(toWrite);
      out.write(toWrite, 0, bufferLen);

      // Wait for the block length of the file to be 1.
      GenericTestUtils.waitFor(() -> {
        try {
          return fs.getFileBlockLocations(path, 0, bufferLen).length == 1;
        } catch (IOException e) {
          return false;
        }
      }, 100, 10000);

      // Set up the InjectionHandler.
      DFSClientFaultInjector.set(Mockito.mock(DFSClientFaultInjector.class));
      DFSClientFaultInjector injector = DFSClientFaultInjector.get();
      final AtomicInteger count = new AtomicInteger(0);
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          // Mock access token was invalid when connecting to first datanode
          // throw InvalidBlockTokenException.
          if (count.getAndIncrement() == 0) {
            throw new InvalidBlockTokenException("Mock InvalidBlockTokenException");
          }
          return null;
        }
      }).when(injector).failCreateBlockReader();

      try (DFSInputStream in = new DFSInputStream(fs.getClient(), file,
          false, null)) {
        int bufLen = 1024;
        byte[] buf = new byte[bufLen];
        // Seek the offset to 1024 and which should be in the range (0, fileSize).
        in.seek(1024);
        int read = in.read(buf, 0, bufLen);
        assertEquals(1024, read);
      }
    } finally {
      DFSClientFaultInjector.set(oldFaultInjector);
      IOUtils.closeStream(out);
    }
  }
}
