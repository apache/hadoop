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

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

public class TestExternalBlockReader {
  private static final Logger LOG =
          LoggerFactory.getLogger(TestExternalBlockReader.class);

  private static long SEED = 1234;

  @Test
  public void testMisconfiguredExternalBlockReader() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HdfsClientConfigKeys.REPLICA_ACCESSOR_BUILDER_CLASSES_KEY,
        "org.apache.hadoop.hdfs.NonExistentReplicaAccessorBuilderClass");
    conf.setLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    final int TEST_LENGTH = 2048;
    DistributedFileSystem dfs = cluster.getFileSystem();
    try {
      DFSTestUtil.createFile(dfs, new Path("/a"), TEST_LENGTH, (short)1, SEED);
      FSDataInputStream stream = dfs.open(new Path("/a"));
      byte buf[] = new byte[TEST_LENGTH];
      IOUtils.readFully(stream, buf, 0, TEST_LENGTH);
      byte expected[] = DFSTestUtil.
          calculateFileContentsFromSeed(SEED, TEST_LENGTH);
      Assert.assertArrayEquals(expected, buf);
      stream.close();
    } finally {
      dfs.close();
      cluster.shutdown();
    }
  }

  private static final String SYNTHETIC_BLOCK_READER_TEST_UUID_KEY =
      "synthetic.block.reader.test.uuid.key";

  private static final HashMap<String, LinkedList<SyntheticReplicaAccessor>>
      accessors = new HashMap<String, LinkedList<SyntheticReplicaAccessor>>(1);

  public static class SyntheticReplicaAccessorBuilder
      extends ReplicaAccessorBuilder {
    String fileName;
    long blockId;
    String blockPoolId;
    long genstamp;
    boolean verifyChecksum;
    String clientName;
    boolean allowShortCircuit;
    long visibleLength;
    Configuration conf;

    @Override
    public ReplicaAccessorBuilder setFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setBlock(long blockId, String blockPoolId) {
      this.blockId = blockId;
      this.blockPoolId = blockPoolId;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setGenerationStamp(long genstamp) {
      this.genstamp = genstamp;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setVerifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setClientName(String clientName) {
      this.clientName = clientName;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setAllowShortCircuitReads(boolean allowShortCircuit) {
      this.allowShortCircuit = allowShortCircuit;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setVisibleLength(long visibleLength) {
      this.visibleLength = visibleLength;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setConfiguration(Configuration conf) {
      this.conf = conf;
      return this;
    }

    @Override
    public ReplicaAccessorBuilder setBlockAccessToken(byte[] token) {
      return this;
    }

    @Override
    public ReplicaAccessor build() {
      if (visibleLength < 1024) {
        LOG.info("SyntheticReplicaAccessorFactory returning null for a " +
            "smaller replica with size " + visibleLength); //trace
        return null;
      }
      return new SyntheticReplicaAccessor(this);
    }
  }

  public static class SyntheticReplicaAccessor extends ReplicaAccessor {
    final long length;
    final byte contents[];
    final SyntheticReplicaAccessorBuilder builder;
    long totalRead = 0;
    int numCloses = 0;
    String error = "";
    String prefix = "";
    final long genstamp;

    SyntheticReplicaAccessor(SyntheticReplicaAccessorBuilder builder) {
      this.length = builder.visibleLength;
      this.contents = DFSTestUtil.
          calculateFileContentsFromSeed(SEED, Ints.checkedCast(length));
      this.builder = builder;
      this.genstamp = builder.genstamp;
      String uuid = this.builder.conf.
          get(SYNTHETIC_BLOCK_READER_TEST_UUID_KEY);
      LinkedList<SyntheticReplicaAccessor> accessorsList =
          accessors.get(uuid);
      if (accessorsList == null) {
        accessorsList = new LinkedList<SyntheticReplicaAccessor>();
      }
      accessorsList.add(this);
      accessors.put(uuid, accessorsList);
    }

    @Override
    public synchronized int read(long pos, byte[] buf, int off, int len)
        throws IOException {
      if (pos > Integer.MAX_VALUE) {
        return 0;
      } else if (pos < 0) {
        addError("Attempted to read from a location that was less " +
            "than 0 at " + pos);
        return 0;
      }
      int i = off, nread = 0, ipos;
      for (ipos = (int)pos;
           (ipos < contents.length) && (nread < len);
           ipos++) {
        buf[i++] = contents[ipos];
        nread++;
        totalRead++;
        LOG.info("ipos = " + ipos + ", contents.length = " + contents.length + ", nread = " + nread + ", len = " + len);
      }
      if ((nread == 0) && (ipos >= contents.length)) {
        return -1;
      }
      return nread;
    }

    @Override
    public synchronized int read(long pos, ByteBuffer buf) throws IOException {
      if (pos > Integer.MAX_VALUE) {
        return 0;
      } else if (pos < 0) {
        addError("Attempted to read from a location that was less " +
            "than 0 at " + pos);
        return 0;
      }
      int i = 0, nread = 0, ipos;
      for (ipos = (int)pos;
           ipos < contents.length; ipos++) {
        try {
          buf.put(contents[ipos]);
        } catch (BufferOverflowException bos) {
          break;
        }
        nread++;
        totalRead++;
      }
      if ((nread == 0) && (ipos >= contents.length)) {
        return -1;
      }
      return nread;
    }

    @Override
    public synchronized void close() throws IOException {
      numCloses++;
    }

    @Override
    public boolean isLocal() {
      return true;
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }

    @Override
    public int getNetworkDistance() {
      return 0;
    }

    synchronized String getError() {
      return error;
    }

    long getGenerationStamp() {
      return genstamp;
    }

    synchronized void addError(String text) {
      LOG.error("SyntheticReplicaAccessor error: " + text);
      error = error + prefix + text;
      prefix = "; ";
    }
  }

  @Test
  public void testExternalBlockReader() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HdfsClientConfigKeys.REPLICA_ACCESSOR_BUILDER_CLASSES_KEY,
        SyntheticReplicaAccessorBuilder.class.getName());
    conf.setLong(HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY, 1024);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);
    String uuid = UUID.randomUUID().toString();
    conf.set(SYNTHETIC_BLOCK_READER_TEST_UUID_KEY, uuid);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .hosts(new String[] {NetUtils.getLocalHostname()})
        .build();
    final int TEST_LENGTH = 2047;
    DistributedFileSystem dfs = cluster.getFileSystem();
    try {
      DFSTestUtil.createFile(dfs, new Path("/a"), TEST_LENGTH, (short)1, SEED);
      HdfsDataInputStream stream =
          (HdfsDataInputStream)dfs.open(new Path("/a"));
      byte buf[] = new byte[TEST_LENGTH];
      stream.seek(1000);
      IOUtils.readFully(stream, buf, 1000, TEST_LENGTH - 1000);
      stream.seek(0);
      IOUtils.readFully(stream, buf, 0, 1000);
      byte expected[] = DFSTestUtil.
          calculateFileContentsFromSeed(SEED, TEST_LENGTH);
      ReadStatistics stats = stream.getReadStatistics();
      Assert.assertEquals(1024, stats.getTotalShortCircuitBytesRead());
      Assert.assertEquals(2047, stats.getTotalLocalBytesRead());
      Assert.assertEquals(2047, stats.getTotalBytesRead());
      Assert.assertArrayEquals(expected, buf);
      stream.close();
      ExtendedBlock block = DFSTestUtil.getFirstBlock(dfs, new Path("/a"));
      Assert.assertNotNull(block);
      LinkedList<SyntheticReplicaAccessor> accessorList = accessors.get(uuid);
      Assert.assertNotNull(accessorList);
      Assert.assertEquals(3, accessorList.size());
      SyntheticReplicaAccessor accessor = accessorList.get(0);
      Assert.assertTrue(accessor.builder.allowShortCircuit);
      Assert.assertEquals(block.getBlockPoolId(),
          accessor.builder.blockPoolId);
      Assert.assertEquals(block.getBlockId(),
          accessor.builder.blockId);
      Assert.assertEquals(dfs.getClient().clientName,
          accessor.builder.clientName);
      Assert.assertEquals("/a", accessor.builder.fileName);
      Assert.assertEquals(block.getGenerationStamp(),
          accessor.getGenerationStamp());
      Assert.assertTrue(accessor.builder.verifyChecksum);
      Assert.assertEquals(1024L, accessor.builder.visibleLength);
      Assert.assertEquals(24L, accessor.totalRead);
      Assert.assertEquals("", accessor.getError());
      Assert.assertEquals(1, accessor.numCloses);
      byte[] tempBuf = new byte[5];
      Assert.assertEquals(-1, accessor.read(TEST_LENGTH,
            tempBuf, 0, 0));
      Assert.assertEquals(-1, accessor.read(TEST_LENGTH,
            tempBuf, 0, tempBuf.length));
      accessors.remove(uuid);
    } finally {
      dfs.close();
      cluster.shutdown();
    }
  }
}
