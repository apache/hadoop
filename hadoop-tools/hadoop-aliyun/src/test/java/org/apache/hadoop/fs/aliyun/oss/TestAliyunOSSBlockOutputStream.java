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

package org.apache.hadoop.fs.aliyun.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.OSSDataBlocks.ByteBufferBlockFactory;
import org.apache.hadoop.fs.aliyun.oss.statistics.BlockOutputStreamStatistics;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;

import static org.apache.hadoop.fs.aliyun.oss.Constants.BUFFER_DIR_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FAST_UPLOAD_BUFFER_ARRAY_DISK;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FAST_UPLOAD_BUFFER_DISK;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FAST_UPLOAD_BUFFER_MEMORY_LIMIT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FAST_UPLOAD_BYTEBUFFER;
import static org.apache.hadoop.fs.aliyun.oss.Constants.FAST_UPLOAD_BYTEBUFFER_DISK;
import static org.apache.hadoop.fs.aliyun.oss.Constants.MULTIPART_UPLOAD_PART_SIZE_DEFAULT;
import static org.apache.hadoop.fs.aliyun.oss.Constants.MULTIPART_UPLOAD_PART_SIZE_KEY;
import static org.apache.hadoop.fs.contract.ContractTestUtils.IO_CHUNK_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests regular and multi-part upload functionality for
 * AliyunOSSBlockOutputStream.
 */
@Timeout(30 * 60)
public class TestAliyunOSSBlockOutputStream {
  private FileSystem fs;
  private static final int PART_SIZE = 1024 * 1024;
  private static String testRootPath =
      AliyunOSSTestUtils.generateUniqueTestPath();
  private static final long MEMORY_LIMIT = 10 * 1024 * 1024;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(MULTIPART_UPLOAD_PART_SIZE_KEY, PART_SIZE);
    conf.setInt(IO_CHUNK_BUFFER_SIZE,
        conf.getInt(MULTIPART_UPLOAD_PART_SIZE_KEY, 0));
    conf.setInt(Constants.UPLOAD_ACTIVE_BLOCKS_KEY, 20);
    conf.setLong(FAST_UPLOAD_BUFFER_MEMORY_LIMIT, MEMORY_LIMIT);
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(testRootPath), true);
    }
  }

  private Path getTestPath() {
    return new Path(testRootPath + "/test-aliyun-oss");
  }

  @Test
  public void testZeroByteUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), 0);
    bufferShouldReleased(true);
  }

  @Test
  public void testRegularUpload() throws IOException {
    FileSystem.clearStatistics();
    long size = 1024 * 1024;
    FileSystem.Statistics statistics =
        FileSystem.getStatistics("oss", AliyunOSSFileSystem.class);
    // This test is a little complicated for statistics, lifecycle is
    // generateTestFile
    //   fs.create(getFileStatus)    read 1
    //   output stream write         write 1
    // path exists(fs.exists)        read 1
    // verifyReceivedData
    //   fs.open(getFileStatus)      read 1
    //   input stream read           read 2(part size is 512K)
    // fs.delete
    //   getFileStatus & delete & exists & create fake dir read 2, write 2
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size - 1);
    assertEquals(7, statistics.getReadOps());
    assertEquals(size - 1, statistics.getBytesRead());
    assertEquals(3, statistics.getWriteOps());
    assertEquals(size - 1, statistics.getBytesWritten());
    bufferShouldReleased();

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    assertEquals(14, statistics.getReadOps());
    assertEquals(2 * size - 1, statistics.getBytesRead());
    assertEquals(6, statistics.getWriteOps());
    assertEquals(2 * size - 1, statistics.getBytesWritten());
    bufferShouldReleased();

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size + 1);
    assertEquals(22, statistics.getReadOps());
    assertEquals(3 * size, statistics.getBytesRead());
    assertEquals(10, statistics.getWriteOps());
    assertEquals(3 * size, statistics.getBytesWritten());
    bufferShouldReleased();
  }

  @Test
  public void testMultiPartUpload() throws IOException {
    long size = 6 * 1024 * 1024;
    FileSystem.clearStatistics();
    FileSystem.Statistics statistics =
        FileSystem.getStatistics("oss", AliyunOSSFileSystem.class);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size - 1);
    assertEquals(17, statistics.getReadOps());
    assertEquals(size - 1, statistics.getBytesRead());
    assertEquals(8, statistics.getWriteOps());
    assertEquals(size - 1, statistics.getBytesWritten());
    bufferShouldReleased();

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    assertEquals(34, statistics.getReadOps());
    assertEquals(2 * size - 1, statistics.getBytesRead());
    assertEquals(16, statistics.getWriteOps());
    assertEquals(2 * size - 1, statistics.getBytesWritten());
    bufferShouldReleased();

    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size + 1);
    assertEquals(52, statistics.getReadOps());
    assertEquals(3 * size, statistics.getBytesRead());
    assertEquals(25, statistics.getWriteOps());
    assertEquals(3 * size, statistics.getBytesWritten());
    bufferShouldReleased();
  }

  @Test
  public void testMultiPartUploadConcurrent() throws IOException {
    FileSystem.clearStatistics();
    long size = 50 * 1024 * 1024 - 1;
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    FileSystem.Statistics statistics =
        FileSystem.getStatistics("oss", AliyunOSSFileSystem.class);
    assertEquals(105, statistics.getReadOps());
    assertEquals(size, statistics.getBytesRead());
    assertEquals(52, statistics.getWriteOps());
    assertEquals(size, statistics.getBytesWritten());
    bufferShouldReleased();
  }

  @Test
  public void testHugeUpload() throws IOException {
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), PART_SIZE - 1);
    bufferShouldReleased();
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), PART_SIZE);
    bufferShouldReleased();
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(),
        MULTIPART_UPLOAD_PART_SIZE_DEFAULT + 1);
    bufferShouldReleased();
  }

  @Test
  public void testMultiPartUploadLimit() throws IOException {
    long partSize1 = AliyunOSSUtils.calculatePartSize(10 * 1024, 100 * 1024);
    assert(10 * 1024 / partSize1 < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);

    long partSize2 = AliyunOSSUtils.calculatePartSize(200 * 1024, 100 * 1024);
    assert(200 * 1024 / partSize2 < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);

    long partSize3 = AliyunOSSUtils.calculatePartSize(10000 * 100 * 1024,
        100 * 1024);
    assert(10000 * 100 * 1024 / partSize3
        < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);

    long partSize4 = AliyunOSSUtils.calculatePartSize(10001 * 100 * 1024,
        100 * 1024);
    assert(10001 * 100 * 1024 / partSize4
        < Constants.MULTIPART_UPLOAD_PART_NUM_LIMIT);
  }

  @Test
  /**
   * This test is used to verify HADOOP-16306.
   * Test small file uploading so that oss fs will upload file directly
   * instead of multi part upload.
   */
  public void testSmallUpload() throws IOException {
    long size = fs.getConf().getInt(MULTIPART_UPLOAD_PART_SIZE_KEY, 1024);
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size - 1);
    bufferShouldReleased();
  }

  private void bufferShouldReleased() throws IOException {
    bufferShouldReleased(false);
  }

  private void bufferShouldReleased(boolean zeroSizeFile) throws IOException {
    String bufferDir = fs.getConf().get(BUFFER_DIR_KEY);
    String bufferType = fs.getConf().get(FAST_UPLOAD_BUFFER);
    if (bufferType.equals(FAST_UPLOAD_BUFFER_DISK)) {
      assertNotNull(bufferDir);
      Path bufferPath = new Path(fs.getConf().get(BUFFER_DIR_KEY));
      FileStatus[] files = bufferPath.getFileSystem(
          fs.getConf()).listStatus(bufferPath);
      // Temporary file should be deleted
      assertEquals(0, files.length);
    } else {
      if (bufferType.equals(FAST_UPLOAD_BYTEBUFFER)) {
        OSSDataBlocks.ByteBufferBlockFactory
            blockFactory = (OSSDataBlocks.ByteBufferBlockFactory)
                ((AliyunOSSFileSystem)fs).getBlockFactory();
        assertEquals(0, blockFactory.getOutstandingBufferCount(),
            "outstanding buffers in " + blockFactory);
      }
    }
    BlockOutputStreamStatistics statistics =
        ((AliyunOSSFileSystem)fs).getBlockOutputStreamStatistics();
    assertEquals(statistics.getBlocksAllocated(),
        statistics.getBlocksReleased());
    if (zeroSizeFile) {
      assertEquals(statistics.getBlocksAllocated(), 0);
    } else {
      assertTrue(statistics.getBlocksAllocated() >= 1);
    }
    assertEquals(statistics.getBytesReleased(),
        statistics.getBytesAllocated());
  }

  @Test
  public void testDirectoryAllocator() throws Throwable {
    Configuration conf = fs.getConf();
    File tmp = AliyunOSSUtils.createTmpFileForWrite("out-", 1024, conf);
    assertTrue(tmp.exists(), "not found: " + tmp);
    tmp.delete();

    // tmp should not in DeleteOnExitHook
    try {
      Class<?> c = Class.forName("java.io.DeleteOnExitHook");
      Field field = c.getDeclaredField("files");
      field.setAccessible(true);
      String name = field.getName();
      LinkedHashSet<String> files = (LinkedHashSet<String>)field.get(name);
      assertTrue(files.isEmpty(), "in DeleteOnExitHook");
      assertFalse(
          (new ArrayList<>(files)).contains(tmp.getPath()), "in DeleteOnExitHook");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDirectoryAllocatorRR() throws Throwable {
    File dir1 = GenericTestUtils.getRandomizedTestDir();
    File dir2 = GenericTestUtils.getRandomizedTestDir();
    dir1.mkdirs();
    dir2.mkdirs();

    Configuration conf = new Configuration();
    conf.set(BUFFER_DIR_KEY, dir1 + ", " + dir2);
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
    File tmp1 = AliyunOSSUtils.createTmpFileForWrite("out-", 1024, conf);
    tmp1.delete();
    File tmp2 = AliyunOSSUtils.createTmpFileForWrite("out-", 1024, conf);
    tmp2.delete();
    assertNotEquals(tmp1.getParent(), tmp2.getParent(),
        "round robin not working");
  }

  @Test
  public void testByteBufferIO() throws IOException {
    try (OSSDataBlocks.ByteBufferBlockFactory factory =
         new OSSDataBlocks.ByteBufferBlockFactory((AliyunOSSFileSystem)fs)) {
      int limit = 128;
      OSSDataBlocks.ByteBufferBlockFactory.ByteBufferBlock block
          = factory.create(1, limit, null);
      assertEquals(1, factory.getOutstandingBufferCount(),
          "outstanding buffers in " + factory);

      byte[] buffer = ContractTestUtils.toAsciiByteArray("test data");
      int bufferLen = buffer.length;
      block.write(buffer, 0, bufferLen);
      assertEquals(bufferLen, block.dataSize());
      assertEquals(limit - bufferLen, block.remainingCapacity(),
          "capacity in " + block);
      assertTrue(block.hasCapacity(64), "hasCapacity(64) in " + block);
      assertTrue(block.hasCapacity(limit - bufferLen), "No capacity in " + block);

      // now start the write
      OSSDataBlocks.BlockUploadData blockUploadData = block.startUpload();
      ByteBufferBlockFactory.ByteBufferBlock.ByteBufferInputStream
          stream =
          (ByteBufferBlockFactory.ByteBufferBlock.ByteBufferInputStream)
              blockUploadData.getUploadStream();
      assertTrue(stream.markSupported(), "Mark not supported in " + stream);
      assertTrue(stream.hasRemaining(), "!hasRemaining() in " + stream);

      int expected = bufferLen;
      assertEquals(expected, stream.available(),
          "wrong available() in " + stream);

      assertEquals('t', stream.read());
      stream.mark(limit);
      expected--;
      assertEquals(expected, stream.available(),
          "wrong available() in " + stream);

      // read into a byte array with an offset
      int offset = 5;
      byte[] in = new byte[limit];
      assertEquals(2, stream.read(in, offset, 2));
      assertEquals('e', in[offset]);
      assertEquals('s', in[offset + 1]);
      expected -= 2;
      assertEquals(expected, stream.available(),
          "wrong available() in " + stream);

      // read to end
      byte[] remainder = new byte[limit];
      int c;
      int index = 0;
      while ((c = stream.read()) >= 0) {
        remainder[index++] = (byte) c;
      }
      assertEquals(expected, index);
      assertEquals('a', remainder[--index]);

      assertEquals(0, stream.available(), "wrong available() in " + stream);
      assertTrue(!stream.hasRemaining(), "hasRemaining() in " + stream);

      // go the mark point
      stream.reset();
      assertEquals('e', stream.read());

      // when the stream is closed, the data should be returned
      stream.close();
      assertEquals(1, factory.getOutstandingBufferCount(),
          "outstanding buffers in " + factory);
      block.close();
      assertEquals(0, factory.getOutstandingBufferCount(),
          "outstanding buffers in " + factory);
      stream.close();
      assertEquals(0, factory.getOutstandingBufferCount(),
          "outstanding buffers in " + factory);
    }
  }

  @Test
  public void testFastUploadArrayDisk() throws IOException {
    testFastUploadFallback(FAST_UPLOAD_BUFFER_ARRAY_DISK);
  }

  @Test
  public void testFastUploadByteBufferDisk() throws IOException {
    testFastUploadFallback(FAST_UPLOAD_BYTEBUFFER_DISK);
  }

  private void testFastUploadFallback(String name) throws IOException {
    Configuration conf = fs.getConf();
    fs.close();

    conf.set(FAST_UPLOAD_BUFFER, name);

    fs = AliyunOSSTestUtils.createTestFileSystem(conf);
    long size = 5 * MEMORY_LIMIT;
    ContractTestUtils.createAndVerifyFile(fs, getTestPath(), size);
    OSSDataBlocks.MemoryBlockFactory
        blockFactory = ((OSSDataBlocks.MemoryAndDiskBlockFactory)
        ((AliyunOSSFileSystem)fs).getBlockFactory()).getMemoryFactory();
    assertEquals(blockFactory.getMemoryUsed(), 0);

    Path bufferPath = new Path(fs.getConf().get(BUFFER_DIR_KEY));
    FileStatus[] files = bufferPath.getFileSystem(
        fs.getConf()).listStatus(bufferPath);
    // Temporary file should be deleted
    assertEquals(0, files.length);

    BlockOutputStreamStatistics statistics =
        ((AliyunOSSFileSystem)fs).getBlockOutputStreamStatistics();
    assertEquals(statistics.getBlocksAllocated(),
        statistics.getBlocksReleased());
    assertTrue(statistics.getBlocksAllocated() > 1);
    assertEquals(statistics.getBytesReleased(),
        statistics.getBytesAllocated());
    assertTrue(statistics.getBytesAllocated() >= MEMORY_LIMIT);
    assertTrue(statistics.getDiskBlocksAllocated() > 0);
    assertEquals(statistics.getDiskBlocksAllocated(),
        statistics.getDiskBlocksReleased());
  }
}
