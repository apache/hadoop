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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Random;

import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.StreamCapabilities;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;

/**
 * Test semantics of functions flush, hflush, hsync, and close for block blobs,
 * block blobs with compaction, and page blobs.
 */
public class ITestOutputStreamSemantics extends AbstractWasbTestBase {

  private static final String PAGE_BLOB_DIR = "/pageblob";
  private static final String BLOCK_BLOB_DIR = "/blockblob";
  private static final String BLOCK_BLOB_COMPACTION_DIR = "/compaction";

  private byte[] getRandomBytes() {
    byte[] buffer = new byte[PageBlobFormatHelpers.PAGE_SIZE
        - PageBlobFormatHelpers.PAGE_HEADER_SIZE];
    Random rand = new Random();
    rand.nextBytes(buffer);
    return buffer;
  }

  private Path getBlobPathWithTestName(String parentDir) {
    return new Path(parentDir + "/" + methodName.getMethodName());
  }

  private void validate(Path path, byte[] writeBuffer, boolean isEqual)
      throws IOException {
    String blobPath = path.toUri().getPath();
    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] readBuffer = new byte[PageBlobFormatHelpers.PAGE_SIZE
          - PageBlobFormatHelpers.PAGE_HEADER_SIZE];
      int numBytesRead = inputStream.read(readBuffer, 0, readBuffer.length);

      if (isEqual) {
        assertArrayEquals(
            String.format("Bytes read do not match bytes written to %1$s",
                blobPath),
            writeBuffer,
            readBuffer);
      } else {
        assertThat(
            String.format("Bytes read unexpectedly match bytes written to %1$s",
                blobPath),
            readBuffer,
            IsNot.not(IsEqual.equalTo(writeBuffer)));
      }
    }
  }

  private boolean isBlockBlobAppendStreamWrapper(FSDataOutputStream stream) {
    return
    ((SyncableDataOutputStream)
        ((NativeAzureFileSystem.NativeAzureFsOutputStream)
            stream.getWrappedStream())
            .getOutStream())
        .getOutStream()
        instanceof  BlockBlobAppendStream;
  }

  private boolean isPageBlobStreamWrapper(FSDataOutputStream stream) {
    return
        ((SyncableDataOutputStream) stream.getWrappedStream())
        .getOutStream()
            instanceof  PageBlobOutputStream;
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();

    // Configure the page blob directories
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, PAGE_BLOB_DIR);

    // Configure the block blob with compaction directories
    conf.set(AzureNativeFileSystemStore.KEY_BLOCK_BLOB_WITH_COMPACTION_DIRECTORIES,
        BLOCK_BLOB_COMPACTION_DIR);

    return AzureBlobStorageTestAccount.create(
        "",
        EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
        conf);
  }

  // Verify flush writes data to storage for Page Blobs
  @Test
  public void testPageBlobFlush() throws IOException {
    Path path = getBlobPathWithTestName(PAGE_BLOB_DIR);

    try (FSDataOutputStream stream = fs.create(path)) {
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      stream.flush();

      // flush is asynchronous for Page Blob, so we need to
      // wait for it to complete
      SyncableDataOutputStream syncStream =
          (SyncableDataOutputStream) stream.getWrappedStream();
      PageBlobOutputStream pageBlobStream =
          (PageBlobOutputStream)syncStream.getOutStream();
      pageBlobStream.waitForLastFlushCompletion();

      validate(path, buffer, true);
    }
  }


  // Verify hflush writes data to storage for Page Blobs
  @Test
  public void testPageBlobHFlush() throws IOException {
    Path path = getBlobPathWithTestName(PAGE_BLOB_DIR);

    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isPageBlobStreamWrapper(stream));
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      stream.hflush();
      validate(path, buffer, true);
    }
  }

  // HSync must write data to storage for Page Blobs
  @Test
  public void testPageBlobHSync() throws IOException {
    Path path = getBlobPathWithTestName(PAGE_BLOB_DIR);

    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isPageBlobStreamWrapper(stream));
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      stream.hsync();
      validate(path, buffer, true);
    }
  }

  // Close must write data to storage for Page Blobs
  @Test
  public void testPageBlobClose() throws IOException {
    Path path = getBlobPathWithTestName(PAGE_BLOB_DIR);

    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isPageBlobStreamWrapper(stream));
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      stream.close();
      validate(path, buffer, true);
    }
  }

  // Page Blobs have StreamCapabilities.HFLUSH and StreamCapabilities.HSYNC.
  @Test
  public void testPageBlobCapabilities() throws IOException {
    Path path = getBlobPathWithTestName(PAGE_BLOB_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(stream.hasCapability(StreamCapabilities.HFLUSH));
      assertTrue(stream.hasCapability(StreamCapabilities.HSYNC));
      assertFalse(stream.hasCapability(StreamCapabilities.DROPBEHIND));
      assertFalse(stream.hasCapability(StreamCapabilities.READAHEAD));
      assertFalse(stream.hasCapability(StreamCapabilities.UNBUFFER));
      stream.write(getRandomBytes());
    }
  }

  // Verify flush does not write data to storage for Block Blobs
  @Test
  public void testBlockBlobFlush() throws Exception {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_DIR);
    byte[] buffer = getRandomBytes();

    try (FSDataOutputStream stream = fs.create(path)) {
      for (int i = 0; i < 10; i++) {
        stream.write(buffer);
        stream.flush();
      }
    }
    String blobPath = path.toUri().getPath();
    // Create a blob reference to read and validate the block list
    CloudBlockBlob blob = testAccount.getBlobReference(blobPath.substring(1));
    // after the stream is closed, the block list should be non-empty
    ArrayList<BlockEntry> blockList = blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        null,null, null);
    assertEquals(1, blockList.size());
  }

  // Verify hflush does not write data to storage for Block Blobs
  @Test
  public void testBlockBlobHFlush() throws Exception {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_DIR);
    byte[] buffer = getRandomBytes();

    try (FSDataOutputStream stream = fs.create(path)) {
      for (int i = 0; i < 10; i++) {
        stream.write(buffer);
        stream.hflush();
      }
    }
    String blobPath = path.toUri().getPath();
    // Create a blob reference to read and validate the block list
    CloudBlockBlob blob = testAccount.getBlobReference(blobPath.substring(1));
    // after the stream is closed, the block list should be non-empty
    ArrayList<BlockEntry> blockList = blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        null,null, null);
    assertEquals(1, blockList.size());
  }

  // Verify hsync does not write data to storage for Block Blobs
  @Test
  public void testBlockBlobHSync() throws Exception {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_DIR);
    byte[] buffer = getRandomBytes();

    try (FSDataOutputStream stream = fs.create(path)) {
      for (int i = 0; i < 10; i++) {
        stream.write(buffer);
        stream.hsync();
      }
    }
    String blobPath = path.toUri().getPath();
    // Create a blob reference to read and validate the block list
    CloudBlockBlob blob = testAccount.getBlobReference(blobPath.substring(1));
    // after the stream is closed, the block list should be non-empty
    ArrayList<BlockEntry> blockList = blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        null,null, null);
    assertEquals(1, blockList.size());
  }

  // Close must write data to storage for Block Blobs
  @Test
  public void testBlockBlobClose() throws IOException {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_DIR);

    try (FSDataOutputStream stream = fs.create(path)) {
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      stream.close();
      validate(path, buffer, true);
    }
  }

  // Block Blobs do not have any StreamCapabilities.
  @Test
  public void testBlockBlobCapabilities() throws IOException {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      assertFalse(stream.hasCapability(StreamCapabilities.HFLUSH));
      assertFalse(stream.hasCapability(StreamCapabilities.HSYNC));
      assertFalse(stream.hasCapability(StreamCapabilities.DROPBEHIND));
      assertFalse(stream.hasCapability(StreamCapabilities.READAHEAD));
      assertFalse(stream.hasCapability(StreamCapabilities.UNBUFFER));
      stream.write(getRandomBytes());
    }
  }

  // Verify flush writes data to storage for Block Blobs with compaction
  @Test
  public void testBlockBlobCompactionFlush() throws Exception {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_COMPACTION_DIR);
    byte[] buffer = getRandomBytes();

    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isBlockBlobAppendStreamWrapper(stream));
      for (int i = 0; i < 10; i++) {
        stream.write(buffer);
        stream.flush();
      }
    }
    String blobPath = path.toUri().getPath();
    // Create a blob reference to read and validate the block list
    CloudBlockBlob blob = testAccount.getBlobReference(blobPath.substring(1));
    // after the stream is closed, the block list should be non-empty
    ArrayList<BlockEntry> blockList = blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        null,null, null);
    assertEquals(1, blockList.size());
  }

  // Verify hflush writes data to storage for Block Blobs with Compaction
  @Test
  public void testBlockBlobCompactionHFlush() throws Exception {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_COMPACTION_DIR);
    byte[] buffer = getRandomBytes();

    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isBlockBlobAppendStreamWrapper(stream));
      for (int i = 0; i < 10; i++) {
        stream.write(buffer);
        stream.hflush();
      }
    }
    String blobPath = path.toUri().getPath();
    // Create a blob reference to read and validate the block list
    CloudBlockBlob blob = testAccount.getBlobReference(blobPath.substring(1));
    // after the stream is closed, the block list should be non-empty
    ArrayList<BlockEntry> blockList = blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        null,null, null);
    assertEquals(10, blockList.size());
  }

  // Verify hsync writes data to storage for Block Blobs with compaction
  @Test
  public void testBlockBlobCompactionHSync() throws Exception {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_COMPACTION_DIR);
    byte[] buffer = getRandomBytes();

    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isBlockBlobAppendStreamWrapper(stream));
      for (int i = 0; i < 10; i++) {
        stream.write(buffer);
        stream.hsync();
      }
    }
    String blobPath = path.toUri().getPath();
    // Create a blob reference to read and validate the block list
    CloudBlockBlob blob = testAccount.getBlobReference(blobPath.substring(1));
    // after the stream is closed, the block list should be non-empty
    ArrayList<BlockEntry> blockList = blob.downloadBlockList(
        BlockListingFilter.COMMITTED,
        null,null, null);
    assertEquals(10, blockList.size());
  }

  // Close must write data to storage for Block Blobs with compaction
  @Test
  public void testBlockBlobCompactionClose() throws IOException {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_COMPACTION_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isBlockBlobAppendStreamWrapper(stream));
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      stream.close();
      validate(path, buffer, true);
    }
  }

  // Block Blobs with Compaction have StreamCapabilities.HFLUSH and HSYNC.
  @Test
  public void testBlockBlobCompactionCapabilities() throws IOException {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_COMPACTION_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(stream.hasCapability(StreamCapabilities.HFLUSH));
      assertTrue(stream.hasCapability(StreamCapabilities.HSYNC));
      assertFalse(stream.hasCapability(StreamCapabilities.DROPBEHIND));
      assertFalse(stream.hasCapability(StreamCapabilities.READAHEAD));
      assertFalse(stream.hasCapability(StreamCapabilities.UNBUFFER));
      stream.write(getRandomBytes());
    }
  }

  // A small write does not write data to storage for Page Blobs
  @Test
  public void testPageBlobSmallWrite() throws IOException {
    Path path = getBlobPathWithTestName(PAGE_BLOB_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isPageBlobStreamWrapper(stream));
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      validate(path, buffer, false);
    }
  }

  // A small write does not write data to storage for Block Blobs
  @Test
  public void testBlockBlobSmallWrite() throws IOException {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      validate(path, buffer, false);
    }
  }

  // A small write does not write data to storage for Block Blobs
  // with Compaction
  @Test
  public void testBlockBlobCompactionSmallWrite() throws IOException {
    Path path = getBlobPathWithTestName(BLOCK_BLOB_COMPACTION_DIR);
    try (FSDataOutputStream stream = fs.create(path)) {
      assertTrue(isBlockBlobAppendStreamWrapper(stream));
      byte[] buffer = getRandomBytes();
      stream.write(buffer);
      validate(path, buffer, false);
    }
  }
}
