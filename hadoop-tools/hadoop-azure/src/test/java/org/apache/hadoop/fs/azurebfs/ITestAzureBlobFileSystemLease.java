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
package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_EXPIRED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_PARALLEL_ACCESS_DETECTED;

/**
 * Test lease operations.
 */
public class ITestAzureBlobFileSystemLease extends AbstractAbfsIntegrationTest {
  private static final int TEST_EXECUTION_TIMEOUT = 30 * 1000;
  private static final int LONG_TEST_EXECUTION_TIMEOUT = 90 * 1000;
  private static final String TEST_FILE = "testfile";

  public ITestAzureBlobFileSystemLease() throws Exception {
    super();
  }

  @Test
  public void testNoSingleWriter() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    try (FSDataOutputStream out = fs.create(testFilePath)) {
      Assert.assertFalse("Output stream should not have lease",
          ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    }
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test
  public void testOneWriter() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    FSDataOutputStream out = fs.create(testFilePath);
    Assert.assertTrue("Output stream should have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    out.close();
    Assert.assertFalse("Output stream should not have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test
  public void testSubDir() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(new Path(path(methodName.getMethodName()), "subdir"),
        TEST_FILE);
    fs.mkdirs(testFilePath.getParent().getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    FSDataOutputStream out = fs.create(testFilePath);
    Assert.assertTrue("Output stream should have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    out.close();
    Assert.assertFalse("Output stream should not have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test
  public void testTwoCreate() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    try (FSDataOutputStream out = fs.create(testFilePath)) {
      try (FSDataOutputStream out2 = fs.create(testFilePath)) {
        Assert.fail("Second create succeeded");
      } catch (IOException e) {
        Assert.assertTrue("Unexpected error message: " + e.getMessage(),
            e.getMessage().contains(ERR_PARALLEL_ACCESS_DETECTED));
      }
    }
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  private void twoWriters(AzureBlobFileSystem fs, Path testFilePath, boolean expectException) throws Exception {
    try (FSDataOutputStream out = fs.create(testFilePath)) {
      try (FSDataOutputStream out2 = fs.append(testFilePath)) {
        out2.writeInt(2);
        out2.hsync();
      } catch (IOException e) {
        if (expectException) {
          Assert.assertTrue("Unexpected error message: " + e.getMessage(),
              e.getMessage().contains(ERR_ACQUIRING_LEASE));
        } else {
          Assert.fail("Unexpected exception " + e.getMessage());
        }
      }
      out.writeInt(1);
      out.hsync();
    }

    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testTwoWritersCreateAppendNoSingleWriter() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());

    twoWriters(fs, testFilePath, false);
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testTwoWritersCreateAppendWithSingleWriterEnabled() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    twoWriters(fs, testFilePath, true);
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testLeaseFreedOnClose() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    FSDataOutputStream out;
    out = fs.create(testFilePath);
    out.write(0);
    Assert.assertTrue("Output stream should have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    out.close();
    Assert.assertFalse("Output stream should not have lease after close",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testWriteAfterBreakLease() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    FSDataOutputStream out;
    out = fs.create(testFilePath);
    out.write(0);
    out.hsync();

    fs.breakLease(testFilePath);
    try {
      out.write(1);
      out.hsync();
      Assert.fail("Expected exception on write after lease break");
    } catch (IOException e) {
      Assert.assertTrue("Unexpected error message: " + e.getMessage(),
          e.getMessage().contains(ERR_LEASE_EXPIRED));
    }
    try {
      out.close();
      Assert.fail("Expected exception on close after lease break");
    } catch (IOException e) {
      Assert.assertTrue("Unexpected error message: " + e.getMessage(),
          e.getMessage().contains(ERR_LEASE_EXPIRED));
    }

    Assert.assertTrue(((AbfsOutputStream) out.getWrappedStream()).isLeaseFreed());

    try (FSDataOutputStream out2 = fs.append(testFilePath)) {
      out2.write(2);
      out2.hsync();
    }

    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test(timeout = LONG_TEST_EXECUTION_TIMEOUT)
  public void testLeaseFreedAfterBreak() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    fs.mkdirs(testFilePath.getParent());
    fs.getAbfsStore().getAbfsConfiguration()
        .setAzureSingleWriterDirs(testFilePath.getParent().toString());
    fs.getAbfsStore().updateSingleWriterDirs();

    FSDataOutputStream out = null;
    try {
      out = fs.create(testFilePath);
      out.write(0);

      fs.breakLease(testFilePath);
      while (!((AbfsOutputStream) out.getWrappedStream()).isLeaseFreed()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    } finally {
      try {
        if (out != null) {
          out.close();
        }
        // exception might or might not occur
      } catch (IOException e) {
        Assert.assertTrue("Unexpected error message: " + e.getMessage(),
            e.getMessage().contains(ERR_LEASE_EXPIRED));
      }
    }
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }
}
