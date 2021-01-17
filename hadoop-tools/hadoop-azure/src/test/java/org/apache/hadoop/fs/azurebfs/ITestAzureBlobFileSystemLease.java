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

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SINGLE_WRITER_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_ALREADY_PRESENT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_BROKEN;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_DID_NOT_MATCH;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_EXPIRED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_ID_NOT_PRESENT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_NOT_PRESENT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_ID_SPECIFIED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_PARALLEL_ACCESS_DETECTED;

/**
 * Test lease operations.
 */
public class ITestAzureBlobFileSystemLease extends AbstractAbfsIntegrationTest {
  private static final int TEST_EXECUTION_TIMEOUT = 30 * 1000;
  private static final int LONG_TEST_EXECUTION_TIMEOUT = 90 * 1000;
  private static final int TEST_LEASE_DURATION = 15;
  private static final String TEST_FILE = "testfile";
  private final boolean isHNSEnabled;

  public ITestAzureBlobFileSystemLease() throws Exception {
    super();

    this.isHNSEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
  }

  private AzureBlobFileSystem getCustomFileSystem(String singleWriterDirs, int numLeaseThreads)
      throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", getAbfsScheme()), true);
    conf.set(FS_AZURE_SINGLE_WRITER_KEY, singleWriterDirs);
    conf.setInt(FS_AZURE_LEASE_THREADS, numLeaseThreads);
    return getFileSystem(conf);
  }

  @Test
  public void testNoSingleWriter() throws IOException {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(testFilePath.getParent());
    try (FSDataOutputStream out = fs.create(testFilePath)) {
      Assert.assertFalse("Output stream should not have lease",
          ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    }
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test
  public void testNoLeaseThreads() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 0);
    fs.mkdirs(testFilePath.getParent());
    try (FSDataOutputStream out = fs.create(testFilePath)) {
      Assert.fail("No failure when lease requested with 0 lease threads");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(ERR_NO_LEASE_THREADS, e);
    }
  }

  @Test
  public void testOneWriter() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    Assert.assertTrue("Output stream should have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    out.close();
    Assert.assertFalse("Output stream should not have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test
  public void testSubDir() throws Exception {
    final Path testFilePath = new Path(new Path(path(methodName.getMethodName()), "subdir"),
        TEST_FILE);
    final AzureBlobFileSystem fs =
        getCustomFileSystem(testFilePath.getParent().getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent().getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    Assert.assertTrue("Output stream should have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    out.close();
    Assert.assertFalse("Output stream should not have lease",
        ((AbfsOutputStream) out.getWrappedStream()).hasLease());
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test
  public void testTwoCreate() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

    try (FSDataOutputStream out = fs.create(testFilePath)) {
      LambdaTestUtils.intercept(IOException.class, isHNSEnabled ? ERR_PARALLEL_ACCESS_DETECTED :
          ERR_NO_LEASE_ID_SPECIFIED, () -> {
        try (FSDataOutputStream out2 = fs.create(testFilePath)) {
        }
        return "Expected second create on single writer dir to fail";
      });
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
          GenericTestUtils.assertExceptionContains(ERR_ACQUIRING_LEASE, e);
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
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(testFilePath.getParent());

    twoWriters(fs, testFilePath, false);
  }

  @Test(timeout = LONG_TEST_EXECUTION_TIMEOUT)
  public void testTwoWritersCreateAppendWithSingleWriterEnabled() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

    twoWriters(fs, testFilePath, true);
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testLeaseFreedOnClose() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

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
  public void testWriteAfterBreakLease() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out;
    out = fs.create(testFilePath);
    out.write(0);
    out.hsync();

    fs.breakLease(testFilePath);

    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_EXPIRED, () -> {
      out.write(1);
      out.hsync();
      return "Expected exception on write after lease break but got " + out;
    });

    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_EXPIRED, () -> {
      out.close();
      return "Expected exception on close after lease break but got " + out;
    });

    Assert.assertTrue(((AbfsOutputStream) out.getWrappedStream()).isLeaseFreed());

    try (FSDataOutputStream out2 = fs.append(testFilePath)) {
      out2.write(2);
      out2.hsync();
    }

    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test(timeout = LONG_TEST_EXECUTION_TIMEOUT)
  public void testLeaseFreedAfterBreak() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    out.write(0);

    fs.breakLease(testFilePath);

    while (!((AbfsOutputStream) out.getWrappedStream()).isLeaseFreed()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }

    LambdaTestUtils.intercept(IOException.class, isHNSEnabled ? ERR_LEASE_NOT_PRESENT :
        ERR_LEASE_EXPIRED, () -> {
      out.close();
      return "Expected exception on close after lease break but got " + out;
    });

    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testFileSystemClose() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent().toString(), 1);
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    out.write(0);
    Assert.assertFalse(fs.getAbfsStore().areLeasesFreed());
    fs.close();
    Assert.assertTrue(fs.getAbfsStore().areLeasesFreed());

    LambdaTestUtils.intercept(IOException.class, isHNSEnabled ? ERR_LEASE_NOT_PRESENT :
        ERR_LEASE_EXPIRED, () -> {
      out.close();
      return "Expected exception on close after closed FS but got " + out;
    });

    LambdaTestUtils.intercept(RejectedExecutionException.class, () -> {
      try (FSDataOutputStream out2 = fs.append(testFilePath)) {
      }
      return "Expected exception on new append after closed FS";
    });
  }

  @Test(timeout = TEST_EXECUTION_TIMEOUT)
  public void testFileSystemLeaseOps() throws Exception {
    final Path testDir = path(methodName.getMethodName());
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(testDir);

    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_ID_NOT_PRESENT, () -> {
      fs.breakLease(testDir);
      return "Expected exception on break lease when no lease is held";
    });

    String leaseId = fs.acquireLease(testDir, TEST_LEASE_DURATION);
    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_ALREADY_PRESENT, () -> {
      fs.acquireLease(testDir, TEST_LEASE_DURATION);
      return "Expected exception on acquire lease when lease is already held";
    });
    fs.renewLease(testDir, leaseId);
    fs.releaseLease(testDir, leaseId);
    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_NOT_PRESENT, () -> {
      fs.renewLease(testDir, leaseId);
      return "Expected exception on renew lease after lease has been released";
    });

    String leaseId2 = fs.acquireLease(testDir, TEST_LEASE_DURATION);
    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_DID_NOT_MATCH, () -> {
      fs.renewLease(testDir, leaseId);
      return "Expected exception on renew wrong lease ID";
    });
    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_DID_NOT_MATCH, () -> {
      fs.releaseLease(testDir, leaseId);
      return "Expected exception on release wrong lease ID";
    });
    fs.breakLease(testDir);
    LambdaTestUtils.intercept(IOException.class, ERR_LEASE_BROKEN, () -> {
      fs.renewLease(testDir, leaseId2);
      return "Expected exception on renewing broken lease";
    });
    fs.releaseLease(testDir, leaseId2);
  }
}
