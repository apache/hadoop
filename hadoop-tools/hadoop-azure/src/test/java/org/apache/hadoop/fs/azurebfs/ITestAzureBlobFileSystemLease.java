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
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.INFINITE_LEASE_DURATION;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.CONDITION_NOT_MET;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_EXPIRED_BLOB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_ID_SPECIFIED_BLOB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INFINITE_LEASE_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_ACQUIRING_LEASE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_LEASE_EXPIRED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_ID_SPECIFIED;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_NO_LEASE_THREADS;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test lease operations.
 */
public class ITestAzureBlobFileSystemLease extends AbstractAbfsIntegrationTest {
  private static final int TEST_EXECUTION_TIMEOUT = 30 * 1000;
  private static final int LONG_TEST_EXECUTION_TIMEOUT = 90 * 1000;
  private static final String TEST_FILE = "testfile";
  private final boolean isHNSEnabled;
  private static final int TEST_BYTES = 20;
  private static final String PARALLEL_ACCESS = "Parallel access to the create path "
      + "detected";

  public ITestAzureBlobFileSystemLease() throws Exception {
    super();
    this.isHNSEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
  }

  private AzureBlobFileSystem getCustomFileSystem(Path infiniteLeaseDirs, int numLeaseThreads) throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", getAbfsScheme()), true);
    conf.set(FS_AZURE_INFINITE_LEASE_KEY, infiniteLeaseDirs.toUri().getPath());
    conf.setInt(FS_AZURE_LEASE_THREADS, numLeaseThreads);
    return getFileSystem(conf);
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testNoInfiniteLease() throws IOException {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(testFilePath.getParent());
    try (FSDataOutputStream out = fs.create(testFilePath)) {
      Assertions.assertFalse(
         ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should not have lease");
    }
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testNoLeaseThreads() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 0);
    fs.mkdirs(testFilePath.getParent());
    LambdaTestUtils.intercept(IOException.class, ERR_NO_LEASE_THREADS, () -> {
      try (FSDataOutputStream out = fs.create(testFilePath)) {
      }
      return "No failure when lease requested with 0 lease threads";
    });
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testOneWriter() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    Assertions.assertTrue(
       ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should have lease");
    out.close();
    Assertions.assertFalse(
       ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should not have lease");
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testSubDir() throws Exception {
    final Path testFilePath = new Path(new Path(path(methodName.getMethodName()), "subdir"),
        TEST_FILE);
    final AzureBlobFileSystem fs =
        getCustomFileSystem(testFilePath.getParent().getParent(), 1);
    fs.mkdirs(testFilePath.getParent().getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    Assertions.assertTrue(
       ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should have lease");
    out.close();
    Assertions.assertFalse(
       ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should not have lease");
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testTwoCreate() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();
    assumeValidTestConfigPresent(getRawConfiguration(), FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT);
    fs.mkdirs(testFilePath.getParent());

    try (FSDataOutputStream out = fs.create(testFilePath)) {
      LambdaTestUtils.intercept(IOException.class,
          isHNSEnabled ? PARALLEL_ACCESS
              : client instanceof AbfsBlobClient
                  ? ERR_NO_LEASE_ID_SPECIFIED_BLOB
                  : ERR_NO_LEASE_ID_SPECIFIED, () -> {
            try (FSDataOutputStream out2 = fs.create(testFilePath)) {
            }
            return "Expected second create on infinite lease dir to fail";
          });
    }
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  private void twoWriters(AzureBlobFileSystem fs, Path testFilePath, boolean expectException) throws Exception {
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();
    try (FSDataOutputStream out = fs.create(testFilePath)) {
      try (FSDataOutputStream out2 = fs.append(testFilePath)) {
        out2.writeInt(2);
        out2.hsync();
      } catch (IOException e) {
        if (expectException) {
          GenericTestUtils.assertExceptionContains(ERR_ACQUIRING_LEASE, e);
        } else {
          throw e;
        }
      }
      out.writeInt(1);
      try {
        out.hsync();
      } catch (IOException e) {
        // Etag mismatch leads to condition not met error for blob endpoint.
        if (client instanceof AbfsBlobClient) {
          GenericTestUtils.assertExceptionContains(CONDITION_NOT_MET, e);
        } else {
          throw e;
        }
      }
    } catch (IOException e) {
      // Etag mismatch leads to condition not met error for blob endpoint.
      if (client instanceof AbfsBlobClient) {
        GenericTestUtils.assertExceptionContains(CONDITION_NOT_MET, e);
      } else {
        throw e;
      }
    }

    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testTwoWritersCreateAppendNoInfiniteLease() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getFileSystem();
    assumeThat(isAppendBlobEnabled()).as("Parallel Writes Not Allowed on Append Blobs").isFalse();
    fs.mkdirs(testFilePath.getParent());

    twoWriters(fs, testFilePath, false);
  }

  @Test
  @Timeout(value = LONG_TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testTwoWritersCreateAppendWithInfiniteLeaseEnabled() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    assumeThat(isAppendBlobEnabled()).as("Parallel Writes Not Allowed on Append Blobs").isFalse();
    fs.mkdirs(testFilePath.getParent());

    twoWriters(fs, testFilePath, true);
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testLeaseFreedOnClose() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out;
    out = fs.create(testFilePath);
    out.write(0);
    Assertions.assertTrue(
       ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should have lease");
    out.close();
    Assertions.assertFalse(
       ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should not have lease after close");
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testWriteAfterBreakLease() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out;
    out = fs.create(testFilePath);
    out.write(0);
    out.hsync();

    fs.registerListener(new TracingHeaderValidator(
        getConfiguration().getClientCorrelationId(), fs.getFileSystemId(),
        FSOperationType.BREAK_LEASE, false, 0));
    fs.breakLease(testFilePath);
    fs.registerListener(null);
    LambdaTestUtils.intercept(IOException.class, client instanceof AbfsBlobClient
        ? ERR_LEASE_EXPIRED_BLOB : ERR_LEASE_EXPIRED, () -> {
      out.write(1);
      out.hsync();
      return "Expected exception on write after lease break but got " + out;
    });

    LambdaTestUtils.intercept(IOException.class, client instanceof AbfsBlobClient
        ? ERR_LEASE_EXPIRED_BLOB : ERR_LEASE_EXPIRED, () -> {
      if (isAppendBlobEnabled() && getIngressServiceType() == AbfsServiceType.BLOB) {
        out.write(TEST_BYTES);
      }
      out.close();
      return "Expected exception on close after lease break but got " + out;
    });

    Assertions.assertTrue(
       ((AbfsOutputStream) out.getWrappedStream()).isLeaseFreed(), "Output stream lease should be freed");

    try (FSDataOutputStream out2 = fs.append(testFilePath)) {
      out2.write(2);
      out2.hsync();
    }

    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = LONG_TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testLeaseFreedAfterBreak() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    AbfsClient client = fs.getAbfsStore().getClientHandler().getIngressClient();
    fs.mkdirs(testFilePath.getParent());

    FSDataOutputStream out = fs.create(testFilePath);
    out.write(0);

    fs.breakLease(testFilePath);
    LambdaTestUtils.intercept(IOException.class, client instanceof AbfsBlobClient
        ? ERR_LEASE_EXPIRED_BLOB : ERR_LEASE_EXPIRED, () -> {
      out.close();
      return "Expected exception on close after lease break but got " + out;
    });

    Assertions.assertTrue(
       ((AbfsOutputStream) out.getWrappedStream()).isLeaseFreed(), "Output stream lease should be freed");

    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testInfiniteLease() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    fs.mkdirs(testFilePath.getParent());

    try (FSDataOutputStream out = fs.create(testFilePath)) {
      Assertions.assertTrue(
         ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should have lease");
      out.write(0);
    }
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed());

    try (FSDataOutputStream out = fs.append(testFilePath)) {
      Assertions.assertTrue(
         ((AbfsOutputStream) out.getWrappedStream()).hasLease(), "Output stream should have lease");
      out.write(1);
    }
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testFileSystemClose() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    fs.mkdirs(testFilePath.getParent());

    try (FSDataOutputStream out = fs.create(testFilePath)) {
      out.write(0);
      Assertions.assertFalse(
         fs.getAbfsStore().areLeasesFreed(), "Store leases should exist");
    }
    fs.close();
    Assertions.assertTrue(fs.getAbfsStore().areLeasesFreed(), "Store leases were not freed");

    Callable<String> exceptionRaisingCallable = () -> {
      try (FSDataOutputStream out2 = fs.append(testFilePath)) {
      }
      return "Expected exception on new append after closed FS";
    };
    /*
     * For ApacheHttpClient, the failure would happen when trying to get a connection
     * from KeepAliveCache, which is not possible after the FS is closed, as that
     * also closes the cache.
     *
     * For JDK_Client, the failure happens when trying to submit a task to the
     * executor service, which is not possible after the FS is closed, as that
     * also shuts down the executor service.
     */

    if (getConfiguration().getPreferredHttpOperationType()
        == HttpOperationType.APACHE_HTTP_CLIENT) {
      LambdaTestUtils.intercept(AbfsDriverException.class,
          exceptionRaisingCallable);
    } else {
      LambdaTestUtils.intercept(RejectedExecutionException.class,
          exceptionRaisingCallable);
    }
  }

  @Test
  @Timeout(value = TEST_EXECUTION_TIMEOUT, unit = TimeUnit.MILLISECONDS)
  public void testAcquireRetry() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = getCustomFileSystem(testFilePath.getParent(), 1);
    fs.mkdirs(testFilePath.getParent());
    fs.createNewFile(testFilePath);
    TracingContext tracingContext = getTestTracingContext(fs, true);
    Listener listener = new TracingHeaderValidator(
        getConfiguration().getClientCorrelationId(), fs.getFileSystemId(),
        FSOperationType.TEST_OP, true, 0);
    tracingContext.setListener(listener);

    AbfsLease lease = new AbfsLease(fs.getAbfsClient(),
            testFilePath.toUri().getPath(), true, INFINITE_LEASE_DURATION,
            null, tracingContext);
    Assertions.assertNotNull(lease.getLeaseID(), "Did not successfully lease file");
    listener.setOperation(FSOperationType.RELEASE_LEASE);
    lease.free();
    lease.getTracingContext().setListener(null);
    Assertions.assertEquals(0, lease.getAcquireRetryCount(), "Unexpected acquire retry count");

    AbfsClient mockClient = spy(fs.getAbfsClient());

    doThrow(new AbfsLease.LeaseException("failed to acquire 1"))
        .doThrow(new AbfsLease.LeaseException("failed to acquire 2"))
        .doCallRealMethod().when(mockClient)
        .acquireLease(anyString(), anyInt(), any(), any(TracingContext.class));

    lease = new AbfsLease(mockClient, testFilePath.toUri().getPath(), true, 5, 1,
            INFINITE_LEASE_DURATION, null, tracingContext);
    Assertions.assertNotNull(lease.getLeaseID(), "Acquire lease should have retried");
    lease.free();
    Assertions.assertEquals(2, lease.getAcquireRetryCount(), "Unexpected acquire retry count");

    doThrow(new AbfsLease.LeaseException("failed to acquire")).when(mockClient)
        .acquireLease(anyString(), anyInt(), any(), any(TracingContext.class));

    LambdaTestUtils.intercept(AzureBlobFileSystemException.class, () -> {
      new AbfsLease(mockClient, testFilePath.toUri().getPath(), true, 5, 1,
              INFINITE_LEASE_DURATION, null, tracingContext);
    });
  }
}
