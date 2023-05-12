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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.mockito.Mockito;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INFINITE_LEASE_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;

/**
 * Test append operations.
 */
public class ITestAzureBlobFileSystemAppend extends
    AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final Path TEST_FOLDER_PATH = new Path("testFolder");
  private static final String TEST_FILE = "testfile";

  public ITestAzureBlobFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = TEST_FILE_PATH;
    fs.mkdirs(filePath);
    fs.append(filePath, 0);
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(TEST_FILE_PATH)) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
      assertEquals(0, stream.getPos());
    }
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = TEST_FILE_PATH;
    ContractTestUtils.touch(fs, filePath);
    fs.delete(filePath, false);
    fs.append(filePath);
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = TEST_FOLDER_PATH;
    fs.mkdirs(folderPath);
    fs.append(folderPath);
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendImplicitDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = TEST_FOLDER_PATH;
    fs.mkdirs(folderPath);
    fs.append(folderPath.getParent());
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendFileNotExists() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.append(TEST_FOLDER_PATH);
  }

  @Test(expected = IOException.class)
  public void testIsAppendBlob() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(true).when(store).isAppendBlobKey(anyString());
    fs.getAbfsStore().getAbfsConfiguration().setPrefixMode(PrefixMode.BLOB);

    // Set abfsStore as our mocked value.
    Field privateField = AzureBlobFileSystem.class.getDeclaredField("abfsStore");
    privateField.setAccessible(true);
    privateField.set(fs, store);

    fs.create(TEST_FILE_PATH);
  }

  @Test(expected = IOException.class)
  public void testSmallWriteBlob() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(AZURE_ENABLE_SMALL_WRITE_OPTIMIZATION, "true");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    fs.getAbfsStore().getAbfsConfiguration().setPrefixMode(PrefixMode.BLOB);

    fs.create(TEST_FILE_PATH);
  }

  /** Create file over dfs endpoint and append over blob endpoint **/
  @Test
  public void testCreateOverDfsAppendOverBlob() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    fs.getAbfsClient().createPath(makeQualified(TEST_FILE_PATH).toUri().getPath(), true, false,
            null, null, false,
            null, getTestTracingContext(fs, true));
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    outputStream.write(10);
    outputStream.hsync();
    outputStream.write(20);
    outputStream.hsync();
    outputStream.write(30);
    outputStream.hsync();
  }

  /**
   * Create directory over dfs endpoint and append over blob endpoint.
   * Should return error as append is not supported for directory.
   * **/
  @Test(expected = IOException.class)
  public void testCreateExplicitDirectoryOverDfsAppendOverBlob() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    fs.getAbfsClient().createPath(makeQualified(TEST_FOLDER_PATH).toUri().getPath(), false, false,
            null, null, false,
            null, getTestTracingContext(fs, true));
    FSDataOutputStream outputStream = fs.append(TEST_FOLDER_PATH);
    outputStream.write(10);
    outputStream.hsync();
  }

  /**
   * Recreate file between append and flush. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    Assume.assumeTrue(!fs.getAbfsStore().getAbfsConfiguration().shouldIngressFallbackToDfs());
    fs.create(TEST_FILE_PATH);
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    outputStream.write(10);
    final AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    FSDataOutputStream outputStream1 = fs1.create(TEST_FILE_PATH);
    outputStream.hsync();
  }

  /**
   * Recreate file between append and flush using dfs. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateDirectoryAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(fs.getAbfsStore().getAbfsConfiguration().getPrefixMode() == PrefixMode.BLOB);
    fs.create(TEST_FILE_PATH);
    FSDataOutputStream outputStream = fs.append(TEST_FILE_PATH);
    outputStream.write(10);
    final AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    fs1.mkdirs(TEST_FILE_PATH);
    outputStream.hsync();
  }

  @Test
  public void testTracingForAppend() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    fs.create(TEST_FILE_PATH);
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.APPEND, false, 0));
    fs.append(TEST_FILE_PATH, 10);
  }

  /**
   * Verify that no calls to getBlockList were made.
   */
  @Test
  public void testCreateEmptyBlob() throws IOException {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    fs.create(TEST_FILE_PATH);
    Mockito.verify(spiedClient, Mockito.times(0))
            .getBlockList(Mockito.any(String.class),
                    Mockito.any(TracingContext.class));
  }

  /**
   * Verify that no calls to getBlockList were made.
   */
  @Test
  public void testCreateNonEmptyBlob() throws IOException {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
            client,
            fs.getAbfsStore().getAbfsConfiguration()));
    store.setClient(testClient);

    FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH);
    outputStream.write(10);
    outputStream.hsync();
    outputStream.close();
    fs.append(TEST_FILE_PATH);
    Mockito.verify(testClient, Mockito.times(1))
            .getBlockList(Mockito.any(String.class),
                    Mockito.any(TracingContext.class));
  }

  /**
   * Verify that if getBlockList throws exception append should fail.
   */
  @Test
  public void testValidateGetBlockList() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient testClient = Mockito.spy(TestAbfsClient.createTestClientFromCurrentContext(
            client,
            fs.getAbfsStore().getAbfsConfiguration()));
    store.setClient(testClient);

    AzureBlobFileSystemException exception = Mockito.mock(AzureBlobFileSystemException.class);
    // Throw exception when getBlockList is called
    Mockito.doThrow(exception).when(testClient)
            .getBlockList(Mockito.any(), Mockito.any(TracingContext.class));

    // Create a non-empty file
    FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH);
    outputStream.write(10);
    outputStream.hsync();
    outputStream.close();

    intercept(AzureBlobFileSystemException.class, () -> fs.getAbfsStore()
            .openFileForWrite(TEST_FILE_PATH, null, false, getTestTracingContext(fs, true)));
  }

  /**
   * Verify that parallel write with same offset from different output streams will not throw exception.
   **/
  @Test
  public void testParallelWriteSameOffsetDifferentOutputStreams() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future<?>> futures = new ArrayList<>();

    final byte[] b = new byte[8 * ONE_MB];
    new Random().nextBytes(b);

    // Create three output streams
    FSDataOutputStream out1 = fs.create(TEST_FILE_PATH);
    FSDataOutputStream out2 = fs.append(TEST_FILE_PATH);
    FSDataOutputStream out3 = fs.append(TEST_FILE_PATH);

    // Submit tasks to write to each output stream with the same offset
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out3.write(b, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    int exceptionCaught = 0;
    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }
    assertEquals(exceptionCaught, 0);
  }


  /**
   * Verify that parallel write for different content length will not throw exception.
   **/
  @Test
  public void testParallelWriteDifferentContentLength() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    FileSystem fs = FileSystem.newInstance(configuration);
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future<?>> futures = new ArrayList<>();

    // Create three output streams with different content length
    FSDataOutputStream out1 = fs.create(TEST_FILE_PATH);
    final byte[] b1 = new byte[8 * ONE_MB];
    new Random().nextBytes(b1);

    FSDataOutputStream out2 = fs.append(TEST_FILE_PATH);
    FSDataOutputStream out3 = fs.append(TEST_FILE_PATH);

    // Submit tasks to write to each output stream
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b1, 10, 200);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b1, 20, 300);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out3.write(b1, 30, 400);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    int exceptionCaught = 0;
    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }
    assertEquals(exceptionCaught, 0);
  }

  /**
   * Verify that parallel write for different content length will not throw exception.
   **/
  @Test
  public void testParallelWriteOutputStreamClose() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    final Path SECONDARY_FILE_PATH = new Path("secondarytestfile");
    AzureBlobFileSystem fs = getFileSystem();
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future<?>> futures = new ArrayList<>();

    FSDataOutputStream out1 = fs.create(SECONDARY_FILE_PATH);
    AbfsOutputStream outputStream1 = (AbfsOutputStream) out1.getWrappedStream();
    String fileETag = outputStream1.getETag();
    final byte[] b1 = new byte[8 * ONE_MB];
    new Random().nextBytes(b1);
    final byte[] b2 = new byte[8 * ONE_MB];
    new Random().nextBytes(b2);

    FSDataOutputStream out2 = fs.append(SECONDARY_FILE_PATH);

    // Submit tasks to write to each output stream
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b1, 0, 200);
        out1.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b2, 0, 400);
        out2.hsync();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    int exceptionCaught = 0;

    for (Future<?> future : futures) {
      try {
        future.get(); // wait for the task to complete and handle any exceptions thrown by the lambda expression
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          exceptionCaught++;
        } else {
          System.err.println("Unexpected exception caught: " + cause);
        }
      } catch (InterruptedException e) {
        // handle interruption
      }
    }

    assertEquals(exceptionCaught, 1);
    // Validate that the data written in the buffer is the same as what was read
    final byte[] readBuffer = new byte[8 * ONE_MB];
    int result;
    FSDataInputStream inputStream = fs.open(SECONDARY_FILE_PATH);
    inputStream.seek(0);

    AbfsOutputStream outputStream2 = (AbfsOutputStream) out1.getWrappedStream();
    String out1Etag = outputStream2.getETag();

    AbfsOutputStream outputStream3 = (AbfsOutputStream) out2.getWrappedStream();
    String out2Etag = outputStream3.getETag();

    if (!fileETag.equals(out1Etag)) {
      result = inputStream.read(readBuffer, 0, 4 * ONE_MB);
      assertEquals(result, 200); // Verify that the number of bytes read matches the number of bytes written
      assertArrayEquals(Arrays.copyOfRange(readBuffer, 0, result), Arrays.copyOfRange(b1, 0, result)); // Verify that the data read matches the original data written
    } else if (!fileETag.equals(out2Etag)) {
      result = inputStream.read(readBuffer, 0, 4 * ONE_MB);
      assertEquals(result, 400); // Verify that the number of bytes read matches the number of bytes written
      assertArrayEquals(Arrays.copyOfRange(readBuffer, 0, result), Arrays.copyOfRange(b2, 0, result)); // Verify that the data read matches the original data written
    } else {
      fail("Neither out1 nor out2 was flushed successfully.");
    }
  }

  /**
   * Verify that once flushed etag changes.
   **/
  @Test
  public void testEtagMismatch() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = getFileSystem();
    FSDataOutputStream out1 = fs.create(TEST_FILE_PATH);
    FSDataOutputStream out2 = fs.create(TEST_FILE_PATH);

    out2.write(10);
    out2.hsync();
    out1.write(10);
    intercept(IOException.class, () -> out1.hsync());
  }

  /**
   * Verify that for flush without append no network calls are made.
   **/
  @Test
  public void testNoNetworkCallsForFlush() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH);
    outputStream.hsync();
    Mockito.verify(spiedClient, Mockito.times(0))
            .append(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(TracingContext.class), Mockito.any());
    Mockito.verify(spiedClient, Mockito.times(0)).
            flush(any(byte[].class), anyString(), anyBoolean(), isNull(), isNull(), any(),
                    any(TracingContext.class));
  }

  /**
   * Verify that for flush without append no network calls are made.
   **/
  @Test
  public void testNoNetworkCallsForSecondFlush() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    FSDataOutputStream outputStream = fs.create(TEST_FILE_PATH);
    outputStream.write(10);
    outputStream.hsync();
    outputStream.close();
    Mockito.verify(spiedClient, Mockito.times(1))
            .append(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(TracingContext.class), Mockito.any());
    Mockito.verify(spiedClient, Mockito.times(1)).
            flush(any(byte[].class), anyString(), anyBoolean(), isNull(), isNull(), any(),
                    any(TracingContext.class));
  }

  private AzureBlobFileSystem getCustomFileSystem(Path infiniteLeaseDirs, int numLeaseThreads) throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", getAbfsScheme()), true);
    conf.set(FS_AZURE_INFINITE_LEASE_KEY, infiniteLeaseDirs.toUri().getPath());
    conf.setInt(FS_AZURE_LEASE_THREADS, numLeaseThreads);
    return getFileSystem(conf);
  }

  @Test
  public void testAppendWithLease() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    final Path testFilePath = new Path(path(methodName.getMethodName()), TEST_FILE);
    final AzureBlobFileSystem fs = Mockito.spy(getCustomFileSystem(testFilePath.getParent(), 1));
    AbfsOutputStream outputStream = (AbfsOutputStream) fs.getAbfsStore().createFile(testFilePath, null, true,
            null, null, getTestTracingContext(fs, true), null);
    outputStream.write(10);
    outputStream.close();
    assertNotNull(outputStream.getLeaseId());
  }

}
