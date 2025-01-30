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
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AzureBlobIngressHandler;
import org.apache.hadoop.fs.azurebfs.services.AzureDFSIngressHandler;
import org.apache.hadoop.fs.azurebfs.services.AzureIngressHandler;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.store.BlockUploadStatistics;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.test.LambdaTestUtils;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INFINITE_LEASE_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INGRESS_SERVICE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_ID_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient.generateBlockListXml;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_DISK;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BYTEBUFFER;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Closed;
import static org.apache.hadoop.fs.store.DataBlocks.DataBlock.DestState.Writing;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Test append operations.
 */
public class ITestAzureBlobFileSystemAppend extends
    AbstractAbfsIntegrationTest {

  private static final String TEST_FILE_PATH = "testfile";

  private static final String TEST_FOLDER_PATH = "testFolder";

  private static final int TEN = 10;
  private static final int TWENTY = 20;
  private static final int THIRTY = 30;
  private static final int HUNDRED = 100;

  public ITestAzureBlobFileSystemAppend() throws Exception {
    super();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirShouldFail() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.mkdirs(filePath);
    fs.append(filePath, 0).close();
  }

  @Test
  public void testAppendWithLength0() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try (FSDataOutputStream stream = fs.create(path(TEST_FILE_PATH))) {
      final byte[] b = new byte[1024];
      new Random().nextBytes(b);
      stream.write(b, 1000, 0);
      assertEquals(0, stream.getPos());
    }
  }


  @Test(expected = FileNotFoundException.class)
  public void testAppendFileAfterDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    ContractTestUtils.touch(fs, filePath);
    fs.delete(filePath, false);

    fs.append(filePath).close();
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = path(TEST_FOLDER_PATH);
    fs.mkdirs(folderPath);
    fs.append(folderPath).close();
  }

  @Test
  public void testTracingForAppend() throws IOException {
    AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_FILE_PATH);
    fs.create(testPath).close();
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.APPEND, false, 0));
    fs.append(testPath, TEN);
  }

  @Test
  public void testCloseOfDataBlockOnAppendComplete() throws Exception {
    Set<String> blockBufferTypes = new HashSet<>();
    blockBufferTypes.add(DATA_BLOCKS_BUFFER_DISK);
    blockBufferTypes.add(DATA_BLOCKS_BYTEBUFFER);
    blockBufferTypes.add(DATA_BLOCKS_BUFFER_ARRAY);
    for (String blockBufferType : blockBufferTypes) {
      Configuration configuration = new Configuration(getRawConfiguration());
      configuration.set(DATA_BLOCKS_BUFFER, blockBufferType);
      try (AzureBlobFileSystem fs = Mockito.spy(
          (AzureBlobFileSystem) FileSystem.newInstance(configuration))) {
        AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
        Mockito.doReturn(store).when(fs).getAbfsStore();
        DataBlocks.DataBlock[] dataBlock = new DataBlocks.DataBlock[1];
        Mockito.doAnswer(getBlobFactoryInvocation -> {
          DataBlocks.BlockFactory factory = Mockito.spy(
              (DataBlocks.BlockFactory) getBlobFactoryInvocation.callRealMethod());
          Mockito.doAnswer(factoryCreateInvocation -> {
                dataBlock[0] = Mockito.spy(
                    (DataBlocks.DataBlock) factoryCreateInvocation.callRealMethod());
                return dataBlock[0];
              })
              .when(factory)
              .create(Mockito.anyLong(), Mockito.anyInt(), Mockito.any(
                  BlockUploadStatistics.class));
          return factory;
        }).when(store).getBlockFactory();
        try (OutputStream os = fs.create(
            new Path(getMethodName() + "_" + blockBufferType))) {
          os.write(new byte[1]);
          Assertions.assertThat(dataBlock[0].getState())
              .describedAs(
                  "On write of data in outputStream, state should become Writing")
              .isEqualTo(Writing);
          os.close();
          Mockito.verify(dataBlock[0], Mockito.times(1)).close();
          Assertions.assertThat(dataBlock[0].getState())
              .describedAs(
                  "On close of outputStream, state should become Closed")
              .isEqualTo(Closed);
        }
      }
    }
  }

  /**
   * Creates a file over DFS and attempts to append over Blob.
   * It should fallback to DFS when appending to the file fails.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testCreateOverDfsAppendOverBlob() throws IOException {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_FILE_PATH);
    AzureBlobFileSystemStore.Permissions permissions
        = new AzureBlobFileSystemStore.Permissions(false,
        FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
    fs.getAbfsStore().getClientHandler().getDfsClient().
        createPath(makeQualified(testPath).toUri().getPath(), true, false,
            permissions, false, null,
            null, getTestTracingContext(fs, true));
    fs.getAbfsStore()
        .getAbfsConfiguration()
        .set(FS_AZURE_INGRESS_SERVICE_TYPE, AbfsServiceType.BLOB.name());
    FSDataOutputStream outputStream = fs.append(testPath);
    AzureIngressHandler ingressHandler
        = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
    AbfsClient client = ingressHandler.getClient();
    Assertions.assertThat(client)
        .as("Blob client was not used before fallback")
        .isInstanceOf(AbfsBlobClient.class);
    outputStream.write(TEN);
    outputStream.hsync();
    outputStream.write(TWENTY);
    outputStream.hsync();
    outputStream.write(THIRTY);
    outputStream.hsync();
    AzureIngressHandler ingressHandlerFallback
        = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
    AbfsClient clientFallback = ingressHandlerFallback.getClient();
    Assertions.assertThat(clientFallback)
        .as("DFS client was not used after fallback")
        .isInstanceOf(AbfsDfsClient.class);
  }

  /**
   * Creates a file over Blob and attempts to append over DFS.
   * It should fallback to Blob when appending to the file fails.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testCreateOverBlobAppendOverDfs() throws IOException {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK, true);
    conf.set(FS_AZURE_INGRESS_SERVICE_TYPE,
        String.valueOf(AbfsServiceType.DFS));
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        conf)) {
      Path testPath = path(TEST_FILE_PATH);
      AzureBlobFileSystemStore.Permissions permissions
          = new AzureBlobFileSystemStore.Permissions(false,
          FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
      fs.getAbfsStore()
          .getAbfsConfiguration()
          .setBoolean(FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK, true);
      fs.getAbfsStore()
          .getAbfsConfiguration()
          .set(FS_AZURE_INGRESS_SERVICE_TYPE,
              String.valueOf(AbfsServiceType.DFS));
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(testPath).toUri().getPath(), true, false,
              permissions, false, null,
              null, getTestTracingContext(fs, true));
      FSDataOutputStream outputStream = fs.append(testPath);
      outputStream.write(TEN);
      outputStream.hsync();
      outputStream.write(TWENTY);
      outputStream.hsync();
      outputStream.write(THIRTY);
      outputStream.hsync();
    }
  }

  /**
   * Creates an Append Blob over Blob and attempts to append over DFS.
   * It should fallback to Blob when appending to the file fails.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testCreateAppendBlobOverBlobEndpointAppendOverDfs()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK, true);
    conf.set(FS_AZURE_INGRESS_SERVICE_TYPE,
        String.valueOf(AbfsServiceType.DFS));
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(conf))) {
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      Mockito.doReturn(true).when(store).isAppendBlobKey(anyString());

      // Set abfsStore as our mocked value.
      Field privateField = AzureBlobFileSystem.class.getDeclaredField(
          "abfsStore");
      privateField.setAccessible(true);
      privateField.set(fs, store);
      Path testPath = path(TEST_FILE_PATH);
      AzureBlobFileSystemStore.Permissions permissions
          = new AzureBlobFileSystemStore.Permissions(false,
          FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
      fs.getAbfsStore()
          .getAbfsConfiguration()
          .setBoolean(FS_AZURE_ENABLE_DFSTOBLOB_FALLBACK, true);
      fs.getAbfsStore()
          .getAbfsConfiguration()
          .set(FS_AZURE_INGRESS_SERVICE_TYPE,
              String.valueOf(AbfsServiceType.DFS));
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(testPath).toUri().getPath(), true, false,
              permissions, true, null,
              null, getTestTracingContext(fs, true));
      FSDataOutputStream outputStream = fs.append(testPath);
      outputStream.write(TEN);
      outputStream.hsync();
      outputStream.write(TWENTY);
      outputStream.hsync();
      outputStream.write(THIRTY);
      outputStream.hsync();
    }
  }

  /**
   * Creates an append Blob over DFS and attempts to append over Blob.
   * It should fallback to DFS when appending to the file fails.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Test
  public void testCreateAppendBlobOverDfsEndpointAppendOverBlob()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    Assume.assumeTrue(
        "FNS does not support append blob creation for DFS endpoint",
        getIsNamespaceEnabled(getFileSystem()));
    final AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(true).when(store).isAppendBlobKey(anyString());

    // Set abfsStore as our mocked value.
    Field privateField = AzureBlobFileSystem.class.getDeclaredField(
        "abfsStore");
    privateField.setAccessible(true);
    privateField.set(fs, store);
    Path testPath = path(TEST_FILE_PATH);
    AzureBlobFileSystemStore.Permissions permissions
        = new AzureBlobFileSystemStore.Permissions(false,
        FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
    fs.getAbfsStore().getClientHandler().getDfsClient().
        createPath(makeQualified(testPath).toUri().getPath(), true, false,
            permissions, true, null,
            null, getTestTracingContext(fs, true));
    fs.getAbfsStore()
        .getAbfsConfiguration()
        .set(FS_AZURE_INGRESS_SERVICE_TYPE, AbfsServiceType.BLOB.name());
    FSDataOutputStream outputStream = fs.append(testPath);
    AzureIngressHandler ingressHandler
        = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
    AbfsClient client = ingressHandler.getClient();
    Assertions.assertThat(client)
        .as("Blob client was not used before fallback")
        .isInstanceOf(AbfsBlobClient.class);
    outputStream.write(TEN);
    outputStream.hsync();
    outputStream.write(TWENTY);
    outputStream.hsync();
    outputStream.write(THIRTY);
    outputStream.flush();
    AzureIngressHandler ingressHandlerFallback
        = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
    AbfsClient clientFallback = ingressHandlerFallback.getClient();
    Assertions.assertThat(clientFallback)
        .as("DFS client was not used after fallback")
        .isInstanceOf(AbfsDfsClient.class);
  }


  /**
   * Tests the correct retrieval of the AzureIngressHandler based on the configured ingress service type.
   *
   * @throws IOException if an I/O error occurs
   */
  @Test
  public void testValidateIngressHandler() throws IOException {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_INGRESS_SERVICE_TYPE,
        AbfsServiceType.BLOB.name());
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration)) {
      Path testPath = path(TEST_FILE_PATH);
      AzureBlobFileSystemStore.Permissions permissions
          = new AzureBlobFileSystemStore.Permissions(false,
          FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(testPath).toUri().getPath(), true,
              false,
              permissions, false, null,
              null, getTestTracingContext(fs, true));
      FSDataOutputStream outputStream = fs.append(testPath);
      AzureIngressHandler ingressHandler
          = ((AbfsOutputStream) outputStream.getWrappedStream()).getIngressHandler();
      Assertions.assertThat(ingressHandler)
          .as("Blob Ingress handler instance is not correct")
          .isInstanceOf(AzureBlobIngressHandler.class);
      AbfsClient client = ingressHandler.getClient();
      Assertions.assertThat(client)
          .as("Blob client was not used correctly")
          .isInstanceOf(AbfsBlobClient.class);

      Path testPath1 = new Path("testFile1");
      fs.getAbfsStore().getClientHandler().getBlobClient().
          createPath(makeQualified(testPath1).toUri().getPath(), true,
              false,
              permissions, false, null,
              null, getTestTracingContext(fs, true));
      fs.getAbfsStore()
          .getAbfsConfiguration()
          .set(FS_AZURE_INGRESS_SERVICE_TYPE, AbfsServiceType.DFS.name());
      FSDataOutputStream outputStream1 = fs.append(testPath1);
      AzureIngressHandler ingressHandler1
          = ((AbfsOutputStream) outputStream1.getWrappedStream()).getIngressHandler();
      Assertions.assertThat(ingressHandler1)
          .as("DFS Ingress handler instance is not correct")
          .isInstanceOf(AzureDFSIngressHandler.class);
      AbfsClient client1 = ingressHandler1.getClient();
      Assertions.assertThat(client1)
          .as("Dfs client was not used correctly")
          .isInstanceOf(AbfsDfsClient.class);
    }
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendImplicitDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = new Path(TEST_FOLDER_PATH);
    fs.mkdirs(folderPath);
    fs.append(folderPath.getParent());
  }

  @Test(expected = FileNotFoundException.class)
  public void testAppendFileNotExists() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = new Path(TEST_FOLDER_PATH);
    fs.append(folderPath);
  }

  /**
   * Create directory over dfs endpoint and append over blob endpoint.
   * Should return error as append is not supported for directory.
   * **/
  @Test(expected = IOException.class)
  public void testCreateExplicitDirectoryOverDfsAppendOverBlob()
      throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path folderPath = path(TEST_FOLDER_PATH);
    AzureBlobFileSystemStore.Permissions permissions
        = new AzureBlobFileSystemStore.Permissions(false,
        FsPermission.getDefault(), FsPermission.getUMask(fs.getConf()));
    fs.getAbfsStore().getClientHandler().getDfsClient().
        createPath(makeQualified(folderPath).toUri().getPath(), false, false,
            permissions, false, null,
            null, getTestTracingContext(fs, true));
    FSDataOutputStream outputStream = fs.append(folderPath);
    outputStream.write(TEN);
    outputStream.hsync();
  }

  /**
   * Recreate file between append and flush. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateAppendAndFlush() throws IOException {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.create(filePath);
    Assume.assumeTrue(getIngressServiceType() == AbfsServiceType.BLOB);
    FSDataOutputStream outputStream = fs.append(filePath);
    outputStream.write(TEN);
    try (AzureBlobFileSystem fs1
        = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    FSDataOutputStream outputStream1 = fs1.create(filePath)) {
      outputStream.hsync();
    }
  }

  /**
   * Recreate directory between append and flush. Etag mismatch happens.
   **/
  @Test(expected = IOException.class)
  public void testRecreateDirectoryAppendAndFlush() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    fs.create(filePath);
    FSDataOutputStream outputStream = fs.append(filePath);
    outputStream.write(TEN);
    try (AzureBlobFileSystem fs1
        = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration())) {
      fs1.mkdirs(filePath);
      outputStream.hsync();
    }
  }

  /**
   * Checks a list of futures for exceptions.
   *
   * This method iterates over a list of futures, waits for each task to complete,
   * and handles any exceptions thrown by the lambda expressions. If a
   * RuntimeException is caught, it increments the exceptionCaught counter.
   * If an unexpected exception is caught, it prints the exception to the standard error.
   * Finally, it asserts that no RuntimeExceptions were caught.
   *
   * @param futures The list of futures to check for exceptions.
   */
  private void checkFuturesForExceptions(List<Future<?>> futures, int exceptionVal) {
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
    assertEquals(exceptionCaught, exceptionVal);
  }

  /**
   * Verify that parallel write with same offset from different output streams will not throw exception.
   **/
  @Test
  public void testParallelWriteSameOffsetDifferentOutputStreams()
      throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
     try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration)) {
       ExecutorService executorService = Executors.newFixedThreadPool(5);
       List<Future<?>> futures = new ArrayList<>();

       final byte[] b = new byte[8 * ONE_MB];
       new Random().nextBytes(b);
       final Path filePath = path(TEST_FILE_PATH);
       // Create three output streams
       FSDataOutputStream out1 = fs.create(filePath);
       FSDataOutputStream out2 = fs.append(filePath);
       FSDataOutputStream out3 = fs.append(filePath);

       // Submit tasks to write to each output stream with the same offset
       futures.add(executorService.submit(() -> {
         try {
           out1.write(b, TEN, 2 * HUNDRED);
         } catch (IOException e) {
           throw new RuntimeException(e);
         }
       }));

       futures.add(executorService.submit(() -> {
         try {
           out2.write(b, TEN, 2 * HUNDRED);
         } catch (IOException e) {
           throw new RuntimeException(e);
         }
       }));

       futures.add(executorService.submit(() -> {
         try {
           out3.write(b, TEN, 2 * HUNDRED);
         } catch (IOException e) {
           throw new RuntimeException(e);
         }
       }));

       checkFuturesForExceptions(futures, 0);
     }
  }

  /**
   * Verify that parallel write for different content length will not throw exception.
   **/
  @Test
  public void testParallelWriteDifferentContentLength() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENABLE_CONDITIONAL_CREATE_OVERWRITE, "false");
    try (FileSystem fs = FileSystem.newInstance(configuration)) {
      ExecutorService executorService = Executors.newFixedThreadPool(5);
      List<Future<?>> futures = new ArrayList<>();

      final Path filePath = path(TEST_FILE_PATH);
      // Create three output streams with different content length
      FSDataOutputStream out1 = fs.create(filePath);
      final byte[] b1 = new byte[8 * ONE_MB];
      new Random().nextBytes(b1);

      FSDataOutputStream out2 = fs.append(filePath);
      FSDataOutputStream out3 = fs.append(filePath);

      // Submit tasks to write to each output stream
      futures.add(executorService.submit(() -> {
        try {
          out1.write(b1, TEN, 2 * HUNDRED);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      futures.add(executorService.submit(() -> {
        try {
          out2.write(b1, TWENTY, 3 * HUNDRED);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      futures.add(executorService.submit(() -> {
        try {
          out3.write(b1, THIRTY, 4 * HUNDRED);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }));

      checkFuturesForExceptions(futures, 0);
    }
  }

  /**
   * Verify that parallel write for different content length will not throw exception.
   **/
  @Test
  public void testParallelWriteOutputStreamClose() throws Exception {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    AzureBlobFileSystem fs = getFileSystem();
    final Path secondarytestfile = new Path("secondarytestfile");
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    List<Future<?>> futures = new ArrayList<>();

    FSDataOutputStream out1 = fs.create(secondarytestfile);
    Assume.assumeTrue(getIngressServiceType() == AbfsServiceType.BLOB);
    AbfsOutputStream outputStream1 = (AbfsOutputStream) out1.getWrappedStream();
    String fileETag = outputStream1.getIngressHandler().getETag();
    final byte[] b1 = new byte[8 * ONE_MB];
    new Random().nextBytes(b1);
    final byte[] b2 = new byte[8 * ONE_MB];
    new Random().nextBytes(b2);

    FSDataOutputStream out2 = fs.append(secondarytestfile);

    // Submit tasks to write to each output stream
    futures.add(executorService.submit(() -> {
      try {
        out1.write(b1, 0, 2 * HUNDRED);
        out1.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

    futures.add(executorService.submit(() -> {
      try {
        out2.write(b2, 0, 4 * HUNDRED);
        out2.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }));

   checkFuturesForExceptions(futures, 1);
    // Validate that the data written in the buffer is the same as what was read
    final byte[] readBuffer = new byte[8 * ONE_MB];
    int result;
    FSDataInputStream inputStream = fs.open(secondarytestfile);
    inputStream.seek(0);

    AbfsOutputStream outputStream2 = (AbfsOutputStream) out1.getWrappedStream();
    String out1Etag = outputStream2.getIngressHandler().getETag();

    AbfsOutputStream outputStream3 = (AbfsOutputStream) out2.getWrappedStream();
    String out2Etag = outputStream3.getIngressHandler().getETag();

    if (!fileETag.equals(out1Etag)) {
      result = inputStream.read(readBuffer, 0, 4 * ONE_MB);
      assertEquals(result, 2 * HUNDRED); // Verify that the number of bytes read matches the number of bytes written
      assertArrayEquals(
          Arrays.copyOfRange(readBuffer, 0, result), Arrays.copyOfRange(b1, 0,
              result)); // Verify that the data read matches the original data written
    } else if (!fileETag.equals(out2Etag)) {
      result = inputStream.read(readBuffer, 0, 4 * ONE_MB);
      assertEquals(result, 4 * HUNDRED); // Verify that the number of bytes read matches the number of bytes written
      assertArrayEquals(Arrays.copyOfRange(readBuffer, 0, result),
          Arrays.copyOfRange(b2, 0,
              result)); // Verify that the data read matches the original data written
    } else {
      fail("Neither out1 nor out2 was flushed successfully.");
    }
  }

  /**
   * Verify that once flushed etag changes.
   **/
  @Test
  public void testEtagMismatch() throws Exception {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    AzureBlobFileSystem fs = getFileSystem();
    final Path filePath = path(TEST_FILE_PATH);
    FSDataOutputStream out1 = fs.create(filePath);
    FSDataOutputStream out2 = fs.create(filePath);
    Assume.assumeTrue(getIngressServiceType() == AbfsServiceType.BLOB);
    out2.write(TEN);
    out2.hsync();
    out1.write(TEN);
    intercept(IOException.class, () -> out1.hsync());
  }

  @Test
  public void testAppendWithLease() throws Exception {
    final Path testFilePath = new Path(path(methodName.getMethodName()),
        TEST_FILE_PATH);
    final AzureBlobFileSystem fs = Mockito.spy(
        getCustomFileSystem(testFilePath.getParent(), 1));
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.ALL);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    AbfsOutputStream outputStream = (AbfsOutputStream) fs.getAbfsStore()
        .createFile(testFilePath, null, true,
            permission, umask, getTestTracingContext(fs, true));
    outputStream.write(TEN);
    outputStream.close();
    assertNotNull(outputStream.getLeaseId());
  }

  private AzureBlobFileSystem getCustomFileSystem(Path infiniteLeaseDirs,
      int numLeaseThreads) throws Exception {
    Configuration conf = getRawConfiguration();
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", getAbfsScheme()),
        true);
    conf.set(FS_AZURE_INFINITE_LEASE_KEY, infiniteLeaseDirs.toUri().getPath());
    conf.setInt(FS_AZURE_LEASE_THREADS, numLeaseThreads);
    FileSystem fileSystem = FileSystem.newInstance(conf);
    return (AzureBlobFileSystem) fileSystem;
  }

  @Test
  public void testAppendImplicitDirectoryAzcopy() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFolder(new Path("/src"));
    createAzCopyFile(new Path("/src/file"));
    intercept(FileNotFoundException.class, () -> fs.append(new Path("/src")));
  }

  /**
   * If a write operation fails asynchronously, when the next write comes once failure is
   * registered, that operation would fail with the exception caught on previous
   * write operation.
   * The next close, hsync, hflush would also fail for the last caught exception.
   */
  @Test
  public void testIntermittentAppendFailureToBeReported() throws Exception {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration()))) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      assumeBlobServiceType();

      AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
      AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

      Mockito.doReturn(clientHandler).when(store).getClientHandler();
      Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
      Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();

      Mockito.doThrow(
              new AbfsRestOperationException(HTTP_UNAVAILABLE, "", "", new Exception()))
          .when(blobClient)
          .append(Mockito.anyString(), Mockito.any(byte[].class), Mockito.any(
                  AppendRequestParameters.class), Mockito.any(), Mockito.any(),
              Mockito.any(TracingContext.class));

      byte[] bytes = new byte[1024 * 1024 * 8];
      new Random().nextBytes(bytes);

      LambdaTestUtils.intercept(IOException.class, () -> {
        try (FSDataOutputStream os = createMockedOutputStream(fs,
            new Path("/test/file"), blobClient)) {
          os.write(bytes);
        }
      });

      LambdaTestUtils.intercept(IOException.class, () -> {
        FSDataOutputStream os = createMockedOutputStream(fs,
            new Path("/test/file/file1"), blobClient);
        os.write(bytes);
        os.close();
      });

      LambdaTestUtils.intercept(IOException.class, () -> {
        FSDataOutputStream os = createMockedOutputStream(fs,
            new Path("/test/file/file2"), blobClient);
        os.write(bytes);
        os.hsync();
      });

      LambdaTestUtils.intercept(IOException.class, () -> {
        FSDataOutputStream os = createMockedOutputStream(fs,
            new Path("/test/file/file3"), blobClient);
        os.write(bytes);
        os.hflush();
      });

      LambdaTestUtils.intercept(IOException.class, () -> {
        AbfsOutputStream os = (AbfsOutputStream) createMockedOutputStream(fs,
            new Path("/test/file/file4"), blobClient).getWrappedStream();
        os.write(bytes);
        while (!os.areWriteOperationsTasksDone()) {
          // No operation inside the loop
        }
        os.write(bytes);
      });
    }
  }

  /**
   * Creates a mocked FSDataOutputStream for testing purposes.
   *
   * This method creates a mocked FSDataOutputStream by wrapping an AbfsOutputStream
   * and its associated AzureIngressHandler. The method uses Mockito to create spies
   * for the AbfsOutputStream and AzureIngressHandler, and sets up the necessary
   * interactions between them.
   *
   * @param fs The AzureBlobFileSystem instance used to create the output stream.
   * @param path The Path where the output stream will be created.
   * @param client The AbfsClient instance to be used by the AzureIngressHandler.
   * @return A mocked FSDataOutputStream instance.
   * @throws IOException If an I/O error occurs while creating the output stream.
   */
  private FSDataOutputStream createMockedOutputStream(AzureBlobFileSystem fs,
      Path path,
      AbfsClient client) throws IOException {
    AbfsOutputStream abfsOutputStream = Mockito.spy(
        (AbfsOutputStream) fs.create(path).getWrappedStream());
    AzureIngressHandler ingressHandler = Mockito.spy(
        abfsOutputStream.getIngressHandler());
    Mockito.doReturn(ingressHandler).when(abfsOutputStream).getIngressHandler();
    Mockito.doReturn(client).when(ingressHandler).getClient();

    FSDataOutputStream fsDataOutputStream = Mockito.spy(
        new FSDataOutputStream(abfsOutputStream, null));
    return fsDataOutputStream;
  }

  /**
   * Test to check when async write takes time, the close, hsync, hflush method
   * wait to get async ops completed and then flush. If async ops fail, the methods
   * will throw exception.
   */
  @Test
  public void testWriteAsyncOpFailedAfterCloseCalled() throws Exception {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration()))) {
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
      AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());
      AbfsDfsClient dfsClient = Mockito.spy(clientHandler.getDfsClient());

      AbfsClient client = clientHandler.getIngressClient();
      if (clientHandler.getIngressClient() instanceof AbfsBlobClient) {
        Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
        Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();
      } else {
        Mockito.doReturn(dfsClient).when(clientHandler).getDfsClient();
        Mockito.doReturn(dfsClient).when(clientHandler).getIngressClient();
      }
      Mockito.doReturn(clientHandler).when(store).getClientHandler();

      byte[] bytes = new byte[1024 * 1024 * 8];
      new Random().nextBytes(bytes);

      AtomicInteger count = new AtomicInteger(0);

      Mockito.doAnswer(answer -> {
            count.incrementAndGet();
            while (count.get() < 2) {
              // No operation inside the loop
            }
            Thread.sleep(10 * HUNDRED);
            throw new AbfsRestOperationException(HTTP_UNAVAILABLE, "", "",
                new Exception());
          })
          .when(client instanceof AbfsBlobClient ? blobClient : dfsClient)
          .append(Mockito.anyString(), Mockito.any(byte[].class), Mockito.any(
                  AppendRequestParameters.class), Mockito.any(), Mockito.any(),
              Mockito.any(TracingContext.class));

      Mockito.doAnswer(answer -> {
            count.incrementAndGet();
            while (count.get() < 2) {
              // No operation inside the loop
            }
            Thread.sleep(10 * HUNDRED);
            throw new AbfsRestOperationException(HTTP_UNAVAILABLE, "", "",
                new Exception());
          })
          .when(client instanceof AbfsBlobClient ? blobClient : dfsClient)
          .append(Mockito.anyString(), Mockito.any(byte[].class), Mockito.any(
                  AppendRequestParameters.class), Mockito.any(), Mockito.any(),
              Mockito.any(TracingContext.class));

      FSDataOutputStream os = createMockedOutputStream(fs,
          new Path("/test/file"),
          client instanceof AbfsBlobClient ? blobClient : dfsClient);
      os.write(bytes);
      os.write(bytes);
      LambdaTestUtils.intercept(IOException.class, os::close);

      count.set(0);
      FSDataOutputStream os1 = createMockedOutputStream(fs,
          new Path("/test/file1"),
          client instanceof AbfsBlobClient ? blobClient : dfsClient);
      os1.write(bytes);
      os1.write(bytes);
      LambdaTestUtils.intercept(IOException.class, os1::hsync);

      count.set(0);
      FSDataOutputStream os2 = createMockedOutputStream(fs,
          new Path("/test/file2"),
          client instanceof AbfsBlobClient ? blobClient : dfsClient);
      os2.write(bytes);
      os2.write(bytes);
      LambdaTestUtils.intercept(IOException.class, os2::hflush);
    }
  }

  /**
   * Helper method that generates blockId.
   * @param position The offset needed to generate blockId.
   * @return String representing the block ID generated.
   */
  private String generateBlockId(AbfsOutputStream os, long position) {
    String streamId = os.getStreamID();
    String streamIdHash = Integer.toString(streamId.hashCode());
    String blockId = String.format("%d_%s", position, streamIdHash);
    byte[] blockIdByteArray = new byte[BLOCK_ID_LENGTH];
    System.arraycopy(blockId.getBytes(), 0, blockIdByteArray, 0, Math.min(BLOCK_ID_LENGTH, blockId.length()));
    return new String(Base64.encodeBase64(blockIdByteArray), StandardCharsets.UTF_8);
  }

  /**
   * Test to simulate a successful flush operation followed by a connection reset
   * on the response, triggering a retry.
   *
   * This test verifies that the flush operation is retried in the event of a
   * connection reset during the response phase. The test creates a mock
   * AzureBlobFileSystem and its associated components to simulate the flush
   * operation and the connection reset. It then verifies that the flush
   * operation is retried once before succeeding if the md5hash matches.
   *
   * @throws Exception if an error occurs during the test execution.
   */
  @Test
  public void testFlushSuccessWithConnectionResetOnResponseValidMd5() throws Exception {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    // Create a spy of AzureBlobFileSystem
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration()))) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));

      // Create a spy of AzureBlobFileSystemStore
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      assumeBlobServiceType();

      // Create spies for the client handler and blob client
      AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
      AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

      // Set up the spies to return the mocked objects
      Mockito.doReturn(clientHandler).when(store).getClientHandler();
      Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
      Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();
      AtomicInteger flushCount = new AtomicInteger(0);
      FSDataOutputStream os = createMockedOutputStream(fs,
          new Path("/test/file"), blobClient);
      AbfsOutputStream out = (AbfsOutputStream) os.getWrappedStream();
      String eTag = out.getIngressHandler().getETag();
      byte[] bytes = new byte[1024 * 1024 * 8];
      new Random().nextBytes(bytes);
      // Write some bytes and attempt to flush, which should retry
      out.write(bytes);
      List<String> list = new ArrayList<>();
      list.add(generateBlockId(out, 0));
      String blockListXml = generateBlockListXml(list);

      Mockito.doAnswer(answer -> {
        // Set up the mock for the flush operation
        AbfsClientTestUtil.setMockAbfsRestOperationForFlushOperation(blobClient,
            eTag, blockListXml,
            (httpOperation) -> {
              Mockito.doAnswer(invocation -> {
                // Call the real processResponse method
                invocation.callRealMethod();

                int currentCount = flushCount.incrementAndGet();
                if (currentCount == 1) {
                  Mockito.when(httpOperation.getStatusCode())
                      .thenReturn(
                          HTTP_INTERNAL_ERROR); // Status code 500 for Internal Server Error
                  Mockito.when(httpOperation.getStorageErrorMessage())
                      .thenReturn("CONNECTION_RESET"); // Error message
                  throw new IOException("Connection Reset");
                }
                return null;
              }).when(httpOperation).processResponse(
                  Mockito.nullable(byte[].class),
                  Mockito.anyInt(),
                  Mockito.anyInt()
              );

              return httpOperation;
            });
        return answer.callRealMethod();
      }).when(blobClient).flush(
          Mockito.any(byte[].class),
          Mockito.anyString(),
          Mockito.anyBoolean(),
          Mockito.nullable(String.class),
          Mockito.nullable(String.class),
          Mockito.anyString(),
          Mockito.nullable(ContextEncryptionAdapter.class),
          Mockito.any(TracingContext.class)
      );

      out.hsync();
      out.close();
      Mockito.verify(blobClient, Mockito.times(1)).flush(
          Mockito.any(byte[].class),
          Mockito.anyString(),
          Mockito.anyBoolean(),
          Mockito.nullable(String.class),
          Mockito.nullable(String.class),
          Mockito.anyString(),
          Mockito.nullable(ContextEncryptionAdapter.class),
          Mockito.any(TracingContext.class));
    }
  }

  /**
   * Test to simulate a successful flush operation followed by a connection reset
   * on the response, triggering a retry.
   *
   * This test verifies that the flush operation is retried in the event of a
   * connection reset during the response phase. The test creates a mock
   * AzureBlobFileSystem and its associated components to simulate the flush
   * operation and the connection reset. It then verifies that the flush
   * operation is retried once before succeeding if the md5hash matches.
   *
   * @throws Exception if an error occurs during the test execution.
   */
  @Test
  public void testFlushSuccessWithConnectionResetOnResponseInvalidMd5() throws Exception {
    Assume.assumeFalse("Not valid for APPEND BLOB", isAppendBlobEnabled());
    // Create a spy of AzureBlobFileSystem
    try (AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration()))) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));

      // Create a spy of AzureBlobFileSystemStore
      AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
      assumeBlobServiceType();

      // Create spies for the client handler and blob client
      AbfsClientHandler clientHandler = Mockito.spy(store.getClientHandler());
      AbfsBlobClient blobClient = Mockito.spy(clientHandler.getBlobClient());

      // Set up the spies to return the mocked objects
      Mockito.doReturn(clientHandler).when(store).getClientHandler();
      Mockito.doReturn(blobClient).when(clientHandler).getBlobClient();
      Mockito.doReturn(blobClient).when(clientHandler).getIngressClient();
      AtomicInteger flushCount = new AtomicInteger(0);
      FSDataOutputStream os = createMockedOutputStream(fs,
          new Path("/test/file"), blobClient);
      AbfsOutputStream out = (AbfsOutputStream) os.getWrappedStream();
      String eTag = out.getIngressHandler().getETag();
      byte[] bytes = new byte[1024 * 1024 * 8];
      new Random().nextBytes(bytes);
      // Write some bytes and attempt to flush, which should retry
      out.write(bytes);
      List<String> list = new ArrayList<>();
      list.add(generateBlockId(out, 0));
      String blockListXml = generateBlockListXml(list);

      Mockito.doAnswer(answer -> {
        // Set up the mock for the flush operation
        AbfsClientTestUtil.setMockAbfsRestOperationForFlushOperation(blobClient,
            eTag, blockListXml,
            (httpOperation) -> {
              Mockito.doAnswer(invocation -> {
                // Call the real processResponse method
                invocation.callRealMethod();

                int currentCount = flushCount.incrementAndGet();
                if (currentCount == 1) {
                  Mockito.when(httpOperation.getStatusCode())
                      .thenReturn(
                          HTTP_INTERNAL_ERROR); // Status code 500 for Internal Server Error
                  Mockito.when(httpOperation.getStorageErrorMessage())
                      .thenReturn("CONNECTION_RESET"); // Error message
                  throw new IOException("Connection Reset");
                } else if (currentCount == 2) {
                  Mockito.when(httpOperation.getStatusCode())
                      .thenReturn(HTTP_OK);
                  Mockito.when(httpOperation.getStorageErrorMessage())
                      .thenReturn("HTTP_OK");
                }
                return null;
              }).when(httpOperation).processResponse(
                  Mockito.nullable(byte[].class),
                  Mockito.anyInt(),
                  Mockito.anyInt()
              );

              return httpOperation;
            });
        return answer.callRealMethod();
      }).when(blobClient).flush(
          Mockito.any(byte[].class),
          Mockito.anyString(),
          Mockito.anyBoolean(),
          Mockito.nullable(String.class),
          Mockito.nullable(String.class),
          Mockito.anyString(),
          Mockito.nullable(ContextEncryptionAdapter.class),
          Mockito.any(TracingContext.class)
      );

      FSDataOutputStream os1 = createMockedOutputStream(fs,
          new Path("/test/file"), blobClient);
      AbfsOutputStream out1 = (AbfsOutputStream) os1.getWrappedStream();
      byte[] bytes1 = new byte[1024 * 1024 * 8];
      new Random().nextBytes(bytes1);
      out1.write(bytes1);

      //parallel flush call should lead to the first call failing because of md5 mismatch.
      Thread parallelFlushThread = new Thread(() -> {
        try {
          out1.hsync();
        } catch (IOException e) {
        }
      });

      parallelFlushThread.start(); // Start the parallel flush operation
      parallelFlushThread.join();
      // Perform the first flush operation
      intercept(IOException.class,
          "The condition specified using HTTP conditional header(s) is not met.",
          out::hsync
      );
    }
  }
}
