/*
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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Test class for ReadBufferManagerV2 functionality.
 */
public class ITestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  private static final String TEST_FILE_NAME_PREFIX = "testFile";
  private static final int LESS_NUM_FILES = 1;
  private static final int SMALL_FILE_SIZE = 3 * ONE_MB;

  public ITestReadBufferManagerV2() throws Exception {
  }

  /**
   * Test to verify that ReadBufferManagerV2 can read different files concurrently.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadBufferManagerV2() throws Exception {
    try (AzureBlobFileSystem fs = getConfiguredFileSystem(true)) {
      AzureBlobFileSystem spiedFs = spy(fs);
      AzureBlobFileSystemStore spiedStore = spy(fs.getAbfsStore());
      AbfsClient spiedClient = spy(spiedStore.getClient());
      doReturn(spiedClient).when(spiedStore).getClient();
      doReturn(spiedStore).when(spiedFs).getAbfsStore();
      int numOfFiles = LESS_NUM_FILES;
      int fileSize = SMALL_FILE_SIZE;
      Path[] testPaths = createFilesWithContent(spiedFs, numOfFiles);
      ExecutorService executorService = Executors.newFixedThreadPool(
          numOfFiles);

      int[] fileIdx = new int[1];
      try {
        for (int i = 0; i < numOfFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = spiedFs.open(
                testPaths[fileIdx[0]++])) {
              int bytesRead = iStream.read(new byte[fileSize], 0,
                  fileSize);
              Assertions.assertEquals(fileSize, bytesRead,
                  "Read size should match file size");
            }
            return null;
          });
        }
      } catch (Exception e) {
        System.out.println(
            "Exception occurred during file read: " + e.getMessage());
      } finally {
        shutdownExecutorService(executorService);
      }

      int expectedReadCalls = numOfFiles * (int)Math.ceil((double)fileSize/getConfiguration().getReadBufferSize());
      verify(spiedClient, atLeast(expectedReadCalls)).read(anyString(),
         anyLong(), any(), anyInt(), anyInt(), anyString(), anyString(),
          nullable(ContextEncryptionAdapter.class), any(TracingContext.class));
    }
  }

  /**
   * Test to verify that ReadBufferManagerV2 can read different files concurrently.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadSameFileFromMultipleStreamsWithV2Disabled() throws Exception {
    try (AzureBlobFileSystem fs = getConfiguredFileSystem(false)) {
      AzureBlobFileSystem spiedFs = spy(fs);
      AzureBlobFileSystemStore spiedStore = spy(fs.getAbfsStore());
      AbfsClient spiedClient = spy(spiedStore.getClient());
      doReturn(spiedClient).when(spiedStore).getClient();
      doReturn(spiedStore).when(spiedFs).getAbfsStore();
      int numOfFiles = LESS_NUM_FILES;
      int fileSize = SMALL_FILE_SIZE;
      Path[] testPaths = createFilesWithContent(spiedFs, 1);
      ExecutorService executorService = Executors.newFixedThreadPool(
          numOfFiles);

      try {
        for (int i = 0; i < numOfFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = spiedFs.open(
                testPaths[0])) {
              int bytesRead = iStream.read(new byte[fileSize], 0,
                  fileSize);
              Assertions.assertEquals(fileSize, bytesRead,
                  "Read size should match file size");
            }
            return null;
          });
        }
      } catch (Exception e) {
        System.out.println(
            "Exception occurred during file read: " + e.getMessage());
      } finally {
        shutdownExecutorService(executorService);
      }

      int expectedReadCalls = (numOfFiles * fileSize)
          / getConfiguration().getReadBufferSize();
      verify(spiedClient, atLeast(expectedReadCalls)).read(eq(testPaths[0].getName()),
          anyLong(), any(), anyInt(), anyInt(), anyString(), anyString(),
          nullable(ContextEncryptionAdapter.class), any(TracingContext.class));
    }
  }

  /**
   * Test to verify that ReadBufferManagerV2 can read different files concurrently.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadSameFileFromMultipleStreamsWithV2Enabled() throws Exception {
    try (AzureBlobFileSystem fs = getConfiguredFileSystem(true)) {
      AzureBlobFileSystem spiedFs = spy(fs);
      AzureBlobFileSystemStore spiedStore = spy(fs.getAbfsStore());
      AbfsClient spiedClient = spy(spiedStore.getClient());
      doReturn(spiedClient).when(spiedStore).getClient();
      doReturn(spiedStore).when(spiedFs).getAbfsStore();
      int numOfFiles = LESS_NUM_FILES;
      int fileSize = SMALL_FILE_SIZE;
      Path[] testPaths = createFilesWithContent(spiedFs, numOfFiles);
      ExecutorService executorService = Executors.newFixedThreadPool(
          numOfFiles);

      int[] fileIdx = new int[1];
      try {
        for (int i = 0; i < numOfFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = spiedFs.open(
                testPaths[fileIdx[0]++])) {
              int bytesRead = iStream.read(new byte[fileSize], 0,
                  fileSize);
              Assertions.assertEquals(fileSize, bytesRead,
                  "Read size should match file size");
            }
            return null;
          });
        }
      } catch (Exception e) {
        System.out.println(
            "Exception occurred during file read: " + e.getMessage());
      } finally {
        shutdownExecutorService(executorService);
      }

      int expectedReadCalls = fileSize / getConfiguration().getReadBufferSize();
      verify(spiedClient, atLeast(expectedReadCalls)).read(eq(testPaths[0].getName()),
          anyLong(), any(), anyInt(), anyInt(), anyString(), anyString(),
          nullable(ContextEncryptionAdapter.class), any(TracingContext.class));
    }
  }

  private AzureBlobFileSystem getConfiguredFileSystem(boolean isReadAheahdV2Enabled) throws IOException {
    Configuration conf = getConfiguration().getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, isReadAheahdV2Enabled);
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, isReadAheahdV2Enabled);
    return (AzureBlobFileSystem) FileSystem.newInstance(conf);
  }

  private Path[] createFilesWithContent(FileSystem fs,
      int numFiles) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(numFiles);
    Path[] tesFilePaths = new Path[numFiles];
    int[] fileIdx = new int[1];
    try {
      for (int i = 0; i < numFiles; i++) {
        final String fileName = ITestReadBufferManagerV2.TEST_FILE_NAME_PREFIX + i;
        executorService.submit((Callable<Void>) () -> {
          byte[] fileContent = getRandomBytesArray(
              ITestReadBufferManagerV2.SMALL_FILE_SIZE);
          tesFilePaths[fileIdx[0]++] = createFileWithContent(fs, fileName, fileContent);
          return null;
        });
      }
    } finally {
      shutdownExecutorService(executorService);
    }
    return tesFilePaths;
  }

  private void shutdownExecutorService(ExecutorService executorService) {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }
  }
}
