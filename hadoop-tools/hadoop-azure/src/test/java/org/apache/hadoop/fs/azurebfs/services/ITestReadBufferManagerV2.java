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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_BLOCK_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

/**
 * Test class for ReadBufferManagerV2 functionality.
 */
public class ITestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  private static final String TEST_FILE_NAME_PREFIX = "testFile";
  private static final int LESS_NUM_FILES = 5;
  private static final int SMALL_FILE_SIZE = 30 * ONE_MB;

  public ITestReadBufferManagerV2() throws Exception {
    super();
    getConfiguration().setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
  }

  /**
   * Test to verify that ReadBufferManagerV2 can read different files concurrently.
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testReadBufferManagerV2() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(FS_AZURE_READ_AHEAD_BLOCK_SIZE, String.valueOf(4 * ONE_MB));
    try (AzureBlobFileSystem fs = getConfiguredFileSystem(true, configMap)) {
      int numOfFiles = LESS_NUM_FILES;
      Path[] testPaths = createFilesWithContent(fs, numOfFiles);
      ExecutorService executorService = Executors.newFixedThreadPool(
          numOfFiles);

      int[] fileIdx = new int[1];
      try {
        for (int i = 0; i < numOfFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = fs.open(testPaths[fileIdx[0]++])) {
              int bytesRead = iStream.read(new byte[SMALL_FILE_SIZE], 0,
                  SMALL_FILE_SIZE);
              Assertions.assertEquals(SMALL_FILE_SIZE, bytesRead,
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
    }
  }

  private AzureBlobFileSystem getConfiguredFileSystem(boolean isRAV2Enabled,
      Map<String, String> configurations) throws IOException {
    Configuration conf = getConfiguration().getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, isRAV2Enabled);
    for (Map.Entry<String, String> entry : configurations.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

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
