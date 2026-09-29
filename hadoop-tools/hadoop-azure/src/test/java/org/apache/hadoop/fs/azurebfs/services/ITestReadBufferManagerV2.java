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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.assertj.core.api.Assertions.assertThat;

public class ITestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  private static final int LESS_NUM_FILES = 2;
  private static final int MORE_NUM_FILES = 5;
  private static final int SMALL_FILE_SIZE = 6 * ONE_MB;
  private static final int LARGE_FILE_SIZE = 50 * ONE_MB;
  private static final int BLOCK_SIZE = 4 * ONE_MB;

  public ITestReadBufferManagerV2() throws Exception {
  }

  @Test
  public void testReadDifferentFilesInParallel() throws Exception {
    try (AzureBlobFileSystem fs = getConfiguredFileSystem()) {
      int fileSize = LARGE_FILE_SIZE;
      int numFiles = MORE_NUM_FILES;
      byte[] fileContent = getRandomBytesArray(fileSize);

      Path[] testPaths = new Path[numFiles];
      int[] idx = {0};
      for (int i = 0; i < numFiles; i++) {
        final String fileName = methodName.getMethodName() + i;
        testPaths[i] = createFileWithContent(fs, fileName, fileContent);
      }
      ExecutorService executorService = Executors.newFixedThreadPool(numFiles);
      Map<String, Long> metricMap = getInstrumentationMap(fs);
      long requestsMadeBeforeTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());
      try {
        for (int i = 0; i < numFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = fs.open(testPaths[idx[0]++])) {
              byte[] buffer = new byte[fileSize];
              int bytesRead = iStream.read(buffer, 0, fileSize);
              assertThat(bytesRead).isEqualTo(fileSize);
              assertThat(buffer).isEqualTo(fileContent);
            }
            return null;
          });
        }
      } finally {
        executorService.shutdown();
        // wait for all tasks to finish
        executorService.awaitTermination(1, TimeUnit.MINUTES);
      }
      metricMap = getInstrumentationMap(fs);
      long requestsMadeAfterTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());
      int expectedRequests = numFiles // Get Path Status for each file
          + ((int) Math.ceil((double) fileSize / BLOCK_SIZE))
          * numFiles; // Read requests for each file
      assertEquals(expectedRequests,
          requestsMadeAfterTest - requestsMadeBeforeTest);
    }
  }

  @Test
  public void testReadSameFileInParallel() throws Exception {
    try (AzureBlobFileSystem fs = getConfiguredFileSystem()) {
      int fileSize = SMALL_FILE_SIZE;
      int numFiles = LESS_NUM_FILES;
      byte[] fileContent = getRandomBytesArray(fileSize);

      final String fileName = methodName.getMethodName();
      Path testPath = createFileWithContent(fs, fileName, fileContent);
      ExecutorService executorService = Executors.newFixedThreadPool(numFiles);
      Map<String, Long> metricMap = getInstrumentationMap(fs);
      long requestsMadeBeforeTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());
      try {
        for (int i = 0; i < numFiles; i++) {
          executorService.submit((Callable<Void>) () -> {
            try (FSDataInputStream iStream = fs.open(testPath)) {
              byte[] buffer = new byte[fileSize];
              int bytesRead = iStream.read(buffer, 0, fileSize);
              assertThat(bytesRead).isEqualTo(fileSize);
              assertThat(buffer).isEqualTo(fileContent);
            }
            return null;
          });
        }
      } finally {
        executorService.shutdown();
        // wait for all tasks to finish
        executorService.awaitTermination(1, TimeUnit.MINUTES);
      }
      metricMap = getInstrumentationMap(fs);
      long requestsMadeAfterTest = metricMap
          .get(CONNECTIONS_MADE.getStatName());
      int expectedRequests = numFiles // Get Path Status for each file
          + ((int) Math.ceil(
          (double) fileSize / BLOCK_SIZE)); // Read requests for each file
      assertEquals(expectedRequests,
          requestsMadeAfterTest - requestsMadeBeforeTest);
    }
  }

  private AzureBlobFileSystem getConfiguredFileSystem() throws Exception {
    Configuration config = new Configuration(getRawConfiguration());
    config.set(FS_AZURE_ENABLE_READAHEAD_V2, TRUE);
    config.set(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, TRUE);
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(config);
    return fs;
  }
}
