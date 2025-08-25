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
package org.apache.hadoop.fs.azurebfs.services;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit Tests around different components of Read Buffer Manager V2
 */
public class TestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  public TestReadBufferManagerV2() throws Exception {
    super();
  }

  /**
   * Test to verify init of ReadBufferManagerV2
   * @throws Exception if test fails
   */
  @Test
  public void testReadBufferManagerV2Init() throws Exception {
    assertThat(ReadBufferManagerV2.getInstance())
        .as("ReadBufferManager should be uninitialized").isNull();
    intercept(IllegalStateException.class, "ReadBufferManagerV2 is not configured.", () -> {
      ReadBufferManagerV2.getBufferManager();
    });
    ReadBufferManagerV2.setReadBufferManagerConfigs(
        getConfiguration().getReadAheadBlockSize(), getConfiguration());
    // verify that multiple invocations of getBufferManager returns same instance.
    ReadBufferManagerV2 bufferManager = ReadBufferManagerV2.getBufferManager();
    ReadBufferManagerV2 bufferManager2 = ReadBufferManagerV2.getBufferManager();
    ReadBufferManagerV2 bufferManager3 = ReadBufferManagerV2.getInstance();
    assertThat(bufferManager).isNotNull();
    assertThat(bufferManager2).isNotNull();
    assertThat(bufferManager).isSameAs(bufferManager2);
    assertThat(bufferManager3).isNotNull();
    assertThat(bufferManager3).isSameAs(bufferManager);

    // Verify default values are not invalid.
    assertThat(bufferManager.getMinBufferPoolSize()).isGreaterThan(0);
    assertThat(bufferManager.getMaxBufferPoolSize()).isGreaterThan(0);
  }

  /**
   * Test to verify that cpu monitor thread is not active if disabled.
   * @throws Exception if test fails
   */
  @Test
  public void testDynamicScalingSwitchingOnAndOff() throws Exception {
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2, true);
    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, true);
    try(AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(getFileSystem().getUri(), conf)) {
      AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
      ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfiguration.getReadAheadBlockSize(), abfsConfiguration);
      ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager();
      assertThat(bufferManagerV2.getCpuMonitoringThread())
          .as("CPU Monitor thread should be initialized").isNotNull();
    }

    conf.setBoolean(FS_AZURE_ENABLE_READAHEAD_V2_DYNAMIC_SCALING, false);
    try(AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(getFileSystem().getUri(), conf)) {
      AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
      ReadBufferManagerV2.setReadBufferManagerConfigs(abfsConfiguration.getReadAheadBlockSize(), abfsConfiguration);
      ReadBufferManagerV2 bufferManagerV2 = ReadBufferManagerV2.getBufferManager();
      assertThat(bufferManagerV2.getCpuMonitoringThread())
          .as("CPU Monitor thread should not be initialized").isNull();
    }
  }

  /**
   * Test to verify that prefetch for same file and same position is not queued
   * even when attempted by different input streams instances.
   * @throws Exception if test fails
   */
  @Test
  public void testPrefetchAlreadyQueued() throws Exception {

  }
}
