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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestReadBufferManagerV2 extends AbstractAbfsIntegrationTest {

  public TestReadBufferManagerV2() throws Exception {
    super();
  }

  @Test
  public void testReadBufferManagerV2Init() throws Exception {
    Assertions.assertThat(ReadBufferManagerV2.getInstance()).isNull();
    intercept(IllegalStateException.class, "ReadBufferManagerV2 is not configured.", () -> {
      ReadBufferManagerV2.getBufferManager();
    });
    ReadBufferManagerV2.setReadBufferManagerConfigs(
        getConfiguration().getReadAheadBlockSize(), getConfiguration());
    ReadBufferManagerV2 bufferManager = ReadBufferManagerV2.getBufferManager();
    ReadBufferManagerV2 bufferManager2 = ReadBufferManagerV2.getBufferManager();
    Assertions.assertThat(bufferManager).isNotNull();
    Assertions.assertThat(bufferManager2).isNotNull();
    Assertions.assertThat(bufferManager).isSameAs(bufferManager2);
    Assertions.assertThat(ReadBufferManagerV2.getInstance()).isNotNull();
    Assertions.assertThat(ReadBufferManagerV2.getInstance()).isSameAs(bufferManager);

    Assertions.assertThat(bufferManager.getMinBufferPoolSize()).isGreaterThan(0);
    Assertions.assertThat(bufferManager.getMaxBufferPoolSize()).isGreaterThan(0);
    Assertions.assertThat(bufferManager.getCurrentThreadPoolSize()).isEqualTo(bufferManager.getMinThreadPoolSize());
  }
}
