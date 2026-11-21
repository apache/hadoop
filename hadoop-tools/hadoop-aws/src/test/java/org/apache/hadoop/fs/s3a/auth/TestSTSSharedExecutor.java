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

package org.apache.hadoop.fs.s3a.auth;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.s3a.impl.LazySharedThreadPoolHolder;

import static org.apache.hadoop.fs.s3a.Constants.AWS_STS_CLIENT_SHARED_THREADPOOL_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.AWS_STS_CLIENT_SHARED_THREADPOOL_KEEPALIVE;
import static org.apache.hadoop.fs.s3a.Constants.AWS_STS_CLIENT_SHARED_THREADPOOL_SIZE;

public class TestSTSSharedExecutor {

  @Test
  public void testStsExecutorHolderWiredWithExpectedKeys() {
    LazySharedThreadPoolHolder holder = STSClientFactory.stsExecutorHolder();
    Assertions.assertThat(holder.getConfigKeys())
        .as("enabled, size and keepalive keys")
        .containsExactly(AWS_STS_CLIENT_SHARED_THREADPOOL_ENABLED,
            AWS_STS_CLIENT_SHARED_THREADPOOL_SIZE,
            AWS_STS_CLIENT_SHARED_THREADPOOL_KEEPALIVE);
  }
}
