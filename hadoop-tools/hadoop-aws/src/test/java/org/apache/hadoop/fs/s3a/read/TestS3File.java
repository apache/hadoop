/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.read;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.common.ExecutorServiceFuturePool;
import org.junit.Test;

import org.apache.hadoop.fs.common.ExceptionAsserts;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.test.AbstractHadoopTestBase;

public class TestS3File extends AbstractHadoopTestBase {
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private final ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(threadPool);
  private final S3AInputStream.InputStreamCallbacks client = MockS3File.createClient("bucket");

  @Test
  public void testArgChecks() throws Exception {
    S3AReadOpContext readContext = Fakes.createReadContext(futurePool, "key", 10, 10, 1);
    S3ObjectAttributes attrs = Fakes.createObjectAttributes("bucket", "key", 10);
    S3AInputStreamStatistics stats =
        readContext.getS3AStatisticsContext().newInputStreamStatistics();
    ChangeTracker changeTracker = Fakes.createChangeTracker("bucket", "key", 10);

    // Should not throw.
    new S3File(readContext, attrs, client, stats, changeTracker);

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'context' must not be null",
        () -> new S3File(null, attrs, client, stats, changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'s3Attributes' must not be null",
        () -> new S3File(readContext, null, client, stats, changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'client' must not be null",
        () -> new S3File(readContext, attrs, null, stats, changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'streamStatistics' must not be null",
        () -> new S3File(readContext, attrs, client, null, changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'changeTracker' must not be null",
        () -> new S3File(readContext, attrs, client, stats, null));
  }
}
