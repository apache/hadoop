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

import static org.junit.Assert.*;

import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.FuturePool;
import org.apache.hadoop.fs.common.ExceptionAsserts;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3FileTest {
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  private final FuturePool futurePool = new ExecutorServiceFuturePool(threadPool);
  private final S3AInputStream.InputStreamCallbacks client = TestS3File.createClient("bucket");

  @Test
  public void testArgChecks() {
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

  @Test
  public void testProperties() throws IOException {
    // S3File file = new S3File(client, "bucket", "key", 10);

    // assertEquals("s3://bucket/key", file.getPath());
    // assertEquals(10, file.size());
  }
}
