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

package org.apache.hadoop.fs.s3a.prefetch;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import org.apache.hadoop.fs.impl.prefetch.ExceptionAsserts;
import org.apache.hadoop.fs.impl.prefetch.ExecutorServiceFuturePool;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamCallbacks;
import org.apache.hadoop.test.AbstractHadoopTestBase;

public class TestS3ARemoteObject extends AbstractHadoopTestBase {

  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);

  private final ExecutorServiceFuturePool futurePool =
      new ExecutorServiceFuturePool(threadPool);

  private final ObjectInputStreamCallbacks client =
      MockS3ARemoteObject.createClient("bucket");

  @Test
  public void testArgChecks() throws Exception {
    S3AReadOpContext readContext =
        S3APrefetchFakes.createReadContext(futurePool, "key", 10);
    S3ObjectAttributes attrs =
        S3APrefetchFakes.createObjectAttributes("bucket", "key", 10);
    S3AInputStreamStatistics stats =
        readContext.getS3AStatisticsContext().newInputStreamStatistics();
    ChangeTracker changeTracker =
        S3APrefetchFakes.createChangeTracker("bucket", "key", 10);

    // Should not throw.
    new S3ARemoteObject(readContext, attrs, client, stats, changeTracker);

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'context' must not be null",
        () -> new S3ARemoteObject(null, attrs, client, stats, changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'s3Attributes' must not be null",
        () -> new S3ARemoteObject(readContext, null, client, stats,
            changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'client' must not be null",
        () -> new S3ARemoteObject(readContext, attrs, null, stats,
            changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'streamStatistics' must not be null",
        () -> new S3ARemoteObject(readContext, attrs, client, null,
            changeTracker));

    ExceptionAsserts.assertThrows(
        IllegalArgumentException.class,
        "'changeTracker' must not be null",
        () -> new S3ARemoteObject(readContext, attrs, client, stats, null));
  }
}
