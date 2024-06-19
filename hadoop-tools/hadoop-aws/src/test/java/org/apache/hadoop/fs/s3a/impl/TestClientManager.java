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

package org.apache.hadoop.fs.s3a.impl;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.test.StubS3ClientFactory;
import org.apache.hadoop.fs.statistics.impl.StubDurationTrackerFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static java.lang.Thread.sleep;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.functional.FutureIO.toSupplier;
import static org.mockito.Mockito.mock;

/**
 * Test the client manager.
 */
public class TestClientManager extends AbstractHadoopTestBase {

  public static final Logger LOG = LoggerFactory.getLogger(TestClientManager.class);

  private S3AsyncClient asyncClient;

  private S3TransferManager transferManager;

  private S3Client s3Client;

  private URI uri;

  @Before
  public void setUp() throws Exception {
    asyncClient = mock(S3AsyncClient.class);
    transferManager = mock(S3TransferManager.class);
    s3Client = mock(S3Client.class);
    uri = new URI("https://bucket/");
  }

  /**
   * Create a single s3 client.
   */
  @Test
  public void testCreateS3Client() throws Throwable {

    final StubS3ClientFactory factory = factory(Duration.ZERO);
    final ClientManager manager = manager(factory);

    Assertions.assertThat(manager.getOrCreateS3Client())
        .describedAs("manager %s", manager)
        .isSameAs(s3Client);
    Assertions.assertThat(factory.clientCreationCount())
        .describedAs("client creation count")
        .isEqualTo(1);

    // second attempt returns same instance
    Assertions.assertThat(manager.getOrCreateS3Client())
        .describedAs("manager %s", manager)
        .isSameAs(s3Client);

    // and the factory counter is not incremented.
    Assertions.assertThat(factory.clientCreationCount())
        .describedAs("client creation count")
        .isEqualTo(1);

    // now close
    manager.close();

    // and expect a failure
    intercept(IllegalStateException.class, () ->
        manager.getOrCreateS3Client());
  }

  /**
   * Create a stub client factory.
   * @param delay delay when creating a client.
   * @return the factory
   */
  private StubS3ClientFactory factory(final Duration delay) {
    return new StubS3ClientFactory(s3Client, asyncClient, transferManager,
        delay);
  }

  /**
   * Create a manager instance using the given factory.
   * @param factory factory for clients.
   * @return a client manager
   */
  private ClientManager manager(final StubS3ClientFactory factory) {
    return new ClientManagerImpl(
        factory,
        new S3ClientFactory.S3ClientCreationParameters()
            .withPathUri(uri),
        StubDurationTrackerFactory.STUB_DURATION_TRACKER_FACTORY);
  }

  /**
   * Create an async s3 client.
   */
  @Test
  public void testCreateAsyncS3Client() throws Throwable {

    final StubS3ClientFactory factory = factory(Duration.ofMillis(100));
    final ClientManager manager = manager(factory);

    Assertions.assertThat(manager.getOrCreateAsyncClient())
        .describedAs("manager %s", manager)
        .isSameAs(asyncClient);

    manager.getOrCreateAsyncClient();
    // and the factory counter is not incremented.
    Assertions.assertThat(factory.asyncClientCreationCount())
        .describedAs("client creation count")
        .isEqualTo(1);

    // now close
    manager.close();

    // and expect a failure
    intercept(IllegalStateException.class, () ->
        manager.getOrCreateAsyncClient());
  }

  /**
   * Create a transfer manager; this will demand create an async s3 client
   * if needed.
   */
  @Test
  public void testCreateTransferManagerAndAsyncClient() throws Throwable {

    final StubS3ClientFactory factory = factory(Duration.ZERO);
    final ClientManager manager = manager(factory);

    Assertions.assertThat(manager.getOrCreateTransferManager())
        .describedAs("manager %s", manager)
        .isSameAs(transferManager);

    // and we created an async client
    Assertions.assertThat(factory.asyncClientCreationCount())
        .describedAs("client creation count")
        .isEqualTo(1);
    Assertions.assertThat(factory.transferManagerCreationCount())
        .describedAs("client creation count")
        .isEqualTo(1);

    // now close
    manager.close();

    // and expect a failure
    intercept(IllegalStateException.class, () ->
        manager.getOrCreateTransferManager());
  }

  /**
   * Create a transfer manager with the async client already created.
   */
  @Test
  public void testCreateTransferManagerWithAsyncClientAlreadyCreated() throws Throwable {
    final StubS3ClientFactory factory = factory(Duration.ZERO);
    final ClientManager manager = manager(factory);

    manager.getOrCreateAsyncClient();
    Assertions.assertThat(manager.getOrCreateTransferManager())
        .describedAs("manager %s", manager)
        .isSameAs(transferManager);

    // no new async client was created.
    Assertions.assertThat(factory.asyncClientCreationCount())
        .describedAs("client creation count")
        .isEqualTo(1);
  }

  /**
   * Create clients in parallel and verify that the first one blocks
   * the others.
   * There's a bit of ordering complexity which uses a semaphore and a sleep
   * to block one of the acquisitions until the initial operation has started.
   * There's then an assertion that the time the first client created
   */
  @Test
  public void testParallelClientCreation() throws Throwable {
    final StubS3ClientFactory factory = factory(Duration.ofSeconds(5));
    final ClientManager manager = manager(factory);
    // time of first client creation in millis
    final AtomicLong clientCreated = new AtomicLong(0);
    Semaphore sem = new Semaphore(1);
    sem.acquire();

    // execute the first creation in a separate thread.
    final CompletableFuture<S3Client> futureClient =
        supplyAsync(toSupplier(() -> {
          LOG.info("creating #1 s3 client");
          sem.release();
          final S3Client client = manager.getOrCreateS3Client();
          clientCreated.set(System.currentTimeMillis());
          LOG.info("#1 s3 client created");
          return client;
        }));

    // wait until the async closure has started
    sem.acquire();

    sleep(1000);
    // expect to block.
    LOG.info("creating #2 s3 client");
    final S3Client client2 = manager.getOrCreateS3Client();
    LOG.info("created #2 s3 client");

    // now assert that the #1 client has succeeded, without
    // even calling futureClient.get() to evaluate the result.
    Assertions.assertThat(clientCreated.get())
        .describedAs("time client #1 was created")
        .isGreaterThan(0);

    final S3Client orig = futureClient.get();
    Assertions.assertThat(orig)
        .describedAs("async created client from %s", manager)
        .isSameAs(client2);
  }

  /**
   * Parallel transfer manager creation.
   * This will force creation of the async client
   */
  @Test
  public void testParallelTransferManagerCreation() throws Throwable {
    final StubS3ClientFactory factory = factory(Duration.ofSeconds(5));
    final ClientManager manager = manager(factory);
    // time of first client creation in millis
    final AtomicLong clientCreated = new AtomicLong(0);
    Semaphore sem = new Semaphore(1);
    sem.acquire();

    // execute the first creation in a separate thread.
    final CompletableFuture<S3TransferManager> futureClient =
        supplyAsync(toSupplier(() -> {
          LOG.info("creating #1 instance");
          sem.release();
          final S3TransferManager r = manager.getOrCreateTransferManager();
          clientCreated.set(System.currentTimeMillis());
          LOG.info("#1 instance created");
          return r;
        }));

    // wait until the async closure has started
    sem.acquire();

    sleep(1000);
    // expect to block.
    LOG.info("creating #2 instance");
    final S3TransferManager instance2 = manager.getOrCreateTransferManager();
    LOG.info("created #2 instance");

    // now assert that the #1 mananger has succeeded, without
    // even calling futureClient.get() to evaluate the result.
    Assertions.assertThat(clientCreated.get())
        .describedAs("time client #1 was created")
        .isGreaterThan(0);

    final S3TransferManager orig = futureClient.get();
    Assertions.assertThat(orig)
        .describedAs("async created instance from %s", manager)
        .isSameAs(instance2);
  }
}
