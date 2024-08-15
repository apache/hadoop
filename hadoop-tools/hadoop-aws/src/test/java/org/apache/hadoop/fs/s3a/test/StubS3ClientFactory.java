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

package org.apache.hadoop.fs.s3a.test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;

/**
 * Stub implementation of {@link S3ClientFactory}.
 * returns the preconfigured clients.
 * No checks for null values are made.
 * <p>
 * The {@link #launcher} operation is invoked before creating
 * the sync and async client libraries, which is where failures,
 * delays etc can be added.
 * It is not used in {@link #createS3TransferManager(S3AsyncClient)}
 * because that is normally a fast phase.
 */
public final class StubS3ClientFactory implements S3ClientFactory {

  /**
   * The class name of this factory.
   */
  public static final String STUB_FACTORY = StubS3ClientFactory.class.getName();

  private final S3Client client;

  private final S3AsyncClient asyncClient;

  private final S3TransferManager transferManager;

  private final InvocationRaisingIOE launcher;

  private AtomicInteger clientCreationCount = new AtomicInteger(0);

  private AtomicInteger asyncClientCreationCount = new AtomicInteger(0);

  private AtomicInteger transferManagerCreationCount = new AtomicInteger(0);

  public StubS3ClientFactory(
      final S3Client client,
      final S3AsyncClient asyncClient,
      final S3TransferManager transferManager,
      final InvocationRaisingIOE launcher) {

    this.client = client;
    this.asyncClient = asyncClient;
    this.transferManager = transferManager;
    this.launcher = launcher;
  }

  @Override
  public S3Client createS3Client(final URI uri, final S3ClientCreationParameters parameters)
      throws IOException {
    clientCreationCount.incrementAndGet();
    launcher.apply();
    return client;
  }

  @Override
  public S3AsyncClient createS3AsyncClient(final URI uri,
      final S3ClientCreationParameters parameters)
      throws IOException {
    asyncClientCreationCount.incrementAndGet();
    launcher.apply();
    return asyncClient;
  }

  @Override
  public S3TransferManager createS3TransferManager(final S3AsyncClient s3AsyncClient) {
    transferManagerCreationCount.incrementAndGet();
    return transferManager;
  }

  public int clientCreationCount() {
    return clientCreationCount.get();
  }

  public int asyncClientCreationCount() {
    return asyncClientCreationCount.get();
  }

  public int transferManagerCreationCount() {
    return transferManagerCreationCount.get();
  }

  @Override
  public String toString() {
    return "StubS3ClientFactory{" +
        "client=" + client +
        ", asyncClient=" + asyncClient +
        ", transferManager=" + transferManager +
        ", clientCreationCount=" + clientCreationCount.get() +
        ", asyncClientCreationCount=" + asyncClientCreationCount.get() +
        ", transferManagerCreationCount=" + transferManagerCreationCount.get() +
        '}';
  }
}
