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

import java.io.IOException;
import java.io.UncheckedIOException;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.apache.hadoop.service.Service;

/**
 * Interface for on-demand/async creation of AWS clients
 * and extension services.
 */
public interface ClientManager extends Service {

  /**
   * Get the transfer manager, creating it and any dependencies if needed.
   * @return a transfer manager
   * @throws IOException on any failure to create the manager
   */
  S3TransferManager getOrCreateTransferManager()
      throws IOException;

  /**
   * Get the S3Client, raising a failure to create as an IOException.
   * @return the S3 client
   * @throws IOException failure to create the client.
   */
  S3Client getOrCreateS3Client() throws IOException;

  /**
   * Get the S3Client, raising a failure to create as an UncheckedIOException.
   * @return the S3 client
   * @throws UncheckedIOException failure to create the client.
   */
  S3Client getOrCreateS3ClientUnchecked() throws UncheckedIOException;

  /**
   * Get the Async S3Client,raising a failure to create as an IOException.
   * @return the Async S3 client
   * @throws IOException failure to create the client.
   */
  S3AsyncClient getOrCreateAsyncClient() throws IOException;

  /**
   * Get or create an unencrypted S3 client.
   * This is used for unencrypted operations when CSE is enabled with V1 compatibility.
   * @return unencrypted S3 client
   * @throws IOException on any failure
   */
  S3Client getOrCreateUnencryptedS3Client() throws IOException;

  /**
   * Get the AsyncS3Client, raising a failure to create as an UncheckedIOException.
   * @return the S3 client
   * @throws UncheckedIOException failure to create the client.
   */
  S3Client getOrCreateAsyncS3ClientUnchecked() throws UncheckedIOException;

}
