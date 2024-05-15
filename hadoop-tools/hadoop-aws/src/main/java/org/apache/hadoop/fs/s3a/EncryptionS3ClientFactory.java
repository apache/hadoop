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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.encryption.s3.S3AsyncEncryptionClient;
import software.amazon.encryption.s3.S3EncryptionClient;

import org.apache.hadoop.fs.s3a.impl.CSEMaterials;

import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.unavailable;

public class EncryptionS3ClientFactory extends DefaultS3ClientFactory {

  private static final String ENCRYPTION_CLIENT_CLASSNAME =
      "software.amazon.encryption.s3.S3EncryptionClient";

  /**
   * Encryption client availability.
   */
  private static final boolean ENCRYPTION_CLIENT_FOUND = checkForEncryptionClient();

  /**
   * S3Client to be wrapped by encryption client.
   */
  private S3Client s3Client;

  /**
   * S3AsyncClient to be wrapped by encryption client.
   */
  private S3AsyncClient s3AsyncClient;

  private static boolean checkForEncryptionClient() {
    try {
      ClassLoader cl = EncryptionS3ClientFactory.class.getClassLoader();
      cl.loadClass(ENCRYPTION_CLIENT_CLASSNAME);
      LOG.debug("encryption client class {} found", ENCRYPTION_CLIENT_CLASSNAME);
      return true;
    } catch (Exception e) {
      LOG.debug("encryption client class {} not found", ENCRYPTION_CLIENT_CLASSNAME, e);
      return false;
    }
  }

  /**
   * Is the Encryption client available?
   * @return true if it was found in the classloader
   */
  private static synchronized boolean isEncryptionClientAvailable() {
    return ENCRYPTION_CLIENT_FOUND;
  }


  @Override
  public S3Client createS3Client(URI uri, S3ClientCreationParameters parameters) throws IOException {

    if (!isEncryptionClientAvailable()) {
      throw unavailable(uri, ENCRYPTION_CLIENT_CLASSNAME, null, "No encryption client available");
    }

    s3Client = super.createS3Client(uri, parameters);
    s3AsyncClient = super.createS3AsyncClient(uri, parameters);

    return createS3EncryptionClient(parameters.getClientSideEncryptionMaterials());
  }

  @Override
  public S3AsyncClient createS3AsyncClient(URI uri, S3ClientCreationParameters parameters) throws IOException {

    if (!isEncryptionClientAvailable()) {
      throw unavailable(uri, ENCRYPTION_CLIENT_CLASSNAME, null, "No encryption client available");
    }

    return createS3AsyncEncryptionClient(parameters.getClientSideEncryptionMaterials());
  }

  private S3Client createS3EncryptionClient(final CSEMaterials cseMaterials) {
    S3EncryptionClient.Builder s3EncryptionClientBuilder =
        S3EncryptionClient.builder().wrappedAsyncClient(s3AsyncClient).wrappedClient(s3Client)
            .enableLegacyUnauthenticatedModes(true);

    if (cseMaterials.getCseKeyType().equals(CSEMaterials.CSEKeyType.KMS)) {
      s3EncryptionClientBuilder.kmsKeyId(cseMaterials.getKmsKeyId());
    }

    return s3EncryptionClientBuilder.build();
  }


  private S3AsyncClient createS3AsyncEncryptionClient(final CSEMaterials cseMaterials) {

    S3AsyncEncryptionClient.Builder s3EncryptionAsyncClientBuilder =
        S3AsyncEncryptionClient.builder().wrappedClient(s3AsyncClient)
            .enableLegacyUnauthenticatedModes(true);

    if (cseMaterials.getCseKeyType().equals(CSEMaterials.CSEKeyType.KMS)) {
      s3EncryptionAsyncClientBuilder.kmsKeyId(cseMaterials.getKmsKeyId());
    }

    return s3EncryptionAsyncClientBuilder.build();
  }

}
