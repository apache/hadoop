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
import java.net.URI;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.encryption.s3.S3AsyncEncryptionClient;
import software.amazon.encryption.s3.S3EncryptionClient;
import software.amazon.encryption.s3.materials.CryptographicMaterialsManager;
import software.amazon.encryption.s3.materials.DefaultCryptoMaterialsManager;
import software.amazon.encryption.s3.materials.Keyring;
import software.amazon.encryption.s3.materials.KmsKeyring;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.functional.LazyAtomicReference;

import static org.apache.hadoop.fs.s3a.impl.InstantiationIOException.unavailable;

/**
 * Factory class to create encrypted s3 client and encrypted async s3 client.
 */
public class EncryptionS3ClientFactory extends DefaultS3ClientFactory {

  /**
   * Encryption client class name.
   * value: {@value}
   */
  private static final String ENCRYPTION_CLIENT_CLASSNAME =
      "software.amazon.encryption.s3.S3EncryptionClient";

  /**
   * Encryption client availability.
   */
  private static final LazyAtomicReference<Boolean> ENCRYPTION_CLIENT_AVAILABLE =
      LazyAtomicReference.lazyAtomicReferenceFromSupplier(
          EncryptionS3ClientFactory::checkForEncryptionClient
      );


  /**
   * S3Client to be wrapped by encryption client.
   */
  private S3Client s3Client;

  /**
   * S3AsyncClient to be wrapped by encryption client.
   */
  private S3AsyncClient s3AsyncClient;

  /**
   * Checks if {@link #ENCRYPTION_CLIENT_CLASSNAME} is available in the class path.
   * @return true if available, false otherwise.
   */
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
    return ENCRYPTION_CLIENT_AVAILABLE.get();
  }

  /**
   * Creates both synchronous and asynchronous encrypted s3 clients.
   * Synchronous client is wrapped by encryption client first and then
   * Asynchronous client is wrapped by encryption client.
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return encrypted s3 client
   * @throws IOException IO failures
   */
  @Override
  public S3Client createS3Client(URI uri, S3ClientCreationParameters parameters)
      throws IOException {
    if (!isEncryptionClientAvailable()) {
      throw unavailable(uri, ENCRYPTION_CLIENT_CLASSNAME, null,
          "No encryption client available");
    }

    s3Client = super.createS3Client(uri, parameters);
    s3AsyncClient = super.createS3AsyncClient(uri, parameters);

    return createS3EncryptionClient(parameters);
  }

  /**
   * Create async encrypted s3 client.
   * @param uri S3A file system URI
   * @param parameters parameter object
   * @return async encrypted s3 client
   * @throws IOException IO failures
   */
  @Override
  public S3AsyncClient createS3AsyncClient(URI uri, S3ClientCreationParameters parameters)
      throws IOException {
    if (!isEncryptionClientAvailable()) {
      throw unavailable(uri, ENCRYPTION_CLIENT_CLASSNAME, null,
          "No encryption client available");
    }
    return createS3AsyncEncryptionClient(parameters);
  }

  /**
   * Creates an S3EncryptionClient instance based on the provided parameters.
   *
   * @param parameters The S3ClientCreationParameters containing the necessary configuration.
   * @return An instance of S3EncryptionClient.
   * @throws IOException If an error occurs during the creation of the S3EncryptionClient.
   */
  private S3Client createS3EncryptionClient(S3ClientCreationParameters parameters)
      throws IOException {
    CSEMaterials cseMaterials = parameters.getClientSideEncryptionMaterials();
    Preconditions.checkArgument(s3AsyncClient != null,
        "S3 async client not initialized");
    Preconditions.checkArgument(s3Client != null,
        "S3 client not initialized");
    Preconditions.checkArgument(parameters != null,
        "S3ClientCreationParameters is not initialized");

    S3EncryptionClient.Builder s3EncryptionClientBuilder =
        S3EncryptionClient.builder()
            .wrappedAsyncClient(s3AsyncClient)
            .wrappedClient(s3Client)
            // this is required for doing S3 ranged GET calls
            .enableLegacyUnauthenticatedModes(true)
            // this is required for backward compatibility with older encryption clients
            .enableLegacyWrappingAlgorithms(true);

    switch (cseMaterials.getCseKeyType()) {
    case KMS:
      Keyring kmsKeyring = createKmsKeyring(parameters, cseMaterials);
      CryptographicMaterialsManager kmsCryptoMaterialsManager =
          DefaultCryptoMaterialsManager.builder()
              .keyring(kmsKeyring)
              .build();
      s3EncryptionClientBuilder.cryptoMaterialsManager(kmsCryptoMaterialsManager);
      break;
    case CUSTOM:
      Keyring keyring;
      try {
        keyring =
            getKeyringProvider(cseMaterials.getCustomKeyringClassName(), cseMaterials.getConf());
      } catch (RuntimeException e) {
        throw new IOException("Failed to instantiate a custom keyring provider: " + e, e);
      }
      CryptographicMaterialsManager customCryptoMaterialsManager =
          DefaultCryptoMaterialsManager.builder()
              .keyring(keyring)
              .build();
      s3EncryptionClientBuilder.cryptoMaterialsManager(customCryptoMaterialsManager);
      break;
    default:
      break;
    }
    return s3EncryptionClientBuilder.build();
  }

  /**
   * Creates KmsKeyring instance based on the provided S3ClientCreationParameters and CSEMaterials.
   *
   * @param parameters The S3ClientCreationParameters containing the necessary configuration.
   * @param cseMaterials The CSEMaterials containing the KMS key ID and other encryption materials.
   * @return A KmsKeyring instance configured with the appropriate KMS client and wrapping key ID.
   */
  private Keyring createKmsKeyring(S3ClientCreationParameters parameters,
      CSEMaterials cseMaterials) {
    KmsClientBuilder kmsClientBuilder = KmsClient.builder();
    if (parameters.getCredentialSet() != null) {
      kmsClientBuilder.credentialsProvider(parameters.getCredentialSet());
    }
    // check if kms region is configured.
    if (parameters.getKmsRegion() != null) {
      kmsClientBuilder.region(Region.of(parameters.getKmsRegion()));
    } else if (parameters.getRegion() != null) {
      // fallback to s3 region if kms region is not configured.
      kmsClientBuilder.region(Region.of(parameters.getRegion()));
    } else if (parameters.getEndpoint() != null) {
      // fallback to s3 endpoint config if both kms region and s3 region is not set.
      String endpointStr = parameters.getEndpoint();
      URI endpoint = getS3Endpoint(endpointStr, cseMaterials.getConf());
      kmsClientBuilder.endpointOverride(endpoint);
    }
    return KmsKeyring.builder()
        .kmsClient(kmsClientBuilder.build())
        .wrappingKeyId(cseMaterials.getKmsKeyId())
        .build();
  }

  /**
   * Creates an S3AsyncEncryptionClient instance based on the provided parameters.
   *
   * @param parameters The S3ClientCreationParameters containing the necessary configuration.
   * @return An instance of S3AsyncEncryptionClient.
   * @throws IOException If an error occurs during the creation of the S3AsyncEncryptionClient.
   */
  private S3AsyncClient createS3AsyncEncryptionClient(S3ClientCreationParameters parameters)
      throws IOException {
    Preconditions.checkArgument(s3AsyncClient != null,
        "S3 async client not initialized");
    Preconditions.checkArgument(parameters != null,
        "S3ClientCreationParameters is not initialized");

    S3AsyncEncryptionClient.Builder s3EncryptionAsyncClientBuilder =
        S3AsyncEncryptionClient.builder()
            .wrappedClient(s3AsyncClient)
            // this is required for doing S3 ranged GET calls
            .enableLegacyUnauthenticatedModes(true)
            // this is required for backward compatibility with older encryption clients
            .enableLegacyWrappingAlgorithms(true);

    CSEMaterials cseMaterials = parameters.getClientSideEncryptionMaterials();
    switch (cseMaterials.getCseKeyType()) {
    case KMS:
      Keyring kmsKeyring = createKmsKeyring(parameters, cseMaterials);
      CryptographicMaterialsManager kmsCryptoMaterialsManager =
          DefaultCryptoMaterialsManager.builder()
              .keyring(kmsKeyring)
              .build();
      s3EncryptionAsyncClientBuilder.cryptoMaterialsManager(kmsCryptoMaterialsManager);
      break;
    case CUSTOM:
      Keyring keyring;
      try {
        keyring =
            getKeyringProvider(cseMaterials.getCustomKeyringClassName(), cseMaterials.getConf());
      } catch (RuntimeException e) {
        throw new IOException("Failed to instantiate a custom keyring provider: " + e, e);
      }
      CryptographicMaterialsManager customCryptoMaterialsManager =
          DefaultCryptoMaterialsManager.builder()
              .keyring(keyring)
              .build();
      s3EncryptionAsyncClientBuilder.cryptoMaterialsManager(customCryptoMaterialsManager);
      break;
    default:
      break;
    }
    return s3EncryptionAsyncClientBuilder.build();
  }

  /**
   * Creates and returns a Keyring provider instance based on the given class name.
   *
   * <p>This method attempts to instantiate a Keyring provider using reflection. It first tries
   * to create an instance using the standard ReflectionUtils.newInstance method. If that fails,
   * it falls back to an alternative instantiation method, which is primarily used for testing
   * purposes (specifically for CustomKeyring.java).
   *
   * @param className The fully qualified class name of the Keyring provider to instantiate.
   * @param conf The Configuration object to be passed to the Keyring provider constructor.
   * @return An instance of the specified Keyring provider.
   * @throws RuntimeException If unable to create the Keyring provider instance.
   */
  private Keyring getKeyringProvider(String className, Configuration conf)  {
    Class<? extends Keyring> keyringProviderClass = getCustomKeyringProviderClass(className);
    try {
      return ReflectionUtils.newInstance(keyringProviderClass, conf);
    } catch (Exception e) {
      LOG.warn("Failed to create Keyring provider", e);
      // This is for testing purposes to support CustomKeyring.java
      try {
        return ReflectionUtils.newInstance(keyringProviderClass, conf,
            new Class[] {Configuration.class}, conf);
      } catch (Exception ex) {
        throw new RuntimeException("Failed to create Keyring provider", ex);
      }
    }
  }

  /**
   * Retrieves the Class object for the custom Keyring provider based on the provided class name.
   *
   * @param className The fully qualified class name of the custom Keyring provider implementation.
   * @return The Class object representing the custom Keyring provider implementation.
   * @throws IllegalArgumentException If the provided class name is null or empty,
   * or if the specified class is not found.
   */
  private Class<? extends Keyring> getCustomKeyringProviderClass(String className) {
    Preconditions.checkArgument(className !=null && !className.isEmpty(),
        "Custom Keyring class name is null or empty");
    try {
      return Class.forName(className).asSubclass(Keyring.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Custom CryptographicMaterialsManager class " + className + "not found", e);
    }
  }
}
