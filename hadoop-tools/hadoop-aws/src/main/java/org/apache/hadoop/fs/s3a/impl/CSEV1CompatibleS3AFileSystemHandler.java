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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Listing;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.util.ReflectionUtils;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.Constants.S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.impl.CSEUtils.isObjectEncrypted;

/**
 * An extension of the {@link CSES3AFileSystemHandler} class.
 * This handles certain file system operations when client-side encryption is enabled with v1 client
 * compatibility.
 * {@link org.apache.hadoop.fs.s3a.Constants#S3_ENCRYPTION_CSE_V1_COMPATIBILITY_ENABLED}.
 */
public class CSEV1CompatibleS3AFileSystemHandler extends CSES3AFileSystemHandler {

  /**
   * Constructs a new instance of {@code CSEV1CompatibleS3AFileSystemHandler}.
   */
  public CSEV1CompatibleS3AFileSystemHandler() {
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation returns a {@link Listing.AcceptAllButS3nDirsAndCSEInstructionFile}
   * or {@link Listing.AcceptAllButSelfAndS3nDirsAndCSEInstructionFile} object
   * based on the value of the {@code includeSelf} parameter.
   */
  @Override
  public Listing.FileStatusAcceptor getFileStatusAcceptor(Path path, boolean includeSelf) {
    return includeSelf
        ? new Listing.AcceptAllButS3nDirsAndCSEInstructionFile()
        : new Listing.AcceptAllButSelfAndS3nDirsAndCSEInstructionFile(path);
  }

  /**
   * Returns a {@link Listing.FileStatusAcceptor} object.
   * That determines which files and directories should be included in a listing operation.
   *
   * @param path the path for which the listing is being performed
   * @return a {@link Listing.FileStatusAcceptor} object
   */
  @Override
  public Listing.FileStatusAcceptor getFileStatusAcceptor(Path path) {
    return new Listing.AcceptFilesOnlyExceptCSEInstructionFile(path);
  }

  /**
   * Retrieves an object from the S3.
   * If the S3 object is encrypted, it uses the encrypted S3 client to retrieve the object else
   * it uses the unencrypted S3 client.
   *
   * @param store   The S3AStore object representing the S3 bucket.
   * @param request The GetObjectRequest containing the details of the object to retrieve.
   * @param factory The RequestFactory used to create the GetObjectRequest.
   * @return A ResponseInputStream containing the GetObjectResponse.
   * @throws IOException If an error occurs while retrieving the object.
   */
  @Override
  public ResponseInputStream<GetObjectResponse> getObject(S3AStore store, GetObjectRequest request,
      RequestFactory factory) throws IOException {
    boolean isEncrypted = isObjectEncrypted(store.getOrCreateS3Client(), factory, request.key());
    return isEncrypted ? store.getOrCreateS3Client().getObject(request)
        : store.getOrCreateUnencryptedS3Client().getObject(request);
  }

  /**
   * Retrieves the S3 client factory for the specified class and configuration.
   *
   * @param conf  The Hadoop configuration object.
   * @return The S3 client factory instance.
   */
  @Override
  public S3ClientFactory getUnencryptedS3ClientFactory(Configuration conf) {
    Class<? extends S3ClientFactory> s3ClientFactoryClass = conf.getClass(
        S3_CLIENT_FACTORY_IMPL, DEFAULT_S3_CLIENT_FACTORY_IMPL,
        S3ClientFactory.class);
    return ReflectionUtils.newInstance(s3ClientFactoryClass, conf);
  }


  /**
   * Retrieves the unpadded size of an object in the S3 bucket.
   *
   * @param key The key (path) of the object in the S3 bucket.
   * @param length The length of the object.
   * @param store The S3AStore object representing the S3 bucket.
   * @param bucket The name of the S3 bucket.
   * @param factory The RequestFactory used to create the HeadObjectRequest.
   * @param response The HeadObjectResponse containing the metadata of the object.
   * @return The unpadded size of the object in bytes.
   * @throws IOException If an error occurs while retrieving the object size.
   */
  @Override
  public long getS3ObjectSize(String key, long length, S3AStore store, String bucket,
      RequestFactory factory, HeadObjectResponse response) throws IOException {
    return CSEUtils.getUnPaddedObjectLength(store.getOrCreateS3Client(), bucket,
        key, factory, length, response);
  }
}
