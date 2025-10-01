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
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.util.ReflectionUtils;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.Constants.S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.Statistic.CLIENT_SIDE_ENCRYPTION_ENABLED;

/**
 * An implementation of the {@link S3AFileSystemOperations} interface.
 * This handles certain filesystem operations when s3 client side encryption is disabled.
 */
public class BaseS3AFileSystemOperations implements S3AFileSystemOperations {

  /**
   * Constructs a new instance of {@code BaseS3AFileSystemOperations}.
   */
  public BaseS3AFileSystemOperations() {
  }

  /**
   * Retrieves an object from the S3.
   *
   * @param store   The S3AStore object representing the S3 bucket.
   * @param request The GetObjectRequest containing the details of the object to retrieve.
   * @param factory The RequestFactory used to create the GetObjectRequest.
   * @return A ResponseInputStream containing the GetObjectResponse.
   * @throws IOException If an error occurs while retrieving the object.
   */
  @Override
  public ResponseInputStream<GetObjectResponse> getObject(S3AStore store,
      GetObjectRequest request,
      RequestFactory factory) throws IOException {
    return store.getOrCreateS3Client().getObject(request);
  }

  /**
   * Set the client side encryption gauge to 0.
   * @param ioStatisticsStore The IOStatisticsStore of the filesystem.
   */
  @Override
  public void setCSEGauge(IOStatisticsStore ioStatisticsStore) {
    ioStatisticsStore.setGauge(CLIENT_SIDE_ENCRYPTION_ENABLED.getSymbol(), 0L);
  }

  /**
   * Retrieves the client-side encryption materials for the given bucket and encryption algorithm.
   *
   * @param conf      The Hadoop configuration object.
   * @param bucket    The name of the S3 bucket.
   * @param algorithm The client-side encryption algorithm to use.
   * @return null.
   */
  @Override
  public CSEMaterials getClientSideEncryptionMaterials(Configuration conf, String bucket,
      S3AEncryptionMethods algorithm) {
    return null;
  }

  /**
   * Retrieves the S3 client factory for the specified class and configuration.
   *
   * @param conf  The Hadoop configuration object.
   * @return The S3 client factory instance.
   */
  @Override
  public S3ClientFactory getS3ClientFactory(Configuration conf) {
    Class<? extends S3ClientFactory> s3ClientFactoryClass = conf.getClass(
        S3_CLIENT_FACTORY_IMPL, DEFAULT_S3_CLIENT_FACTORY_IMPL,
        S3ClientFactory.class);
    return ReflectionUtils.newInstance(s3ClientFactoryClass, conf);
  }

  /**
   * Retrieves the S3 client factory for the specified class and configuration.
   *
   * @param conf  The Hadoop configuration object.
   * @return null.
   */
  @Override
  public S3ClientFactory getUnencryptedS3ClientFactory(Configuration conf) {
    return null;
  }


  /**
   * Return the size of S3 object.
   *
   * @param key The key (path) of the object in the S3 bucket.
   * @param length The expected length of the object.
   * @param store The S3AStore object representing the S3 bucket.
   * @param response The HeadObjectResponse containing the metadata of the object.
   * @return The size of the object in bytes.
   */
  @Override
  public long getS3ObjectSize(String key, long length, S3AStore store,
      HeadObjectResponse response) throws IOException {
    return length;
  }

}
