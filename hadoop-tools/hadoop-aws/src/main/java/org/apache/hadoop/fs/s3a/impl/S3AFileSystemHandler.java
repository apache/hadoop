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

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Listing;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * An interface that defines the contract for handling certain filesystem operations.
 */
public interface S3AFileSystemHandler {

  /**
   * Returns a {@link Listing.FileStatusAcceptor} object.
   * That determines which files and directories should be included in a listing operation.
   *
   * @param path         the path for which the listing is being performed
   * @param includeSelf  a boolean indicating whether the path itself should
   *                     be included in the listing
   * @return a {@link Listing.FileStatusAcceptor} object
   */
  Listing.FileStatusAcceptor getFileStatusAcceptor(Path path, boolean includeSelf);

  /**
   * Returns a {@link Listing.FileStatusAcceptor} object.
   * That determines which files and directories should be included in a listing operation.
   *
   * @param path         the path for which the listing is being performed
   * @return a {@link Listing.FileStatusAcceptor} object
   */
  Listing.FileStatusAcceptor getFileStatusAcceptor(Path path);

  /**
   * Retrieves an object from the S3.
   *
   * @param store The S3AStore object representing the S3 bucket.
   * @param request The GetObjectRequest containing the details of the object to retrieve.
   * @param factory The RequestFactory used to create the GetObjectRequest.
   * @return A ResponseInputStream containing the GetObjectResponse.
   * @throws IOException If an error occurs while retrieving the object.
   */
  ResponseInputStream<GetObjectResponse> getObject(S3AStore store, GetObjectRequest request,
      RequestFactory factory) throws IOException;

  /**
   * Set the client side encryption gauge to 0 or 1, indicating if CSE is enabled.
   * @param ioStatisticsStore The IOStatisticsStore of the filesystem.
   */
  void setCSEGauge(IOStatisticsStore ioStatisticsStore);

  /**
   * Retrieves the client-side encryption materials for the given bucket and encryption algorithm.
   *
   * @param conf The Hadoop configuration object.
   * @param bucket The name of the S3 bucket.
   * @param algorithm The client-side encryption algorithm to use.
   * @return The client-side encryption materials.
   * @throws IOException If an error occurs while retrieving the encryption materials.
   */
  CSEMaterials getClientSideEncryptionMaterials(Configuration conf, String bucket,
      S3AEncryptionMethods algorithm) throws IOException;

  /**
   * Retrieves the S3 client factory for the specified class and configuration.
   *
   * @param conf The Hadoop configuration object.
   * @return The S3 client factory instance.
   */
  S3ClientFactory getS3ClientFactory(Configuration conf);

  /**
   * Retrieves the S3 client factory for the specified class and configuration.
   *
   * @param conf The Hadoop configuration object.
   * @return The S3 client factory instance.
   */
  S3ClientFactory getUnencryptedS3ClientFactory(Configuration conf);


  /**
   * Retrieves the size of a S3 object.
   *
   * @param key The key (path) of the object in the S3 bucket.
   * @param length The expected length of the object, if known. If not known, pass -1.
   * @param store The S3AStore object representing the S3 bucket.
   * @param bucket The name of the S3 bucket.
   * @param factory The RequestFactory used to create the HeadObjectRequest.
   * @param response The HeadObjectResponse containing the metadata of the object.
   * @return The size of the object in bytes.
   * @throws IOException If an error occurs while retrieving the object size.
   */
  long getS3ObjectSize(String key, long length, S3AStore store, String bucket,
      RequestFactory factory, HeadObjectResponse response) throws IOException;

}

