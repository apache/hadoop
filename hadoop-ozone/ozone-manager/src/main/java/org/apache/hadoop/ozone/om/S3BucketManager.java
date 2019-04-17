/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;

/**
 * An interface that maps S3 buckets to Ozone
 * volume/bucket.
 */
public interface S3BucketManager {
  /**
   * Creates an s3 bucket and maps it to Ozone volume/bucket.
   * @param  userName - Name of the user who owns the bucket.
   * @param bucketName - S3 Bucket Name.
   * @throws  IOException in case the bucket cannot be created.
   */
  void createS3Bucket(String userName, String bucketName) throws IOException;

  /**
   * Deletes an s3 bucket and removes mapping of Ozone volume/bucket.
   * @param bucketName - S3 Bucket Name.
   * @throws  IOException in case the bucket cannot be deleted.
   */
  void deleteS3Bucket(String bucketName) throws IOException;

  /**
   * Returns the Ozone volume/bucket where the S3 Bucket points to.
   * @param s3BucketName - S3 Bucket Name
   * @return String - Ozone volume/bucket
   * @throws IOException in case of failure to retrieve mapping.
   */
  String getOzoneBucketMapping(String s3BucketName) throws IOException;

  /**
   * Returns Ozone volume name for a given S3Bucket.
   * @param s3BucketName - S3 bucket name.
   * @return String - Ozone volume name where is s3bucket resides.
   * @throws IOException - in case of failure to retrieve mapping.
   */
  String getOzoneVolumeName(String s3BucketName) throws IOException;

  /**
   * Returns Ozone bucket name for a given s3Bucket.
   * @param s3BucketName  - S3 bucket Name.
   * @return  Ozone bucket name for this given S3 bucket
   * @throws IOException - in case of failure to retrieve mapping.
   */
  String getOzoneBucketName(String s3BucketName) throws IOException;

  /**
   * Returns volume Name for a user.
   * @param userName
   */
  String getOzoneVolumeNameForUser(String userName) throws IOException;

  /**
   * Create ozone volume if required, this will be needed during creates3Bucket.
   * @param userName
   * @return true - if volume is successfully created. false - if volume
   * already exists or volume creation failure.
   * @throws IOException - incase of volume creation failure.
   */
  boolean createOzoneVolumeIfNeeded(String userName) throws IOException;
}
