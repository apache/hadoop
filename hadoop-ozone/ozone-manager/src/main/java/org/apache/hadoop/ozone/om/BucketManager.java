/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;

import java.io.IOException;
import java.util.List;

/**
 * BucketManager handles all the bucket level operations.
 */
public interface BucketManager {
  /**
   * Creates a bucket.
   * @param bucketInfo - OmBucketInfo for creating bucket.
   */
  OmBucketInfo createBucket(OmBucketInfo bucketInfo) throws IOException;

  /**
   * Apply Create Bucket changes to OM DB.
   * @param omBucketInfo
   * @throws IOException
   */
  void applyCreateBucket(OmBucketInfo omBucketInfo) throws IOException;


  /**
   * Returns Bucket Information.
   * @param volumeName - Name of the Volume.
   * @param bucketName - Name of the Bucket.
   */
  OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException;

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  OmBucketInfo setBucketProperty(OmBucketArgs args) throws IOException;

  /**
   * Apply SetBucket Property changes to OM DB.
   * @param omBucketInfo
   * @throws IOException
   */
  void applySetBucketProperty(OmBucketInfo omBucketInfo) throws IOException;

  /**
   * Deletes an existing empty bucket from volume.
   * @param volumeName - Name of the volume.
   * @param bucketName - Name of the bucket.
   * @throws IOException
   */
  void deleteBucket(String volumeName, String bucketName) throws IOException;

  /**
   * Apply Delete Bucket changes to OM DB.
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  void applyDeleteBucket(String volumeName, String bucketName)
      throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo}
   * in the given volume.
   *
   * @param volumeName
   *   Required parameter volume name determines buckets in which volume
   *   to return.
   * @param startBucket
   *   Optional start bucket name parameter indicating where to start
   *   the bucket listing from, this key is excluded from the result.
   * @param bucketPrefix
   *   Optional start key parameter, restricting the response to buckets
   *   that begin with the specified name.
   * @param maxNumOfBuckets
   *   The maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName,
      String startBucket, String bucketPrefix, int maxNumOfBuckets)
      throws IOException;
}
