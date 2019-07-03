/**
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

package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_SECRET;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;

/**
 * Utility class contains helper functions required for OM lock.
 */
final class OzoneManagerLockUtil {


  private OzoneManagerLockUtil() {
  }

  /**
   * Generate resource lock name for the given resource name.
   *
   * @param resource
   * @param resourceName
   */
  public static String generateResourceLockName(
      OzoneManagerLock.Resource resource, String resourceName) {

    if (resource == OzoneManagerLock.Resource.S3_BUCKET_LOCK) {
      return OM_S3_PREFIX + resourceName;
    } else if (resource == OzoneManagerLock.Resource.VOLUME_LOCK) {
      return OM_KEY_PREFIX + resourceName;
    } else if (resource == OzoneManagerLock.Resource.USER_LOCK) {
      return OM_USER_PREFIX + resourceName;
    } else if (resource == OzoneManagerLock.Resource.S3_SECRET_LOCK) {
      return OM_S3_SECRET + resourceName;
    } else if (resource == OzoneManagerLock.Resource.PREFIX_LOCK) {
      return OM_PREFIX + resourceName;
    } else {
      // This is for developers who mistakenly call this method with resource
      // bucket type, as for bucket type we need bucket and volumeName.
      throw new IllegalArgumentException("Bucket resource type is passed, " +
          "to get BucketResourceLockName, use generateBucketLockName method");
    }

  }

  /**
   * Generate bucket lock name.
   * @param volumeName
   * @param bucketName
   */
  public static String generateBucketLockName(String volumeName,
      String bucketName) {
    return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName;

  }

}
