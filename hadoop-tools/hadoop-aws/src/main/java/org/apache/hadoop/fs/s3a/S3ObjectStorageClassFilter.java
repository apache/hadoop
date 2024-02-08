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

import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import java.util.Set;
import java.util.function.Function;
import software.amazon.awssdk.services.s3.model.ObjectStorageClass;
import software.amazon.awssdk.services.s3.model.S3Object;


/**
 * <pre>
 * {@link S3ObjectStorageClassFilter} will filter the S3 files based on the
 * {@code fs.s3a.glacier.read.restored.objects} configuration set in {@link S3AFileSystem}
 * The config can have 3 values:
 * {@code READ_ALL}: Retrieval of Glacier files will fail with InvalidObjectStateException:
 * The operation is not valid for the object's storage class.
 * {@code SKIP_ALL_GLACIER}: If this value is set then this will ignore any S3 Objects which are
 * tagged with Glacier storage classes and retrieve the others.
 * {@code READ_RESTORED_GLACIER_OBJECTS}: If this value is set then restored status of the Glacier
 * object will be checked, if restored the objects would be read like normal S3 objects
 * else they will be ignored as the objects would not have been retrieved from the S3 Glacier.
 * </pre>
 */
public enum S3ObjectStorageClassFilter {
  READ_ALL(o -> true),
  SKIP_ALL_GLACIER(S3ObjectStorageClassFilter::isNotGlacierObject),
  READ_RESTORED_GLACIER_OBJECTS(S3ObjectStorageClassFilter::isCompletedRestoredObject);

  private static final Set<ObjectStorageClass> GLACIER_STORAGE_CLASSES = Sets.newHashSet(
      ObjectStorageClass.GLACIER, ObjectStorageClass.DEEP_ARCHIVE);

  private final Function<S3Object, Boolean> filter;

  S3ObjectStorageClassFilter(Function<S3Object, Boolean> filter) {
    this.filter = filter;
  }

  /**
   * Checks if the s3 object is not an object with a storage class of glacier/deep_archive.
   * @param object s3 object
   * @return if the s3 object is not an object with a storage class of glacier/deep_archive
   */
  private static boolean isNotGlacierObject(S3Object object) {
    return !GLACIER_STORAGE_CLASSES.contains(object.storageClass());
  }

  /**
   * Checks if the s3 object is an object with a storage class of glacier/deep_archive.
   * @param object s3 object
   * @return if the s3 object is an object with a storage class of glacier/deep_archive
   */
  private static boolean isGlacierObject(S3Object object) {
    return GLACIER_STORAGE_CLASSES.contains(object.storageClass());
  }

  /**
   * Checks if the s3 object is completely restored.
   * @param object s3 object
   * @return if the s3 object is completely restored
   */
  private static boolean isCompletedRestoredObject(S3Object object) {
    if(isGlacierObject(object)) {
      return object.restoreStatus() != null && !object.restoreStatus().isRestoreInProgress();
    }
    return true;
  }

  public Function<S3Object, Boolean> getFilter() {
    return filter;
  }

}
