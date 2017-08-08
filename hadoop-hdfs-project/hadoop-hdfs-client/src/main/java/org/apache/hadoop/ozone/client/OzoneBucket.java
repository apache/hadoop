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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.client;


import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts.Versioning;

import java.util.List;

/**
 * A class that encapsulates OzoneBucket.
 */
public class OzoneBucket {

  /**
   * Name of the volume in which the bucket belongs to.
   */
  private final String volumeName;
  /**
   * Name of the bucket.
   */
  private final String bucketName;
  /**
   * Bucket ACLs.
   */
  private final List<OzoneAcl> acls;

  /**
   * Type of storage to be used for this bucket.
   * [RAM_DISK, SSD, DISK, ARCHIVE]
   */
  private final StorageType storageType;

  /**
   * Bucket Version flag.
   */
  private final Versioning versioning;


  /**
   * Constructs OzoneBucket from KsmBucketInfo.
   *
   * @param ksmBucketInfo
   */
  public OzoneBucket(KsmBucketInfo ksmBucketInfo) {
    this.volumeName = ksmBucketInfo.getVolumeName();
    this.bucketName = ksmBucketInfo.getBucketName();
    this.acls = ksmBucketInfo.getAcls();
    this.storageType = ksmBucketInfo.getStorageType();
    this.versioning = ksmBucketInfo.getIsVersionEnabled() ?
        Versioning.ENABLED : Versioning.DISABLED;
  }

  /**
   * Returns Volume Name.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name.
   *
   * @return bucketName
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns ACL's associated with the Bucket.
   *
   * @return acls
   */
  public List<OzoneAcl> getAcls() {
    return acls;
  }

  /**
   * Returns StorageType of the Bucket.
   *
   * @return storageType
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Returns Versioning associated with the Bucket.
   *
   * @return versioning
   */
  public Versioning getVersioning() {
    return versioning;
  }

}
