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
package org.apache.hadoop.ozone.web.handlers;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * BucketArgs packages all bucket related arguments to
 * file system calls.
 */
public class BucketArgs extends VolumeArgs {
  private final String bucketName;
  private OzoneConsts.Versioning versioning;
  private StorageType storageType;

  /**
   * Constructor for BucketArgs.
   *
   * @param volumeName - volumeName
   * @param bucketName - bucket Name
   * @param userArgs - userArgs
   */
  public BucketArgs(String volumeName, String bucketName, UserArgs userArgs) {
    super(volumeName, userArgs);
    this.bucketName = bucketName;
    this.versioning = OzoneConsts.Versioning.NOT_DEFINED;
    this.storageType = null;
  }


  /**
   * Constructor for BucketArgs.
   *
   * @param bucketName - bucket Name
   * @param volumeArgs - volume Args
   */
  public BucketArgs(String bucketName, VolumeArgs volumeArgs) {
    super(volumeArgs);
    this.bucketName = bucketName;
    this.versioning = OzoneConsts.Versioning.NOT_DEFINED;
    this.storageType = null;
  }

  /**
   * Constructor for BucketArgs.
   *
   * @param args - Bucket Args
   */
  public BucketArgs(BucketArgs args) {
    this(args.getBucketName(), args);
  }

  /**
   * Returns the Bucket Name.
   *
   * @return Bucket Name
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Returns Versioning Info.
   *
   * @return versioning
   */
  public OzoneConsts.Versioning getVersioning() {
    return versioning;
  }


  /**
   * SetVersioning Info.
   *
   * @param versioning - Enum value
   */
  public void setVersioning(OzoneConsts.Versioning versioning) {
    this.versioning = versioning;
  }

  /**
   * returns the current Storage Class.
   *
   * @return Storage Class
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Sets the Storage Class.
   *
   * @param storageType Set Storage Class
   */
  public void setStorageType(StorageType storageType) {
    this.storageType = storageType;
  }

  /**
   * returns - Volume/bucketName.
   *
   * @return String
   */
  @Override
  public String getResourceName() {
    return getVolumeName() + "/" + getBucketName();
  }

  /**
   * Returns User/Volume name which is the parent of this
   * bucket.
   *
   * @return String
   */
  public String getParentName() {
    return getUserName() + "/" + getVolumeName();
  }
}
