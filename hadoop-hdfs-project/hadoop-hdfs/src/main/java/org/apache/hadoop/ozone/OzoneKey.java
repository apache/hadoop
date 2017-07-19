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

package org.apache.hadoop.ozone;

import org.apache.hadoop.ksm.helpers.KsmKeyInfo;

/**
 * A class that encapsulates OzoneKey.
 */
public class OzoneKey {

  /**
   * Name of the Volume the Key belongs to.
   */
  private final String volumeName;
  /**
   * Name of the Bucket the Key belongs to.
   */
  private final String bucketName;
  /**
   * Name of the Key.
   */
  private final String keyName;
  /**
   * Name of the Container the Key resides in.
   */
  private final String containerName;
  /**
   * Name of the block id SCM assigned for the key.
   */
  private final String blockID;
  /**
   * Size of the data.
   */
  private final long dataSize;

  /**
   * Constructs OzoneKey from KsmKeyInfo.
   *
   * @param ksmKeyInfo
   */
  public OzoneKey(KsmKeyInfo ksmKeyInfo) {
    this.volumeName = ksmKeyInfo.getVolumeName();
    this.bucketName = ksmKeyInfo.getBucketName();
    this.keyName = ksmKeyInfo.getKeyName();
    this.containerName = ksmKeyInfo.getContainerName();
    this.blockID = ksmKeyInfo.getBlockID();
    this.dataSize = ksmKeyInfo.getDataSize();
  }

  /**
   * Returns Volume Name associated with the Key.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Bucket Name associated with the Key.
   *
   * @return bucketName
   */
  public String getBucketName(){
    return bucketName;
  }

  /**
   * Returns the Key Name.
   *
   * @return keyName
   */
  public String getKeyName() {
    return keyName;
  }

  /**
   * Returns Container Name associated with the Key.
   *
   * @return containerName
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Returns BlockID associated with the Key.
   *
   * @return blockID
   */
  public String getBlockID() {
    return blockID;
  }

  /**
   * Returns the size of the data.
   *
   * @return dataSize
   */
  public long getDataSize() {
    return dataSize;
  }
}
