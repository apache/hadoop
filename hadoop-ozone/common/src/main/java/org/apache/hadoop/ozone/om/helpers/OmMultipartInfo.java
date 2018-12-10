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

package org.apache.hadoop.ozone.om.helpers;

/**
 * Class which holds information about the response of initiate multipart
 * upload request.
 */
public class OmMultipartInfo {

  private String volumeName;
  private String bucketName;
  private String keyName;
  private String uploadID;

  /**
   * Construct OmMultipartInfo object which holds information about the
   * response from initiate multipart upload request.
   * @param volume
   * @param bucket
   * @param key
   * @param id
   */
  public OmMultipartInfo(String volume, String bucket, String key, String id) {
    this.volumeName = volume;
    this.bucketName = bucket;
    this.keyName = key;
    this.uploadID = id;
  }

  /**
   * Return volume name.
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Return bucket name.
   * @return bucketName
   */
  public String getBucketName() {
    return bucketName;
  }

  /**
   * Return key name.
   * @return keyName
   */
  public String getKeyName() {
    return keyName;
  }

  /**
   * Return uploadID.
   * @return uploadID
   */
  public String getUploadID() {
    return uploadID;
  }
}
