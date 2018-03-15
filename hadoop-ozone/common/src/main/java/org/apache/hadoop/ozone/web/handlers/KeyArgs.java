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

/**
 * Class that packages all key Arguments.
 */
public class KeyArgs extends BucketArgs {
  private String key;
  private String hash;
  private long size;

  /**
   * Constructor for Key Args.
   *
   * @param volumeName - Volume Name
   * @param bucketName - Bucket Name
   * @param objectName - Key
   */
  public KeyArgs(String volumeName, String bucketName,
                 String objectName, UserArgs args) {
    super(volumeName, bucketName, args);
    this.key = objectName;
  }

  /**
   * Constructor for Key Args.
   *
   * @param objectName - Key
   * @param args - Bucket Args
   */
  public KeyArgs(String objectName, BucketArgs args) {
    super(args);
    this.key = objectName;
  }

  /**
   * Get Key Name.
   *
   * @return String
   */
  public String getKeyName() {
    return this.key;
  }

  /**
   * Computed File hash.
   *
   * @return String
   */
  public String getHash() {
    return hash;
  }

  /**
   * Sets the hash String.
   *
   * @param hash String
   */
  public void setHash(String hash) {
    this.hash = hash;
  }

  /**
   * Returns the file size.
   *
   * @return long - file size
   */
  public long getSize() {
    return size;
  }

  /**
   * Set Size.
   *
   * @param size Size of the file
   */
  public void setSize(long size) {
    this.size = size;
  }

  /**
   * Returns the name of the resource.
   *
   * @return String
   */
  @Override
  public String getResourceName() {
    return super.getResourceName() + "/" + getKeyName();
  }

  /**
   * Parent name of this resource.
   *
   * @return String.
   */
  @Override
  public String getParentName() {
    return super.getResourceName();
  }
}
