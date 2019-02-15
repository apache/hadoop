/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.fsck;

import java.util.Objects;

/**
 * Getter and Setter for BlockDetails.
 */

public class BlockIdDetails {

  private String bucketName;
  private String blockVol;
  private  String keyName;

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getBlockVol() {
    return blockVol;
  }

  public void setBlockVol(String blockVol) {
    this.blockVol = blockVol;
  }

  public String getKeyName() {
    return keyName;
  }

  public void setKeyName(String keyName) {
    this.keyName = keyName;
  }

  @Override
  public String toString() {
    return "BlockIdDetails{" +
        "bucketName='" + bucketName + '\'' +
        ", blockVol='" + blockVol + '\'' +
        ", keyName='" + keyName + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockIdDetails that = (BlockIdDetails) o;
    return Objects.equals(bucketName, that.bucketName) &&
        Objects.equals(blockVol, that.blockVol) &&
        Objects.equals(keyName, that.keyName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucketName, blockVol, keyName);
  }
}
