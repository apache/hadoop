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
package org.apache.hadoop.cblock.meta;

/**
 * A wrapper class that represents the information about a volume. Used in
 * communication between CBlock client and CBlock server only.
 */
public class VolumeInfo {
  private final String userName;
  private final String volumeName;
  private final long volumeSize;
  private final long blockSize;
  private final long usage;

  public VolumeInfo(String userName, String volumeName, long volumeSize,
      long blockSize, long usage) {
    this.userName = userName;
    this.volumeName = volumeName;
    this.volumeSize = volumeSize;
    this.blockSize = blockSize;
    this.usage = usage;
  }

  // When listing volume, the usage will not be set.
  public VolumeInfo(String userName, String volumeName, long volumeSize,
      long blockSize) {
    this.userName = userName;
    this.volumeName = volumeName;
    this.volumeSize = volumeSize;
    this.blockSize = blockSize;
    this.usage = -1;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public long getUsage() {
    return usage;
  }

  public String getUserName() {
    return userName;
  }

  public String getVolumeName() {
    return volumeName;
  }

  @Override
  public String toString() {
    return " userName:" + userName +
        " volumeName:" + volumeName +
        " volumeSize:" + volumeSize +
        " blockSize:" + blockSize +
        " (sizeInBlocks:" + volumeSize/blockSize + ")" +
        " usageInBlocks:" + usage;
  }
}
