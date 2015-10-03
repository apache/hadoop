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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeInfoWithStorage extends DatanodeInfo {
  private final String storageID;
  private final StorageType storageType;

  public DatanodeInfoWithStorage(DatanodeInfo from, String storageID,
      StorageType storageType) {
    super(from);
    this.storageID = storageID;
    this.storageType = storageType;
    setSoftwareVersion(from.getSoftwareVersion());
    setDependentHostNames(from.getDependentHostNames());
    setLevel(from.getLevel());
    setParent(from.getParent());
  }

  public String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  @Override
  public boolean equals(Object o) {
    // allows this class to be used interchangeably with DatanodeInfo
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // allows this class to be used interchangeably with DatanodeInfo
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "DatanodeInfoWithStorage[" + super.toString() + "," + storageID +
        "," + storageType + "]";
  }
}
