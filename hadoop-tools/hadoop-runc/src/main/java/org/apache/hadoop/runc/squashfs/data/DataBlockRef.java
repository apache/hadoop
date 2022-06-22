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

package org.apache.hadoop.runc.squashfs.data;

public class DataBlockRef {

  private final long location;
  private final int logicalSize;
  private final int physicalSize;
  private final boolean compressed;
  private final boolean sparse;

  public DataBlockRef(long location, int logicalSize, int physicalSize,
      boolean compressed, boolean sparse) {
    this.location = location;
    this.logicalSize = logicalSize;
    this.physicalSize = physicalSize;
    this.compressed = compressed;
    this.sparse = sparse;
  }

  public long getLocation() {
    return location;
  }

  public int getLogicalSize() {
    return logicalSize;
  }

  public int getPhysicalSize() {
    return physicalSize;
  }

  public boolean isCompressed() {
    return compressed;
  }

  public boolean isSparse() {
    return sparse;
  }

  public int getInodeSize() {
    return (physicalSize & 0xfffff) | (compressed ? 0 : 0x1000000);
  }

  @Override
  public String toString() {
    return String.format("data-block-ref { location=%d, logicalSize=%d, "
            + "physicalSize=%d, compressed=%s, sparse=%s }",
        location, logicalSize, physicalSize, compressed, sparse);
  }

}
