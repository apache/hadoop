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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartInfo;

/**
 * Class that defines information about each part of a multipart upload key.
 */
public class OmPartInfo {
  private int partNumber;
  private String partName;
  private long modificationTime;
  private long size;

  public OmPartInfo(int number, String name, long time, long size) {
    this.partNumber = number;
    this.partName = name;
    this.modificationTime = time;
    this.size = size;
  }

  public int getPartNumber() {
    return partNumber;
  }

  public String getPartName() {
    return partName;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public long getSize() {
    return size;
  }

  public PartInfo getProto() {
    return PartInfo.newBuilder().setPartNumber(partNumber).setPartName(partName)
       .setModificationTime(modificationTime)
       .setSize(size).build();
  }
}
