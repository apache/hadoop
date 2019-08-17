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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Class which is response for the list parts of a multipart upload key.
 */
public class OmMultipartUploadListParts {
  private HddsProtos.ReplicationType replicationType;
  //When a list is truncated, this element specifies the last part in the list,
  // as well as the value to use for the part-number-marker request parameter
  // in a subsequent request.
  private int nextPartNumberMarker;
 // Indicates whether the returned list of parts is truncated. A true value
 // indicates that the list was truncated.
 // A list can be truncated if the number of parts exceeds the limit
 // returned in the MaxParts element.
  private boolean truncated;
  private final List<OmPartInfo> partInfoList = new ArrayList<>();

  public OmMultipartUploadListParts(HddsProtos.ReplicationType type,
      int nextMarker, boolean truncate) {
    this.replicationType = type;
    this.nextPartNumberMarker = nextMarker;
    this.truncated = truncate;
  }

  public void addPart(OmPartInfo partInfo) {
    partInfoList.add(partInfo);
  }

  public HddsProtos.ReplicationType getReplicationType() {
    return replicationType;
  }

  public int getNextPartNumberMarker() {
    return nextPartNumberMarker;
  }

  public boolean isTruncated() {
    return truncated;
  }

  public void setReplicationType(HddsProtos.ReplicationType replicationType) {
    this.replicationType = replicationType;
  }

  public List<OmPartInfo> getPartInfoList() {
    return partInfoList;
  }

  public void addPartList(List<OmPartInfo> partInfos) {
    this.partInfoList.addAll(partInfos);
  }

  public void addProtoPartList(List<PartInfo> partInfos) {
    partInfos.forEach(partInfo -> partInfoList.add(new OmPartInfo(
        partInfo.getPartNumber(), partInfo.getPartName(),
        partInfo.getModificationTime(), partInfo.getSize())));
  }
}
