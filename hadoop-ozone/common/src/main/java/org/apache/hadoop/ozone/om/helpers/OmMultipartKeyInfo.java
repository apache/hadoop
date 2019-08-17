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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .MultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class represents multipart upload information for a key, which holds
 * upload part information of the key.
 */
public class OmMultipartKeyInfo {
  private String uploadID;
  private TreeMap<Integer, PartKeyInfo> partKeyInfoList;

  /**
   * Construct OmMultipartKeyInfo object which holds multipart upload
   * information for a key.
   * @param id
   * @param list upload parts of a key.
   */
  public OmMultipartKeyInfo(String id, Map<Integer, PartKeyInfo> list) {
    this.uploadID = id;
    this.partKeyInfoList = new TreeMap<>(list);
  }

  /**
   * Returns the uploadID for this multi part upload of a key.
   * @return uploadID
   */
  public String getUploadID() {
    return uploadID;
  }

  public TreeMap<Integer, PartKeyInfo> getPartKeyInfoMap() {
    return partKeyInfoList;
  }

  public void addPartKeyInfo(int partNumber, PartKeyInfo partKeyInfo) {
    this.partKeyInfoList.put(partNumber, partKeyInfo);
  }

  public PartKeyInfo getPartKeyInfo(int partNumber) {
    return partKeyInfoList.get(partNumber);
  }


  /**
   * Construct OmMultipartInfo from MultipartKeyInfo proto object.
   * @param multipartKeyInfo
   * @return OmMultipartKeyInfo
   */
  public static OmMultipartKeyInfo getFromProto(MultipartKeyInfo
                                                 multipartKeyInfo) {
    Map<Integer, PartKeyInfo> list = new HashMap<>();
    multipartKeyInfo.getPartKeyInfoListList().stream().forEach(partKeyInfo
        -> list.put(partKeyInfo.getPartNumber(), partKeyInfo));
    return new OmMultipartKeyInfo(multipartKeyInfo.getUploadID(), list);
  }

  /**
   * Construct MultipartKeyInfo from this object.
   * @return MultipartKeyInfo
   */
  public MultipartKeyInfo getProto() {
    MultipartKeyInfo.Builder builder = MultipartKeyInfo.newBuilder()
        .setUploadID(uploadID);
    partKeyInfoList.forEach((key, value) -> builder.addPartKeyInfoList(value));
    return builder.build();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    return other instanceof OmMultipartKeyInfo && uploadID.equals(
        ((OmMultipartKeyInfo)other).getUploadID());
  }

  @Override
  public int hashCode() {
    return uploadID.hashCode();
  }

}
