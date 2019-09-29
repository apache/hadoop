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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RepeatedKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyInfo;

/**
 * Args for deleted keys. This is written to om metadata deletedTable.
 * Once a key is deleted, it is moved to om metadata deletedTable. Having a
 * {label: List<OMKeyInfo>} ensures that if users create & delete keys with
 * exact same uri multiple times, all the delete instances are bundled under
 * the same key name. This is useful as part of GDPR compliance where an
 * admin wants to confirm if a given key is deleted from deletedTable metadata.
 */
public class RepeatedOmKeyInfo {
  private List<OmKeyInfo> omKeyInfoList;

  public RepeatedOmKeyInfo(List<OmKeyInfo> omKeyInfos) {
    this.omKeyInfoList = omKeyInfos;
  }

  public RepeatedOmKeyInfo(OmKeyInfo omKeyInfos) {
    this.omKeyInfoList = new ArrayList<>();
    this.omKeyInfoList.add(omKeyInfos);
  }

  public void addOmKeyInfo(OmKeyInfo info) {
    this.omKeyInfoList.add(info);
  }

  public List<OmKeyInfo> getOmKeyInfoList() {
    return omKeyInfoList;
  }

  public static RepeatedOmKeyInfo getFromProto(RepeatedKeyInfo
      repeatedKeyInfo) {
    List<OmKeyInfo> list = new ArrayList<>();
    for(KeyInfo k : repeatedKeyInfo.getKeyInfoList()) {
      list.add(OmKeyInfo.getFromProtobuf(k));
    }
    return new RepeatedOmKeyInfo.Builder().setOmKeyInfos(list).build();
  }

  public RepeatedKeyInfo getProto() {
    List<KeyInfo> list = new ArrayList<>();
    for(OmKeyInfo k : omKeyInfoList) {
      list.add(k.getProtobuf());
    }

    RepeatedKeyInfo.Builder builder = RepeatedKeyInfo.newBuilder()
        .addAllKeyInfo(list);
    return builder.build();
  }

  /**
   * Builder of RepeatedOmKeyInfo.
   */
  public static class Builder {
    private List<OmKeyInfo> omKeyInfos;

    public Builder(){}

    public Builder setOmKeyInfos(List<OmKeyInfo> infoList) {
      this.omKeyInfos = infoList;
      return this;
    }

    public RepeatedOmKeyInfo build() {
      return new RepeatedOmKeyInfo(omKeyInfos);
    }
  }
}
