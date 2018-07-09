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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A list of key locations. This class represents one single version of the
 * blocks of a key.
 */
public class OmKeyLocationInfoGroup {
  private final long version;
  private final List<OmKeyLocationInfo> locationList;

  public OmKeyLocationInfoGroup(long version,
                                List<OmKeyLocationInfo> locations) {
    this.version = version;
    this.locationList = locations;
  }

  /**
   * Return only the blocks that are created in the most recent version.
   *
   * @return the list of blocks that are created in the latest version.
   */
  public List<OmKeyLocationInfo> getBlocksLatestVersionOnly() {
    List<OmKeyLocationInfo> list = new ArrayList<>();
    locationList.stream().filter(x -> x.getCreateVersion() == version)
        .forEach(list::add);
    return list;
  }

  public long getVersion() {
    return version;
  }

  public List<OmKeyLocationInfo> getLocationList() {
    return locationList;
  }

  public KeyLocationList getProtobuf() {
    return KeyLocationList.newBuilder()
        .setVersion(version)
        .addAllKeyLocations(
            locationList.stream().map(OmKeyLocationInfo::getProtobuf)
                .collect(Collectors.toList()))
        .build();
  }

  public static OmKeyLocationInfoGroup getFromProtobuf(
      KeyLocationList keyLocationList) {
    return new OmKeyLocationInfoGroup(
        keyLocationList.getVersion(),
        keyLocationList.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList()));
  }

  /**
   * Given a new block location, generate a new version list based upon this
   * one.
   *
   * @param newLocationList a list of new location to be added.
   * @return
   */
  OmKeyLocationInfoGroup generateNextVersion(
      List<OmKeyLocationInfo> newLocationList) throws IOException {
    // TODO : revisit if we can do this method more efficiently
    // one potential inefficiency here is that later version always include
    // older ones. e.g. v1 has B1, then v2, v3...will all have B1 and only add
    // more
    List<OmKeyLocationInfo> newList = new ArrayList<>();
    newList.addAll(locationList);
    for (OmKeyLocationInfo newInfo : newLocationList) {
      // all these new blocks will have addVersion of current version + 1
      newInfo.setCreateVersion(version + 1);
      newList.add(newInfo);
    }
    return new OmKeyLocationInfoGroup(version + 1, newList);
  }

  void appendNewBlocks(List<OmKeyLocationInfo> newLocationList)
      throws IOException {
    for (OmKeyLocationInfo info : newLocationList) {
      info.setCreateVersion(version);
      locationList.add(info);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version:").append(version).append(" ");
    for (OmKeyLocationInfo kli : locationList) {
      sb.append(kli.getLocalID()).append(" || ");
    }
    return sb.toString();
  }
}
