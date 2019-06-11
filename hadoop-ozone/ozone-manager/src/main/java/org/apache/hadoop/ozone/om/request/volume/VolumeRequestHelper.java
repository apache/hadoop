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

package org.apache.hadoop.ozone.om.request.volume;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for Volume requests.
 */
public final class VolumeRequestHelper {

  private VolumeRequestHelper() {

  }

  /**
   * Delete volume from user list. This method should be called after acquiring
   * user lock.
   * @param omMetadataManager
   * @param volume
   * @param owner
   * @return VolumeList - updated volume list for the user.
   * @throws IOException
   */
  public static VolumeList delVolumeFromOwnerList(
      OMMetadataManager omMetadataManager, String volume, String owner)
      throws IOException {
    Preconditions.checkNotNull(omMetadataManager);
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(owner);
    // Get the volume list
    VolumeList volumeList = omMetadataManager.getUserTable().get(owner);

    List<String> prevVolList = new ArrayList<>();
    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    } else {
      throw new OMException(OMException.ResultCodes.USER_NOT_FOUND);
    }

    // Remove the volume from the list
    prevVolList.remove(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();
    return newVolList;
  }


  /**
   * Add volume to user list. This method should be called after acquiring user
   * lock.
   * @param omMetadataManager
   * @param volume
   * @param owner
   * @param maxUserVolumeCount
   * @return VolumeList - which is updated volume list.
   * @throws OMException - if user has volumes greater than
   * maxUserVolumeCount, an exception is thrown.
   */
  public static VolumeList addVolumeToOwnerList(
      OMMetadataManager omMetadataManager, String volume, String owner,
      long maxUserVolumeCount) throws IOException {
    Preconditions.checkNotNull(omMetadataManager);
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(owner);
    String dbUserKey = omMetadataManager.getUserKey(owner);
    Preconditions.checkArgument(maxUserVolumeCount > 0, "maxUserVolumeCount " +
        "should be greater than zero");
    // Get the volume list
    VolumeList volumeList = omMetadataManager.getUserTable().get(dbUserKey);

    // Allowing maxUserVolumeCount zero for the case like when for a user we
    // want to have no volumes. Revisit this if need to be changed later.
    // Check the volume count
    if (maxUserVolumeCount == 0 || (volumeList != null &&
        volumeList.getVolumeNamesList().size() >= maxUserVolumeCount)) {
      throw new OMException("Too many volumes for user:" + dbUserKey,
          OMException.ResultCodes.USER_TOO_MANY_VOLUMES);
    }

    List<String> prevVolList = new ArrayList<>();
    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    }

    // Add the new volume to the list
    prevVolList.add(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();

    return newVolList;
  }
}
