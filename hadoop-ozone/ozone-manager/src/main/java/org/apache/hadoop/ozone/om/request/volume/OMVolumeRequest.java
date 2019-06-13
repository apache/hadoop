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

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines common methods required for volume requests.
 */
public interface OMVolumeRequest {

  /**
   * Delete volume from user volume list. This method should be called after
   * acquiring user lock.
   * @param volumeList - current volume list owned by user.
   * @param volume - volume which needs to deleted from the volume list.
   * @param owner
   * @return VolumeList - updated volume list for the user.
   * @throws IOException
   */
  default VolumeList delVolumeFromOwnerList(VolumeList volumeList,
      String volume, String owner) throws IOException {

    List<String> prevVolList = new ArrayList<>();

    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    } else {
      // No Volumes for this user
      throw new OMException("User not found: " + owner,
          OMException.ResultCodes.USER_NOT_FOUND);
    }

    // Remove the volume from the list
    prevVolList.remove(volume);
    VolumeList newVolList = VolumeList.newBuilder()
        .addAllVolumeNames(prevVolList).build();
    return newVolList;
  }


  /**
   * Add volume to user volume list. This method should be called after
   * acquiring user lock.
   * @param volumeList - current volume list owned by user.
   * @param volume - volume which needs to be added to this list.
   * @param owner
   * @param maxUserVolumeCount
   * @return VolumeList - which is updated volume list.
   * @throws OMException - if user has volumes greater than
   * maxUserVolumeCount, an exception is thrown.
   */
  default VolumeList addVolumeToOwnerList(
      VolumeList volumeList, String volume, String owner,
      long maxUserVolumeCount) throws IOException {

    // Check the volume count
    if (volumeList != null &&
        volumeList.getVolumeNamesList().size() >= maxUserVolumeCount) {
      throw new OMException("Too many volumes for user:" + owner,
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
