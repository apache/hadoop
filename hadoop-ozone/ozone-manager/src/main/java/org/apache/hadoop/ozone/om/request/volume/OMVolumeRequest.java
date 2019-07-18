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

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines common methods required for volume requests.
 */
public abstract class OMVolumeRequest extends OMClientRequest {

  public OMVolumeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * Delete volume from user volume list. This method should be called after
   * acquiring user lock.
   * @param volumeList - current volume list owned by user.
   * @param volume - volume which needs to deleted from the volume list.
   * @param owner
   * @return VolumeList - updated volume list for the user.
   * @throws IOException
   */
  protected VolumeList delVolumeFromOwnerList(VolumeList volumeList,
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
  protected VolumeList addVolumeToOwnerList(VolumeList volumeList,
      String volume, String owner, long maxUserVolumeCount) throws IOException {

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

  /**
   * Create Ozone Volume. This method should be called after acquiring user
   * and volume Lock.
   * @param omMetadataManager
   * @param omVolumeArgs
   * @param volumeList
   * @param dbVolumeKey
   * @param dbUserKey
   * @param transactionLogIndex
   * @throws IOException
   */
  protected void createVolume(final OMMetadataManager omMetadataManager,
      OmVolumeArgs omVolumeArgs, VolumeList volumeList, String dbVolumeKey,
      String dbUserKey, long transactionLogIndex) {
    // Update cache: Update user and volume cache.
    omMetadataManager.getUserTable().addCacheEntry(new CacheKey<>(dbUserKey),
        new CacheValue<>(Optional.of(volumeList), transactionLogIndex));

    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(dbVolumeKey),
        new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));
  }

}
