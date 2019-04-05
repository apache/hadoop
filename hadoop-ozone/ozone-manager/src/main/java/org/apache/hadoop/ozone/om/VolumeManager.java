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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.OmDeleteVolumeResponse;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeOwnerChangeResponse;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

import java.io.IOException;
import java.util.List;

/**
 * OM volume manager interface.
 */
public interface VolumeManager {

  /**
   * Create a new volume.
   * @param args - Volume args to create a volume
   */
  VolumeList createVolume(OmVolumeArgs args)
      throws IOException;

  /**
   * Apply Create Volume changes to OM DB.
   * @param omVolumeArgs
   * @param volumeList
   * @throws IOException
   */
  void applyCreateVolume(OmVolumeArgs omVolumeArgs,
      VolumeList volumeList) throws IOException;

  /**
   * Changes the owner of a volume.
   *
   * @param volume - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  OmVolumeOwnerChangeResponse setOwner(String volume, String owner)
      throws IOException;

  /**
   * Apply Set Owner changes to OM DB.
   * @param oldOwner
   * @param oldOwnerVolumeList
   * @param newOwnerVolumeList
   * @param newOwnerVolumeArgs
   * @throws IOException
   */
  void applySetOwner(String oldOwner, VolumeList oldOwnerVolumeList,
      VolumeList newOwnerVolumeList, OmVolumeArgs newOwnerVolumeArgs)
      throws IOException;

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quota - Quota in bytes.
   * @throws IOException
   */
  OmVolumeArgs setQuota(String volume, long quota) throws IOException;

  /**
   * Apply Set Quota changes to OM DB.
   * @param omVolumeArgs
   * @throws IOException
   */
  void applySetQuota(OmVolumeArgs omVolumeArgs) throws IOException;

  /**
   * Gets the volume information.
   * @param volume - Volume name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  OmVolumeArgs getVolumeInfo(String volume) throws IOException;

  /**
   * Deletes an existing empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  OmDeleteVolumeResponse deleteVolume(String volume) throws IOException;

  /**
   * Apply Delete Volume changes to OM DB.
   * @param volume
   * @param owner
   * @param newVolumeList
   * @throws IOException
   */
  void applyDeleteVolume(String volume, String owner,
      VolumeList newVolumeList) throws IOException;

  /**
   * Checks if the specified user with a role can access this volume.
   *
   * @param volume - volume
   * @param userAcl - user acl which needs to be checked for access
   * @return true if the user has access for the volume, false otherwise
   * @throws IOException
   */
  boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl)
      throws IOException;

  /**
   * Returns a list of volumes owned by a given user; if user is null,
   * returns all volumes.
   *
   * @param userName
   *   volume owner
   * @param prefix
   *   the volume prefix used to filter the listing result.
   * @param startKey
   *   the start volume name determines where to start listing from,
   *   this key is excluded from the result.
   * @param maxKeys
   *   the maximum number of volumes to return.
   * @return a list of {@link OmVolumeArgs}
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumes(String userName, String prefix,
      String startKey, int maxKeys) throws IOException;
}
