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

package org.apache.hadoop.ozone.web.localstorage;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;

import java.io.IOException;


/**
 * PLEASE NOTE : This file is a dummy backend for test purposes
 * and prototyping effort only. It does not handle any Object semantics
 * correctly, neither does it take care of security.
 */
@InterfaceAudience.Private
public class LocalStorageHandler implements StorageHandler {
  /**
   * Constructs LocalStorageHandler.
   */
  public LocalStorageHandler() {
  }

  /**
   * Creates Storage Volume.
   *
   * @param args - volumeArgs
   *
   * @throws IOException
   */
  @Override
  public void createVolume(VolumeArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();
    oz.createVolume(args);

  }

  /**
   * setVolumeOwner - sets the owner of the volume.
   *
   * @param args volumeArgs
   *
   * @throws IOException
   */
  @Override
  public void setVolumeOwner(VolumeArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();
    oz.setVolumeProperty(args, OzoneMetadataManager.VolumeProperty.OWNER);
  }

  /**
   * Set Volume Quota Info.
   *
   * @param args - volumeArgs
   * @param remove - true if the request is to remove the quota
   *
   * @throws IOException
   */
  @Override
  public void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();

    if(remove) {
      OzoneQuota quota = new OzoneQuota();
      args.setQuota(quota);
    }
    oz.setVolumeProperty(args, OzoneMetadataManager.VolumeProperty.QUOTA);
  }


  /**
   * Checks if a Volume exists and the user specified has access to the
   * volume.
   *
   * @param args - volumeArgs
   *
   * @return - Boolean - True if the user can modify the volume.
   * This is possible for owners of the volume and admin users
   *
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(VolumeArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();
    return oz.checkVolumeAccess(args);
  }


  /**
   * Returns Info about the specified Volume.
   *
   * @param args - volumeArgs
   *
   * @return VolumeInfo
   *
   * @throws IOException
   */
  @Override
  public VolumeInfo getVolumeInfo(VolumeArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();
    return oz.getVolumeInfo(args);
  }


  /**
   * Deletes an Empty Volume.
   *
   * @param args - Volume Args
   *
   * @throws IOException
   */
  @Override
  public void deleteVolume(VolumeArgs args) throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();
    oz.deleteVolume(args);

  }

  /**
   * Returns the List of Volumes owned by the specific user.
   *
   * @param args - UserArgs
   *
   * @return - List of Volumes
   *
   * @throws IOException
   */
  @Override
  public ListVolumes listVolumes(UserArgs args)
      throws IOException, OzoneException {
    OzoneMetadataManager oz = OzoneMetadataManager.getOzoneMetadataManager();
    return oz.listVolumes(args);
  }

}
