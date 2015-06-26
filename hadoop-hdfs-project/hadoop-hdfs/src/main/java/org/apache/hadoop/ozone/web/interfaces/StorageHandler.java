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

package org.apache.hadoop.ozone.web.interfaces;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;

import java.io.IOException;

/**
 * Storage handler Interface is the Interface between
 * REST protocol and file system.
 *
 * We will have two default implementations of this interface.
 * One for the local file system that is handy while testing
 * and another which will point to the HDFS backend.
 */
@InterfaceAudience.Private
public interface StorageHandler {

  /**
   * Creates a Storage Volume.
   *
   * @param args - Volume Name
   *
   * @throws IOException
   * @throws OzoneException
   */
  void createVolume(VolumeArgs args) throws IOException, OzoneException;


  /**
   * setVolumeOwner - sets the owner of the volume.
   *
   * @param args owner info is present in the args
   *
   * @throws IOException
   * @throws OzoneException
   */
  void setVolumeOwner(VolumeArgs args) throws IOException, OzoneException;


  /**
   * Set Volume Quota.
   *
   * @param args - Has Quota info
   * @param remove - true if the request is to remove the quota
   *
   * @throws IOException
   * @throws OzoneException
   */
  void setVolumeQuota(VolumeArgs args, boolean remove)
      throws IOException, OzoneException;

  /**
   * Checks if a Volume exists and the user specified has access to the
   * Volume.
   *
   * @param args - Volume Args
   *
   * @return - Boolean - True if the user can modify the volume.
   * This is possible for owners of the volume and admin users
   *
   * @throws IOException
   * @throws OzoneException
   */
  boolean checkVolumeAccess(VolumeArgs args) throws IOException, OzoneException;


  /**
   * Returns the List of Volumes owned by the specific user.
   *
   * @param args - UserArgs
   *
   * @return - List of Volumes
   *
   * @throws IOException
   * @throws OzoneException
   */
  ListVolumes listVolumes(UserArgs args) throws IOException, OzoneException;

  /**
   * Deletes an Empty Volume.
   *
   * @param args - Volume Args
   *
   * @throws IOException
   * @throws OzoneException
   */
  void deleteVolume(VolumeArgs args) throws IOException, OzoneException;


  /**
   * Returns Info about the specified Volume.
   *
   * @param args - Volume Args
   *
   * @return VolumeInfo
   *
   * @throws IOException
   * @throws OzoneException
   */
  VolumeInfo getVolumeInfo(VolumeArgs args) throws IOException, OzoneException;
}
