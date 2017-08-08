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

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.ozone.ksm.helpers.KsmOzoneAclMap;
import org.apache.hadoop.ozone.ksm.helpers.KsmVolumeArgs;

/**
 * A class that encapsulates OzoneVolume.
 */
public class OzoneVolume {

  /**
   * Admin Name of the Volume.
   */
  private final String adminName;
  /**
   * Owner of the Volume.
   */
  private final String ownerName;
  /**
   * Name of the Volume.
   */
  private final String volumeName;
  /**
   * Quota allocated for the Volume.
   */
  private final long quotaInBytes;
  /**
   * Volume ACLs.
   */
  private final KsmOzoneAclMap aclMap;

  /**
   * Constructs OzoneVolume from KsmVolumeArgs.
   *
   * @param ksmVolumeArgs
   */
  public OzoneVolume(KsmVolumeArgs ksmVolumeArgs) {
    this.adminName = ksmVolumeArgs.getAdminName();
    this.ownerName = ksmVolumeArgs.getOwnerName();
    this.volumeName = ksmVolumeArgs.getVolume();
    this.quotaInBytes = ksmVolumeArgs.getQuotaInBytes();
    this.aclMap = ksmVolumeArgs.getAclMap();
  }

  /**
   * Returns Volume's admin name.
   *
   * @return adminName
   */
  public String getAdminName() {
    return adminName;
  }

  /**
   * Returns Volume's owner name.
   *
   * @return ownerName
   */
  public String getOwnerName() {
    return ownerName;
  }

  /**
   * Returns Volume name.
   *
   * @return volumeName
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns Quota allocated for the Volume in bytes.
   *
   * @return quotaInBytes
   */
  public long getQuota() {
    return quotaInBytes;
  }

  /**
   * Returns OzoneAcl list associated with the Volume.
   *
   * @return aclMap
   */
  public KsmOzoneAclMap getAclMap() {
    return aclMap;
  }
}