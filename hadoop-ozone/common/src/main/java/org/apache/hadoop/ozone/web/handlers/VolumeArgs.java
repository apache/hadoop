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

package org.apache.hadoop.ozone.web.handlers;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.request.OzoneQuota;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.UriInfo;

/**
 * VolumeArgs is used to package all volume
 * related arguments in the call to underlying
 * file system.
 */
@InterfaceAudience.Private
public class VolumeArgs extends UserArgs {
  private String adminName;
  private final String volumeName;
  private OzoneQuota quota;

  /**
   * Returns Quota Information.
   *
   * @return Quota
   */
  public OzoneQuota getQuota() {
    return quota;
  }

  /**
   * Returns volume name.
   *
   * @return String
   */
  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Constructs  volume Args.
   *
   * @param userName - User name
   * @param volumeName - volume Name
   * @param requestID _ Request ID
   * @param hostName - Host Name
   * @param request  - Http Request
   * @param info - URI info
   * @param headers - http headers
   * @param groups - list of groups allowed to access the volume
   */
  @SuppressWarnings("parameternumber")
  public VolumeArgs(String userName, String volumeName, String requestID,
                    String hostName, Request request, UriInfo info,
                    HttpHeaders headers, String[] groups) {
    super(userName, requestID, hostName, request, info, headers);
    super.setGroups(groups);
    this.volumeName = volumeName;
  }

  /**
   * Constructs  volume Args.
   *
   * @param volumeName - volume Name
   * @param userArgs - userArgs
   */
  public VolumeArgs(String volumeName, UserArgs userArgs) {
    this(userArgs.getUserName(), volumeName, userArgs.getRequestID(),
         userArgs.getHostName(), userArgs.getRequest(), userArgs.getUri(),
         userArgs.getHeaders(), userArgs.getGroups());
  }

  /**
   * Creates VolumeArgs from another VolumeArgs.
   */
  public VolumeArgs(VolumeArgs volArgs) {
    this(volArgs.getVolumeName(), volArgs);
  }

  /**
   * Sets Quota information.
   *
   * @param quota - Quota Sting
   * @throws IllegalArgumentException
   */
  public void setQuota(String quota) throws IllegalArgumentException {
    this.quota = OzoneQuota.parseQuota(quota);
  }

  /**
   * Sets quota information.
   *
   * @param quota - OzoneQuota
   */
  public void setQuota(OzoneQuota quota) {
    this.quota = quota;
  }

  /**
   * Gets admin Name.
   *
   * @return - Admin Name
   */
  public String getAdminName() {
    return adminName;
  }

  /**
   * Sets Admin Name.
   *
   * @param adminName - Admin Name
   */
  public void setAdminName(String adminName) {
    this.adminName = adminName;
  }

  /**
   * Returns UserName/VolumeName.
   *
   * @return String
   */
  @Override
  public String getResourceName() {
    return super.getResourceName() + "/" + getVolumeName();
  }
}
