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

package org.apache.hadoop.ozone.web.client;

import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.VolumeInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Ozone Volume Class.
 */
public class OzoneVolume {
  private VolumeInfo volumeInfo;
  private Map<String, String> headerMap;
  private final OzoneClient client;

  /**
   * Constructor for OzoneVolume.
   */
  public OzoneVolume(OzoneClient client) {
    this.client = client;
    this.headerMap = new HashMap<>();
  }

  /**
   * Constructor for OzoneVolume.
   *
   * @param volInfo - volume Info.
   * @param client  Client
   */
  public OzoneVolume(VolumeInfo volInfo, OzoneClient client) {
    this.volumeInfo = volInfo;
    this.client = client;
  }

  public String getJsonString() throws IOException {
    return volumeInfo.toJsonString();
  }

  /**
   * sets the Volume Info.
   *
   * @param volInfoString - Volume Info String
   */
  public void setVolumeInfo(String volInfoString) throws IOException {
    this.volumeInfo = VolumeInfo.parse(volInfoString);
  }

  /**
   * Returns volume Name.
   *
   * @return Volume Name.
   */
  public String getVolumeName() {
    return this.volumeInfo.getVolumeName();
  }

  /**
   * Get created by.
   *
   * @return String
   */
  public String getCreatedby() {
    return this.volumeInfo.getCreatedBy();
  }

  /**
   * returns the Owner name.
   *
   * @return String
   */
  public String getOwnerName() {
    return this.volumeInfo.getOwner().getName();
  }

  /**
   * Returns Quota Info.
   *
   * @return Quota
   */
  public OzoneQuota getQuota() {
    return volumeInfo.getQuota();
  }

  /**
   * Returns a Http header from the Last Volume related call.
   *
   * @param headerName - Name of the header
   * @return - Header Value
   */
  public String getHeader(String headerName) {
    return headerMap.get(headerName);
  }

  /**
   * Gets the Client, this is used by Bucket and Key Classes.
   *
   * @return - Ozone Client
   */
  OzoneClient getClient() {
    return client;
  }
}
