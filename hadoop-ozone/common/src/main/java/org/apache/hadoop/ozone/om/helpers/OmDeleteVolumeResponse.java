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
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

/**
 * OM response for delete volume request for a ozone volume.
 */
public  class OmDeleteVolumeResponse {
  private String volume;
  private String owner;
  private VolumeList updatedVolumeList;

  public OmDeleteVolumeResponse(String volume, String owner,
      VolumeList updatedVolumeList) {
    this.volume = volume;
    this.owner = owner;
    this.updatedVolumeList = updatedVolumeList;
  }

  public String getVolume() {
    return volume;
  }

  public String getOwner() {
    return owner;
  }

  public VolumeList getUpdatedVolumeList() {
    return updatedVolumeList;
  }
}