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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

/**
 * OM response for owner change request for a ozone volume.
 */
public class OmVolumeOwnerChangeResponse {
  private VolumeList originalOwnerVolumeList;
  private VolumeList newOwnerVolumeList;
  private OmVolumeArgs newOwnerVolumeArgs;
  private String originalOwner;

  public OmVolumeOwnerChangeResponse(VolumeList originalOwnerVolumeList,
      VolumeList newOwnerVolumeList, OmVolumeArgs newOwnerVolumeArgs,
      String originalOwner) {
    this.originalOwnerVolumeList = originalOwnerVolumeList;
    this.newOwnerVolumeList = newOwnerVolumeList;
    this.newOwnerVolumeArgs = newOwnerVolumeArgs;
    this.originalOwner = originalOwner;
  }

  public String getOriginalOwner() {
    return originalOwner;
  }

  public VolumeList getOriginalOwnerVolumeList() {
    return originalOwnerVolumeList;
  }

  public VolumeList getNewOwnerVolumeList() {
    return newOwnerVolumeList;
  }

  public OmVolumeArgs getNewOwnerVolumeArgs() {
    return newOwnerVolumeArgs;
  }
}
