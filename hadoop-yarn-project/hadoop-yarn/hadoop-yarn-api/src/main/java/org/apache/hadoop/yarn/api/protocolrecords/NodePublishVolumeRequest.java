/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.api.protocolrecords;

import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.protocolrecords.ValidateVolumeCapabilitiesRequest.VolumeCapability;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * The request sent by node manager to CSI driver adaptor
 * to publish a volume on a node.
 */
public abstract class NodePublishVolumeRequest {

  public static NodePublishVolumeRequest newInstance(String volumeId,
      boolean readOnly, String targetPath, String stagingPath,
      VolumeCapability capability,
      Map<String, String> publishContext,
      Map<String, String> secrets) {
    NodePublishVolumeRequest request =
        Records.newRecord(NodePublishVolumeRequest.class);
    request.setVolumeId(volumeId);
    request.setReadonly(readOnly);
    request.setTargetPath(targetPath);
    request.setStagingPath(stagingPath);
    request.setVolumeCapability(capability);
    request.setPublishContext(publishContext);
    request.setSecrets(secrets);
    return request;
  }

  public abstract void setVolumeId(String volumeId);

  public abstract String getVolumeId();

  public abstract void setReadonly(boolean readonly);

  public abstract boolean getReadOnly();

  public abstract void setTargetPath(String targetPath);

  public abstract String getTargetPath();

  public abstract void setStagingPath(String stagingPath);

  public abstract String getStagingPath();

  public abstract void setVolumeCapability(VolumeCapability capability);

  public abstract VolumeCapability getVolumeCapability();

  public abstract void setPublishContext(Map<String, String> publishContext);

  public abstract Map<String, String> getPublishContext();

  public abstract void setSecrets(Map<String, String> secrets);

  public abstract Map<String, String> getSecrets();

  public String toString() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("VolumeId", getVolumeId());
    jsonObject.addProperty("ReadOnly", getReadOnly());
    jsonObject.addProperty("TargetPath", getTargetPath());
    jsonObject.addProperty("StagingPath", getStagingPath());
    if (getVolumeCapability() != null) {
      JsonObject jsonCap = new JsonObject();
      jsonCap.addProperty("AccessMode",
          getVolumeCapability().getAccessMode().name());
      jsonCap.addProperty("VolumeType",
          getVolumeCapability().getVolumeType().name());
      jsonObject.addProperty("VolumeCapability",
          jsonCap.toString());
    }
    return jsonObject.toString();
  }
}
