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

import org.apache.hadoop.yarn.util.Records;

/**
 * The request sent by node manager to CSI driver adaptor
 * to un-publish a volume on a node.
 */
public abstract class NodeUnpublishVolumeRequest {

  public static NodeUnpublishVolumeRequest newInstance(String volumeId,
      String targetPath) {
    NodeUnpublishVolumeRequest request =
        Records.newRecord(NodeUnpublishVolumeRequest.class);
    request.setVolumeId(volumeId);
    request.setTargetPath(targetPath);
    return request;
  }

  public abstract void setVolumeId(String volumeId);

  public abstract void setTargetPath(String targetPath);

  public abstract String getVolumeId();

  public abstract String getTargetPath();
}
