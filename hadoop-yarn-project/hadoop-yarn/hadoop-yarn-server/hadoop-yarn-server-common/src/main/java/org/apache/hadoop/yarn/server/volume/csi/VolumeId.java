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
package org.apache.hadoop.yarn.server.volume.csi;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.util.StringUtils;

/**
 * Unique ID for a volume. This may or may not come from a storage system,
 * YARN depends on this ID to recognized volumes and manage their states.
 */
public class VolumeId {

  private final String volumeId;

  public VolumeId(String volumeId) {
    this.volumeId = volumeId;
  }

  public String getId() {
    return this.volumeId;
  }

  @Override
  public String toString() {
    return this.volumeId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof VolumeId)) {
      return false;
    }
    return StringUtils.equalsIgnoreCase(volumeId,
        ((VolumeId) obj).getId());
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hc = new HashCodeBuilder();
    hc.append(volumeId);
    return hc.toHashCode();
  }
}
