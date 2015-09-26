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
package org.apache.hadoop.fs;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * HDFS-specific volume identifier which implements {@link VolumeId}. Can be
 * used to differentiate between the data directories on a single datanode. This
 * identifier is only unique on a per-datanode basis.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Public
public class HdfsVolumeId implements VolumeId {
  
  private final byte[] id;

  public HdfsVolumeId(byte[] id) {
    Preconditions.checkNotNull(id, "id cannot be null");
    this.id = id;
  }

  @Override
  public int compareTo(VolumeId arg0) {
    if (arg0 == null) {
      return 1;
    }
    return hashCode() - arg0.hashCode();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(id).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    HdfsVolumeId that = (HdfsVolumeId) obj;
    return new EqualsBuilder().append(this.id, that.id).isEquals();
  }

  @Override
  public String toString() {
    return StringUtils.byteToHexString(id);
  }
}
