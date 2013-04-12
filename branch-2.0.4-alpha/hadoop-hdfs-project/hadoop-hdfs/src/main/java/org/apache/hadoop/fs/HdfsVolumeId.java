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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * HDFS-specific volume identifier which implements {@link VolumeId}. Can be
 * used to differentiate between the data directories on a single datanode. This
 * identifier is only unique on a per-datanode basis.
 * 
 * Note that invalid IDs are represented by {@link VolumeId#INVALID_VOLUME_ID}.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Public
public class HdfsVolumeId implements VolumeId {
  
  private final byte[] id;

  public HdfsVolumeId(byte[] id) {
    if (id == null) {
      throw new NullPointerException("A valid Id can only be constructed " +
      		"with a non-null byte array.");
    }
    this.id = id;
  }

  @Override
  public final boolean isValid() {
    return true;
  }

  @Override
  public int compareTo(VolumeId arg0) {
    if (arg0 == null) {
      return 1;
    }
    if (!arg0.isValid()) {
      // any valid ID is greater 
      // than any invalid ID: 
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
    // NB: if (!obj.isValid()) { return false; } check is not necessary
    // because we have class identity checking above, and for this class
    // isValid() is always true.
    return new EqualsBuilder().append(this.id, that.id).isEquals();
  }

  @Override
  public String toString() {
    return Base64.encodeBase64String(id);
  }
}
