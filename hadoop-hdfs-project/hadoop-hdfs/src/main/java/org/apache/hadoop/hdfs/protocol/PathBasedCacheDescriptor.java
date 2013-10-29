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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Preconditions;

/**
 * A directive in a cache pool that includes an identifying ID number.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Public
public final class PathBasedCacheDescriptor extends PathBasedCacheDirective {
  private final long entryId;

  public PathBasedCacheDescriptor(long entryId, Path path,
      short replication, String pool) {
    super(path, replication, pool);
    Preconditions.checkArgument(entryId > 0);
    this.entryId = entryId;
  }

  public long getEntryId() {
    return entryId;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    PathBasedCacheDescriptor other = (PathBasedCacheDescriptor)o;
    return new EqualsBuilder().append(entryId, other.entryId).
        append(getPath(), other.getPath()).
        append(getReplication(), other.getReplication()).
        append(getPool(), other.getPool()).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(entryId).
        append(getPath()).
        append(getReplication()).
        append(getPool()).
        hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ entryId:").append(entryId).
      append(", path:").append(getPath()).
      append(", replication:").append(getReplication()).
      append(", pool:").append(getPool()).
      append(" }");
    return builder.toString();
  }
};
