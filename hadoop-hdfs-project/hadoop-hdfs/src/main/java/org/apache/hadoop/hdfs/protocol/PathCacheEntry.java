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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Preconditions;

/**
 * An entry in the NameNode's path cache.
 */
public final class PathCacheEntry {
  private final long entryId;
  private final PathCacheDirective directive;

  public PathCacheEntry(long entryId, PathCacheDirective directive) {
    Preconditions.checkArgument(entryId > 0);
    this.entryId = entryId;
    this.directive = directive;
  }

  public long getEntryId() {
    return entryId;
  }

  public PathCacheDirective getDirective() {
    return directive;
  }

  @Override
  public boolean equals(Object o) {
    try {
      PathCacheEntry other = (PathCacheEntry)o;
      return new EqualsBuilder().
          append(this.entryId, other.entryId).
          append(this.directive, other.directive).
          isEquals();
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(entryId).
        append(directive).
        hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ entryId:").append(entryId).
        append(", directive:").append(directive.toString()).
        append(" }");
    return builder.toString();
  }
};
