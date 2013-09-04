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

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.EmptyPathError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPoolError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPathNameError;

/**
 * A directive to add a path to a cache pool.
 */
public class PathCacheDirective implements Comparable<PathCacheDirective> {
  private final String path;
  private final long poolId;

  public PathCacheDirective(String path, long poolId) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(poolId > 0);
    this.path = path;
    this.poolId = poolId;
  }

  /**
   * @return The path used in this request.
   */
  public String getPath() {
    return path;
  }

  /**
   * @return The pool used in this request.
   */
  public long getPoolId() {
    return poolId;
  }

  /**
   * Check if this PathCacheDirective is valid.
   * 
   * @throws IOException
   *     If this PathCacheDirective is not valid.
   */
  public void validate() throws IOException {
    if (path.isEmpty()) {
      throw new EmptyPathError(this);
    }
    if (!DFSUtil.isValidName(path)) {
      throw new InvalidPathNameError(this);
    }
    if (poolId <= 0) {
      throw new InvalidPoolError(this);
    }
  }

  @Override
  public int compareTo(PathCacheDirective rhs) {
    return ComparisonChain.start().
        compare(poolId, rhs.getPoolId()).
        compare(path, rhs.getPath()).
        result();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).append(poolId).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    try {
      PathCacheDirective other = (PathCacheDirective)o;
      return other.compareTo(this) == 0;
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ path:").append(path).
      append(", poolId:").append(poolId).
      append(" }");
    return builder.toString();
  }
};
