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
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPoolNameError;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.InvalidPathNameError;

/**
 * A directive to add a path to a cache pool.
 */
public class PathCacheDirective implements Comparable<PathCacheDirective> {
  private final String path;

  private final String pool;

  public PathCacheDirective(String path, String pool) throws IOException {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(pool);
    this.path = path;
    this.pool = pool;
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
  public String getPool() {
    return pool;
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
    if (DFSUtil.isValidName(path)) {
      throw new InvalidPathNameError(this);
    }

    if (pool.isEmpty()) {
      throw new InvalidPoolNameError(this);
    }
  }

  @Override
  public int compareTo(PathCacheDirective rhs) {
    return ComparisonChain.start().
        compare(pool, rhs.getPool()).
        compare(path, rhs.getPath()).
        result();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(path).append(pool).hashCode();
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
      append(", pool:").append(pool).
      append(" }");
    return builder.toString();
  }
};
