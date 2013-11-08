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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.CachePool;

import com.google.common.base.Preconditions;

/**
 * Represents an entry in the PathBasedCache on the NameNode.
 *
 * This is an implementation class, not part of the public API.
 */
@InterfaceAudience.Private
public final class PathBasedCacheEntry {
  private final long entryId;
  private final String path;
  private final short replication;
  private final CachePool pool;

  public PathBasedCacheEntry(long entryId, String path,
      short replication, CachePool pool) {
    Preconditions.checkArgument(entryId > 0);
    this.entryId = entryId;
    Preconditions.checkArgument(replication > 0);
    this.path = path;
    Preconditions.checkNotNull(pool);
    this.replication = replication;
    Preconditions.checkNotNull(path);
    this.pool = pool;
  }

  public long getEntryId() {
    return entryId;
  }

  public String getPath() {
    return path;
  }

  public CachePool getPool() {
    return pool;
  }

  public short getReplication() {
    return replication;
  }

  public PathBasedCacheDirective toDirective() {
    return new PathBasedCacheDirective.Builder().
        setId(entryId).
        setPath(new Path(path)).
        setReplication(replication).
        setPool(pool.getPoolName()).
        build();
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ entryId:").append(entryId).
      append(", path:").append(path).
      append(", replication:").append(replication).
      append(", pool:").append(pool).
      append(" }");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    PathBasedCacheEntry other = (PathBasedCacheEntry)o;
    return entryId == other.entryId;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(entryId).toHashCode();
  }
};
