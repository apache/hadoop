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
  private final CachePool pool;

  public PathBasedCacheEntry(long entryId, String path, CachePool pool) {
    Preconditions.checkArgument(entryId > 0);
    this.entryId = entryId;
    Preconditions.checkNotNull(path);
    this.path = path;
    Preconditions.checkNotNull(pool);
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

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{ entryId:").append(entryId).
      append(", path:").append(path).
      append(", pool:").append(pool).
      append(" }");
    return builder.toString();
  }

  public PathBasedCacheDescriptor getDescriptor() {
    return new PathBasedCacheDescriptor(entryId, new Path(path),
        pool.getPoolName());
  }
};
