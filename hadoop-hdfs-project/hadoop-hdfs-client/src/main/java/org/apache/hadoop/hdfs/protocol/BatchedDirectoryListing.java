/*
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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A struct-like class for holding partial listings returned by the batched
 * listing API. This class is used internally by the HDFS client and namenode
 * and is not meant for public consumption.
 */
@InterfaceAudience.Private
public class BatchedDirectoryListing {

  private final HdfsPartialListing[] listings;
  private final boolean hasMore;
  private final byte[] startAfter;

  public BatchedDirectoryListing(HdfsPartialListing[] listings,
      boolean hasMore, byte[] startAfter) {
    this.listings = listings;
    this.hasMore = hasMore;
    this.startAfter = startAfter;
  }

  public HdfsPartialListing[] getListings() {
    return listings;
  }

  public boolean hasMore() {
    return hasMore;
  }

  public byte[] getStartAfter() {
    return startAfter;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("listings", listings)
        .append("hasMore", hasMore)
        .append("startAfter", startAfter)
        .toString();
  }
}
