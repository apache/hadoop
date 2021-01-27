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
package org.apache.hadoop.hdfs.server.datanode;

/**
 * The caching strategy we should use for an HDFS read or write operation.
 */
public class CachingStrategy {
  private final Boolean dropBehind; // null = use server defaults
  private final Long readahead; // null = use server defaults

  /** flag to indicate if a Provided block is to be read using
  a read through strategy. **/
  private final Boolean readThrough;

  public static CachingStrategy newDefaultStrategy() {
    return new CachingStrategy(null, null);
  }

  public static CachingStrategy newDropBehind() {
    return new CachingStrategy(true, null);
  }

  public static class Builder {
    private Boolean dropBehind;
    private Long readahead;
    private Boolean readThrough;

    public Builder(CachingStrategy prev) {
      this.dropBehind = prev.dropBehind;
      this.readahead = prev.readahead;
      this.readThrough = prev.readThrough;
    }

    public Builder setDropBehind(Boolean dropBehind) {
      this.dropBehind = dropBehind;
      return this;
    }

    public Builder setReadahead(Long readahead) {
      this.readahead = readahead;
      return this;
    }

    public Builder setReadThrough(boolean readThrough) {
      this.readThrough = readThrough;
      return this;
    }

    public CachingStrategy build() {
      return new CachingStrategy(dropBehind, readahead, readThrough);
    }
  }

  public CachingStrategy(Boolean dropBehind, Long readahead) {
    this(dropBehind, readahead, false);
  }

  public CachingStrategy(Boolean dropBehind, Long readahead,
      Boolean readThrough) {
    this.dropBehind = dropBehind;
    this.readahead = readahead;
    this.readThrough = readThrough;
  }

  public Boolean getDropBehind() {
    return dropBehind;
  }

  public Long getReadahead() {
    return readahead;
  }

  public Boolean getReadThrough() {
    return readThrough;
  }

  public String toString() {
    return "CachingStrategy(dropBehind=" + dropBehind +
        ", readahead=" + readahead + ", readThrough=" + readThrough + ")";
  }
}
