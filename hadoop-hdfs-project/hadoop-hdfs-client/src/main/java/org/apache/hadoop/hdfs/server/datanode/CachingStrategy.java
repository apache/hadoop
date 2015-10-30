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

  public static CachingStrategy newDefaultStrategy() {
    return new CachingStrategy(null, null);
  }

  public static CachingStrategy newDropBehind() {
    return new CachingStrategy(true, null);
  }

  public static class Builder {
    private Boolean dropBehind;
    private Long readahead;

    public Builder(CachingStrategy prev) {
      this.dropBehind = prev.dropBehind;
      this.readahead = prev.readahead;
    }

    public Builder setDropBehind(Boolean dropBehind) {
      this.dropBehind = dropBehind;
      return this;
    }

    public Builder setReadahead(Long readahead) {
      this.readahead = readahead;
      return this;
    }

    public CachingStrategy build() {
      return new CachingStrategy(dropBehind, readahead);
    }
  }

  public CachingStrategy(Boolean dropBehind, Long readahead) {
    this.dropBehind = dropBehind;
    this.readahead = readahead;
  }

  public Boolean getDropBehind() {
    return dropBehind;
  }

  public Long getReadahead() {
    return readahead;
  }

  public String toString() {
    return "CachingStrategy(dropBehind=" + dropBehind +
        ", readahead=" + readahead + ")";
  }
}
