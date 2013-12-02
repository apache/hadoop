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

/**
 * CachePoolStats describes cache pool statistics.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachePoolStats {
  public static class Builder {
    private long bytesNeeded;
    private long bytesCached;
    private long filesAffected;

    public Builder() {
    }

    public Builder setBytesNeeded(long bytesNeeded) {
      this.bytesNeeded = bytesNeeded;
      return this;
    }

    public Builder setBytesCached(long bytesCached) {
      this.bytesCached = bytesCached;
      return this;
    }

    public Builder setFilesAffected(long filesAffected) {
      this.filesAffected = filesAffected;
      return this;
    }

    public CachePoolStats build() {
      return new CachePoolStats(bytesNeeded, bytesCached, filesAffected);
    }
  };

  private final long bytesNeeded;
  private final long bytesCached;
  private final long filesAffected;

  private CachePoolStats(long bytesNeeded, long bytesCached, long filesAffected) {
    this.bytesNeeded = bytesNeeded;
    this.bytesCached = bytesCached;
    this.filesAffected = filesAffected;
  }

  public long getBytesNeeded() {
    return bytesNeeded;
  }

  public long getBytesCached() {
    return bytesNeeded;
  }

  public long getFilesAffected() {
    return filesAffected;
  }

  public String toString() {
    return new StringBuilder().append("{").
      append("bytesNeeded:").append(bytesNeeded).
      append(", bytesCached:").append(bytesCached).
      append(", filesAffected:").append(filesAffected).
      append("}").toString();
  }
}
