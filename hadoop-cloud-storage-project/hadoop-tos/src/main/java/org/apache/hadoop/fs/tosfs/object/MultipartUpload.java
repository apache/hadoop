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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;

import java.util.Objects;

public class MultipartUpload implements Comparable<MultipartUpload> {
  private final String key;
  private final String uploadId;
  private final int minPartSize;
  private final int maxPartCount;

  public MultipartUpload(String key, String uploadId, int minPartSize, int maxPartCount) {
    this.key = key;
    this.uploadId = uploadId;
    this.minPartSize = minPartSize;
    this.maxPartCount = maxPartCount;
  }

  public String key() {
    return key;
  }

  public String uploadId() {
    return uploadId;
  }

  public int minPartSize() {
    return minPartSize;
  }

  public int maxPartCount() {
    return maxPartCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof MultipartUpload)) {
      return false;
    }

    MultipartUpload that = (MultipartUpload) o;
    if (!Objects.equals(key, that.key)) {
      return false;
    }
    if (!Objects.equals(uploadId, that.uploadId)) {
      return false;
    }
    if (!Objects.equals(minPartSize, that.minPartSize)) {
      return false;
    }
    return Objects.equals(maxPartCount, that.maxPartCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, uploadId, minPartSize, maxPartCount);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("uploadId", uploadId)
        .add("minPartSize", minPartSize)
        .add("maxPartCount", maxPartCount)
        .toString();
  }

  @Override
  public int compareTo(MultipartUpload o) {
    if (this == o) {
      return 0;
    } else if (o == null) {
      return 1;
    } else if (this.key.compareTo(o.key) == 0) {
      return this.uploadId.compareTo(o.uploadId);
    } else {
      return this.key.compareTo(o.key);
    }
  }
}
