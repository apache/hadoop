/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.commit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.util.JsonCodec;
import org.apache.hadoop.fs.tosfs.util.Serializer;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Metadata that will be serialized as json and be saved in the .pending files.
 */
public class Pending implements Serializer {
  private static final JsonCodec<Pending> CODEC = new JsonCodec<>(Pending.class);

  private String bucket;
  private String destKey;
  private String uploadId;
  private long length;
  private long createdTimestamp;
  private List<Part> parts;

  // No-arg constructor for json serializer, don't use.
  public Pending() {
  }

  public Pending(
      String bucket, String destKey,
      String uploadId, long length,
      long createdTimestamp, List<Part> parts) {
    this.bucket = bucket;
    this.destKey = destKey;
    this.uploadId = uploadId;
    this.length = length;
    this.createdTimestamp = createdTimestamp;
    this.parts = parts;
  }

  public String bucket() {
    return bucket;
  }

  public String destKey() {
    return destKey;
  }

  public String uploadId() {
    return uploadId;
  }

  public long length() {
    return length;
  }

  public long createdTimestamp() {
    return createdTimestamp;
  }

  public List<Part> parts() {
    return parts;
  }

  @Override
  public byte[] serialize() throws IOException {
    return CODEC.toBytes(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("bucket", bucket)
        .add("destKey", destKey)
        .add("uploadId", uploadId)
        .add("length", length)
        .add("createdTimestamp", createdTimestamp)
        .add("uploadParts", StringUtils.join(parts, ","))
        .toString();
  }

  public static Pending deserialize(byte[] data) throws IOException {
    return CODEC.fromBytes(data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, destKey, uploadId, length, createdTimestamp, parts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof Pending)) {
      return false;
    }
    Pending that = (Pending) o;
    return Objects.equals(bucket, that.bucket)
        && Objects.equals(destKey, that.destKey)
        && Objects.equals(uploadId, that.uploadId)
        && Objects.equals(length, that.length)
        && Objects.equals(createdTimestamp, that.createdTimestamp)
        && Objects.equals(parts, that.parts);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String bucket;
    private String destKey;
    private String uploadId;
    private long length;
    private long createdTimestamp;
    private final List<Part> parts = Lists.newArrayList();

    public Builder setBucket(String bucketInput) {
      this.bucket = bucketInput;
      return this;
    }

    public Builder setDestKey(String destKeyInput) {
      this.destKey = destKeyInput;
      return this;
    }

    public Builder setUploadId(String uploadIdInput) {
      this.uploadId = uploadIdInput;
      return this;
    }

    public Builder setLength(long lengthInput) {
      this.length = lengthInput;
      return this;
    }

    public Builder setCreatedTimestamp(long createdTimestampInput) {
      this.createdTimestamp = createdTimestampInput;
      return this;
    }

    public Builder addParts(List<Part> partsInput) {
      this.parts.addAll(partsInput);
      return this;
    }

    public Pending build() {
      Preconditions.checkArgument(StringUtils.isNoneEmpty(bucket), "Empty bucket");
      Preconditions.checkArgument(StringUtils.isNoneEmpty(destKey), "Empty object destination key");
      Preconditions.checkArgument(StringUtils.isNoneEmpty(uploadId), "Empty uploadId");
      Preconditions.checkArgument(length >= 0, "Invalid length: %s", length);
      parts.forEach(
          part -> Preconditions.checkArgument(StringUtils.isNoneEmpty(part.eTag(), "Empty etag")));

      return new Pending(bucket, destKey, uploadId, length, createdTimestamp, parts);
    }
  }
}
