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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.tosfs.util.JsonCodec;
import org.apache.hadoop.fs.tosfs.util.Serializer;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Lists;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PendingSet implements Serializer {
  private static final JsonCodec<PendingSet> CODEC = new JsonCodec<>(PendingSet.class);

  private String jobId;
  private List<Pending> pendings;
  private Map<String, String> extraData;

  // No-arg constructor for json serializer, don't use.
  public PendingSet() {
  }

  public PendingSet(String jobId) {
    this(jobId, Lists.newArrayList());
  }

  public PendingSet(String jobId, List<Pending> pendings) {
    this.jobId = jobId;
    this.pendings = Lists.newArrayList(pendings);
    this.extraData = Maps.newHashMap();
  }

  public PendingSet addAll(Iterable<Pending> items) {
    Iterables.addAll(pendings, items);
    return this;
  }

  public PendingSet add(Pending pending) {
    pendings.add(pending);
    return this;
  }

  public PendingSet addExtraData(String key, String val) {
    extraData.put(key, val);
    return this;
  }

  public String jobId() {
    return jobId;
  }

  public List<Pending> commits() {
    return pendings;
  }

  public Map<String, String> extraData() {
    return extraData;
  }

  public int size() {
    return pendings.size();
  }

  @Override
  public byte[] serialize() throws IOException {
    return CODEC.toBytes(this);
  }

  public static PendingSet deserialize(byte[] data) {
    try {
      return CODEC.fromBytes(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static PendingSet deserialize(FileSystem fs, FileStatus f) {
    try {
      return deserialize(CommitUtils.load(fs, f.getPath()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, pendings, extraData);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PendingSet)) {
      return false;
    }
    PendingSet that = (PendingSet) o;
    return Objects.equals(jobId, that.jobId)
        && Objects.equals(pendings, that.pendings)
        && Objects.equals(extraData, that.extraData);
  }
}
