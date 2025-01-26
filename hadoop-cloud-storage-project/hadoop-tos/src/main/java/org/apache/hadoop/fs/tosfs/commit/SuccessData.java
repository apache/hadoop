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

import org.apache.hadoop.fs.tosfs.util.JsonCodec;
import org.apache.hadoop.fs.tosfs.util.Serializer;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.thirdparty.com.google.common.base.Throwables;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SuccessData implements Serializer {
  private static final JsonCodec<SuccessData> CODEC = new JsonCodec<>(SuccessData.class);

  private String name;
  private boolean success = true;
  private long timestamp;
  private String date;
  private String hostname;
  private String committer;
  private String description;
  private String jobId;
  // Filenames in the commit.
  private final List<String> filenames = new ArrayList<>();

  // Diagnostics information.
  private final Map<String, String> diagnostics = new HashMap<>();

  // No-arg constructor for json serializer, Don't use.
  public SuccessData() {
  }

  public SuccessData(
      String name, boolean success, long timestamp,
      String date, String hostname, String committer,
      String description, String jobId, List<String> filenames) {
    this.name = name;
    this.success = success;
    this.timestamp = timestamp;
    this.date = date;
    this.hostname = hostname;
    this.committer = committer;
    this.description = description;
    this.jobId = jobId;
    this.filenames.addAll(filenames);
  }

  public String name() {
    return name;
  }

  public boolean success() {
    return success;
  }

  public long timestamp() {
    return timestamp;
  }

  public String date() {
    return date;
  }

  public String hostname() {
    return hostname;
  }

  public String committer() {
    return committer;
  }

  public String description() {
    return description;
  }

  public String jobId() {
    return jobId;
  }

  public Map<String, String> diagnostics() {
    return diagnostics;
  }

  public List<String> filenames() {
    return filenames;
  }

  public void recordJobFailure(Throwable thrown) {
    this.success = false;
    String stacktrace = Throwables.getStackTraceAsString(thrown);
    addDiagnosticInfo("exception", thrown.toString());
    addDiagnosticInfo("stacktrace", stacktrace);
  }

  public void addDiagnosticInfo(String key, String value) {
    diagnostics.put(key, value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("success", success)
        .add("timestamp", timestamp)
        .add("date", date)
        .add("hostname", hostname)
        .add("committer", committer)
        .add("description", description)
        .add("jobId", jobId)
        .add("filenames", StringUtils.join(",", filenames))
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public byte[] serialize() throws IOException {
    return CODEC.toBytes(this);
  }

  public static SuccessData deserialize(byte[] data) throws IOException {
    return CODEC.fromBytes(data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, success, timestamp, date, hostname, committer, description, jobId,
        filenames);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof SuccessData)) {
      return false;
    }
    SuccessData that = (SuccessData) o;
    return Objects.equals(name, that.name)
        && Objects.equals(success, that.success)
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(date, that.date)
        && Objects.equals(hostname, that.hostname)
        && Objects.equals(committer, that.committer)
        && Objects.equals(description, that.description)
        && Objects.equals(jobId, that.jobId)
        && Objects.equals(filenames, that.filenames);
  }

  public static class Builder {
    private String name = SuccessData.class.getName();
    private boolean success = true;
    private long timestamp;
    private String date;
    private String hostname;
    private String committer;
    private String description;
    private String jobId;
    private final List<String> filenames = Lists.newArrayList();

    public Builder setName(String nameInput) {
      this.name = nameInput;
      return this;
    }

    public Builder setSuccess(boolean successInput) {
      this.success = successInput;
      return this;
    }

    public Builder setTimestamp(long timestampInput) {
      this.timestamp = timestampInput;
      return this;
    }

    public Builder setDate(String dateInput) {
      this.date = dateInput;
      return this;
    }

    public Builder setHostname(String hostnameInput) {
      this.hostname = hostnameInput;
      return this;
    }

    public Builder setCommitter(String committerInput) {
      this.committer = committerInput;
      return this;
    }

    public Builder setDescription(String descriptionInput) {
      this.description = descriptionInput;
      return this;
    }

    public Builder setJobId(String jobIdInput) {
      this.jobId = jobIdInput;
      return this;
    }

    public Builder addFileNames(Iterable<String> newFileNamesInput) {
      if (newFileNamesInput != null) {
        Iterables.addAll(this.filenames, newFileNamesInput);
      }
      return this;
    }

    public SuccessData build() {
      return new SuccessData(
          name, success, timestamp,
          date, hostname, committer,
          description, jobId, filenames);
    }
  }
}
