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

package org.apache.hadoop.fs.s3a.commit.files;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.PartETag;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.ValidationFailure;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.JsonSerialization;

import static org.apache.hadoop.fs.s3a.commit.CommitUtils.validateCollectionClass;
import static org.apache.hadoop.fs.s3a.commit.ValidationFailure.verify;
import static org.apache.hadoop.util.StringUtils.join;

/**
 * This is the serialization format for uploads yet to be committed.
 * <p>
 * It's marked as {@link Serializable} so that it can be passed in RPC
 * calls; for this to work it relies on the fact that java.io ArrayList
 * and LinkedList are serializable. If any other list type is used for etags,
 * it must also be serialized. Jackson expects lists, and it is used
 * to persist to disk.
 * </p>
 * <p>
 * The statistics published through the {@link IOStatisticsSource}
 * interface are the static ones marshalled with the commit data;
 * they may be empty.
 * </p>
 */
@SuppressWarnings("unused")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SinglePendingCommit extends PersistentCommitData
    implements Iterable<String>, IOStatisticsSource {

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 0x10000 + VERSION;

  /** Version marker. */
  private int version = VERSION;

  /**
   * This is the filename of the pending file itself.
   * Used during processing; it's persistent value, if any, is ignored.
   */
  private String filename;

  /** Path URI of the destination. */
  private String uri = "";

  /** ID of the upload. */
  private String uploadId;

  /** Destination bucket. */
  private String bucket;

  /** Destination key in the bucket. */
  private String destinationKey;

  /** When was the upload created? */
  private long created;

  /** When was the upload saved? */
  private long saved;

  /** timestamp as date; no expectation of parseability. */
  private String date;

  /** Job ID, if known. */
  private String jobId = "";

  /** Task ID, if known. */
  private String taskId = "";

  /** Arbitrary notes. */
  private String text = "";

  /** Ordered list of etags. */
  private List<String> etags;

  /**
   * Any custom extra data committer subclasses may choose to add.
   */
  private Map<String, String> extraData = new HashMap<>(0);

  /**
   * IOStatistics.
   */
  @JsonProperty("iostatistics")
  private IOStatisticsSnapshot iostats = new IOStatisticsSnapshot();

  /** Destination file size. */
  private long length;

  public SinglePendingCommit() {
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<SinglePendingCommit> serializer() {
    return new JsonSerialization<>(SinglePendingCommit.class, false, true);
  }

  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws ValidationFailure if the data is invalid
   */
  public static SinglePendingCommit load(FileSystem fs, Path path)
      throws IOException {
    SinglePendingCommit instance = serializer().load(fs, path);
    instance.filename = path.toString();
    instance.validate();
    return instance;
  }

  /**
   * Deserialize via java Serialization API: deserialize the instance
   * and then call {@link #validate()} to verify that the deserialized
   * data is valid.
   * @param inStream input stream
   * @throws IOException IO problem
   * @throws ClassNotFoundException reflection problems
   * @throws ValidationFailure validation failure
   */
  private void readObject(ObjectInputStream inStream) throws IOException,
      ClassNotFoundException {
    inStream.defaultReadObject();
    validate();
  }

  /**
   * Set the various timestamp fields to the supplied value.
   * @param millis time in milliseconds
   */
  public void touch(long millis) {
    created = millis;
    saved = millis;
    date = new Date(millis).toString();
  }

  /**
   * Set the commit data.
   * @param parts ordered list of etags.
   * @throws ValidationFailure if the data is invalid
   */
  public void bindCommitData(List<PartETag> parts) throws ValidationFailure {
    etags = new ArrayList<>(parts.size());
    int counter = 1;
    for (PartETag part : parts) {
      verify(part.getPartNumber() == counter,
          "Expected part number %s but got %s", counter, part.getPartNumber());
      etags.add(part.getETag());
      counter++;
    }
  }

  @Override
  public void validate() throws ValidationFailure {
    verify(version == VERSION, "Wrong version: %s", version);
    verify(StringUtils.isNotEmpty(bucket), "Empty bucket");
    verify(StringUtils.isNotEmpty(destinationKey),
        "Empty destination");
    verify(StringUtils.isNotEmpty(uploadId), "Empty uploadId");
    verify(length >= 0, "Invalid length: " + length);
    destinationPath();
    verify(etags != null, "No etag list");
    validateCollectionClass(etags, String.class);
    for (String etag : etags) {
      verify(StringUtils.isNotEmpty(etag), "Empty etag");
    }
    if (extraData != null) {
      validateCollectionClass(extraData.keySet(), String.class);
      validateCollectionClass(extraData.values(), String.class);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "SinglePendingCommit{");
    sb.append("version=").append(version);
    sb.append(", uri='").append(uri).append('\'');
    sb.append(", destination='").append(destinationKey).append('\'');
    sb.append(", uploadId='").append(uploadId).append('\'');
    sb.append(", created=").append(created);
    sb.append(", saved=").append(saved);
    sb.append(", size=").append(length);
    sb.append(", date='").append(date).append('\'');
    sb.append(", jobId='").append(jobId).append('\'');
    sb.append(", taskId='").append(taskId).append('\'');
    sb.append(", notes='").append(text).append('\'');
    if (etags != null) {
      sb.append(", etags=[");
      sb.append(join(",", etags));
      sb.append(']');
    } else {
      sb.append(", etags=null");
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public byte[] toBytes() throws IOException {
    validate();
    return serializer().toBytes(this);
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    serializer().save(fs, path, this, overwrite);
  }

  /**
   * Build the destination path of the object.
   * @return the path
   * @throws IllegalStateException if the URI is invalid
   */
  public Path destinationPath() {
    Preconditions.checkState(StringUtils.isNotEmpty(uri), "Empty uri");
    try {
      return new Path(new URI(uri));
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot parse URI " + uri);
    }
  }

  /**
   * Get the number of etags.
   * @return the size of the etag list.
   */
  public int getPartCount() {
    return etags.size();
  }

  /**
   * Iterate over the etags.
   * @return an iterator.
   */
  @Override
  public Iterator<String> iterator() {
    return etags.iterator();
  }

  /** @return version marker. */
  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  /**
   * This is the filename of the pending file itself.
   * Used during processing; it's persistent value, if any, is ignored.
   * @return filename
   */
  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  /** @return path URI of the destination. */
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  /** @return ID of the upload. */
  public String getUploadId() {
    return uploadId;
  }

  public void setUploadId(String uploadId) {
    this.uploadId = uploadId;
  }

  /** @return destination bucket. */
  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  /** @return destination key in the bucket. */
  public String getDestinationKey() {
    return destinationKey;
  }

  public void setDestinationKey(String destinationKey) {
    this.destinationKey = destinationKey;
  }

  /**
   * When was the upload created?
   * @return timestamp
   */
  public long getCreated() {
    return created;
  }

  public void setCreated(long created) {
    this.created = created;
  }

  /**
   * When was the upload saved?
   * @return timestamp
   */
  public long getSaved() {
    return saved;
  }

  public void setSaved(long saved) {
    this.saved = saved;
  }

  /**
   * Timestamp as date; no expectation of parseability.
   * @return date string
   */
  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  /** @return Job ID, if known. */
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  /** @return Task ID, if known. */
  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  /**
   * Arbitrary notes.
   * @return any notes
   */
  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  /** @return ordered list of etags. */
  public List<String> getEtags() {
    return etags;
  }

  public void setEtags(List<String> etags) {
    this.etags = etags;
  }

  /**
   * Any custom extra data committer subclasses may choose to add.
   * @return custom data
   */
  public Map<String, String> getExtraData() {
    return extraData;
  }

  public void setExtraData(Map<String, String> extraData) {
    this.extraData = extraData;
  }

  /**
   * Set/Update an extra data entry.
   * @param key key
   * @param value value
   */
  public void putExtraData(String key, String value) {
    extraData.put(key, value);
  }

  /**
   * Destination file size.
   * @return size of destination object
   */
  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public IOStatisticsSnapshot getIOStatistics() {
    return iostats;
  }

  public void setIOStatistics(final IOStatisticsSnapshot ioStatistics) {
    this.iostats = ioStatistics;
  }
}
