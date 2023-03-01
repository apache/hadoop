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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.ValidationFailure;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.util.JsonSerialization;

/**
 * Summary data saved into a {@code _SUCCESS} marker file.
 *
 * This provides an easy way to determine which committer was used
 * to commit work.
 * <ol>
 *   <li>File length == 0: classic {@code FileOutputCommitter}.</li>
 *   <li>Loadable as {@link SuccessData}:
 *   An S3A committer with name in in {@link #committer} field.</li>
 *   <li>Not loadable? Something else.</li>
 * </ol>
 *
 * This should be considered public, and MUST stay compatible
 * at the JSON format level with that of
 * {@code org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData}
 * <p>
 * The JSON format SHOULD be considered public and evolving
 * with compatibility across versions.
 * <p>
 * All the Java serialization data is different and may change
 * across versions with no stability guarantees other than
 * "manifest summaries MAY be serialized between processes with
 * the exact same version of this binary on their classpaths."
 * That is sufficient for testing in Spark.
 * <p>
 * To aid with Java serialization, the maps and lists are
 * exclusively those which serialize well.
 * IOStatisticsSnapshot has a lot of complexity in marshalling
 * there; this class doesn't worry about concurrent access
 * so is simpler.
 *
 * Note: to deal with scale issues, the S3A committers do not include any
 * more than the number of objects listed in
 * {@link org.apache.hadoop.fs.s3a.commit.CommitConstants#SUCCESS_MARKER_FILE_LIMIT}.
 * This is intended to suffice for basic integration tests.
 * Larger tests should examine the generated files themselves.
 */
@SuppressWarnings("unused")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SuccessData extends PersistentCommitData<SuccessData> {

  private static final Logger LOG = LoggerFactory.getLogger(SuccessData.class);

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@link #serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 1;

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 507133045258460084L + VERSION;

  /**
   * Name to include in persisted data, so as to differentiate from
   * any other manifests: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.files.SuccessData/" + VERSION;

  /**
   * Name of file; includes version marker.
   */
  private String name = NAME;

  /**
   * Did this succeed?
   * It is implicitly true in a _SUCCESS file, but if the file
   * is also saved to a log dir, then it depends on the outcome
   */
  private boolean success = true;

  /** Timestamp of creation. */
  private long timestamp;

  /** Timestamp as date string; no expectation of parseability. */
  private String date;

  /**
   * Host which created the file (implicitly: committed the work).
   */
  private String hostname;

  /**
   * Committer name.
   */
  private String committer;

  /**
   * Description text.
   */
  private String description;

  /** Job ID, if known. */
  private String jobId = "";

  /**
   * Source of the job ID.
   */
  private String jobIdSource = "";

  /**
   * Metrics.
   */
  private Map<String, Long> metrics = new HashMap<>();

  /**
   * Diagnostics information.
   */
  private Map<String, String> diagnostics = new HashMap<>();

  /**
   * Filenames in the commit.
   */
  private List<String> filenames = new ArrayList<>(0);

  /**
   * IOStatistics.
   */
  @JsonProperty("iostatistics")
  private IOStatisticsSnapshot iostatistics = new IOStatisticsSnapshot();

  /**
   * State (committed, aborted).
   */
  private String state;

  /**
   * Stage: last stage executed.
   */
  private String stage;

  @Override
  public void validate() throws ValidationFailure {
    ValidationFailure.verify(name != null,
        "Incompatible file format: no 'name' field");
    ValidationFailure.verify(NAME.equals(name),
        "Incompatible file format: " + name);
  }

  @Override
  public byte[] toBytes(JsonSerialization<SuccessData> serializer) throws IOException {
    return serializer.toBytes(this);
  }

  @Override
  public IOStatistics save(final FileSystem fs,
      final Path path,
      final JsonSerialization<SuccessData> serializer) throws IOException {
    // always set the name field before being saved.
    name = NAME;
    return saveFile(fs, path, this, serializer, true);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "SuccessData{");
    sb.append("committer='").append(committer).append('\'');
    sb.append(", hostname='").append(hostname).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", date='").append(date).append('\'');
    sb.append(", filenames=[").append(
        StringUtils.join(filenames, ", "))
        .append("]");
    sb.append('}');
    return sb.toString();
  }

  /**
   * Dump the metrics (if any) to a string.
   * The metrics are sorted for ease of viewing.
   * @param prefix prefix before every entry
   * @param middle string between key and value
   * @param suffix suffix to each entry
   * @return the dumped string
   */
  public String dumpMetrics(String prefix, String middle, String suffix) {
    return joinMap(metrics, prefix, middle, suffix);
  }

  /**
   * Dump the diagnostics (if any) to a string.
   * @param prefix prefix before every entry
   * @param middle string between key and value
   * @param suffix suffix to each entry
   * @return the dumped string
   */
  public String dumpDiagnostics(String prefix, String middle, String suffix) {
    return joinMap(diagnostics, prefix, middle, suffix);
  }

  /**
   * Join any map of string to value into a string, sorting the keys first.
   * @param map map to join
   * @param prefix prefix before every entry
   * @param middle string between key and value
   * @param suffix suffix to each entry
   * @return a string for reporting.
   */
  protected static String joinMap(Map<String, ?> map,
      String prefix,
      String middle, String suffix) {
    if (map == null) {
      return "";
    }
    List<String> list = new ArrayList<>(map.keySet());
    Collections.sort(list);
    StringBuilder sb = new StringBuilder(list.size() * 32);
    for (String k : list) {
      sb.append(prefix)
          .append(k)
          .append(middle)
          .append(map.get(k))
          .append(suffix);
    }
    return sb.toString();
  }

  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws ValidationFailure if the data is invalid
   */
  public static SuccessData load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading success data from {}", path);
    SuccessData instance = serializer().load(fs, path);
    instance.validate();
    return instance;
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<SuccessData> serializer() {
    return new JsonSerialization<>(SuccessData.class, false, false);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /** @return timestamp of creation. */
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /** @return timestamp as date; no expectation of parseability. */
  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  /**
   * @return host which created the file (implicitly: committed the work).
   */
  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * @return committer name.
   */
  public String getCommitter() {
    return committer;
  }

  public void setCommitter(String committer) {
    this.committer = committer;
  }

  /**
   * @return any description text.
   */
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return any metrics.
   */
  public Map<String, Long> getMetrics() {
    return metrics;
  }

  public void setMetrics(Map<String, Long> metrics) {
    this.metrics = metrics;
  }

  /**
   * @return a list of filenames in the commit.
   */
  public List<String> getFilenames() {
    return filenames;
  }

  public void setFilenames(List<String> filenames) {
    this.filenames = filenames;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(Map<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  /**
   * Add a diagnostics entry.
   * @param key name
   * @param value value
   */
  public void addDiagnostic(String key, String value) {
    diagnostics.put(key, value);
  }

  /** @return Job ID, if known. */
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getJobIdSource() {
    return jobIdSource;
  }

  public void setJobIdSource(final String jobIdSource) {
    this.jobIdSource = jobIdSource;
  }

  @Override
  public IOStatisticsSnapshot getIOStatistics() {
    return iostatistics;
  }

  public void setIOStatistics(final IOStatisticsSnapshot ioStatistics) {
    this.iostatistics = ioStatistics;
  }

  /**
   * Set the success flag.
   * @param success did the job succeed?
   */
  public void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * Get the success flag.
   * @return did the job succeed?
   */
  public boolean getSuccess() {
    return success;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getStage() {
    return stage;
  }

  /**
   * Add a diagnostics entry.
   * @param key name
   * @param value value
   */
  public void putDiagnostic(String key, String value) {
    diagnostics.put(key, value);
  }

  /**
   * Note a failure by setting success flag to false,
   * then add the exception to the diagnostics.
   * @param thrown throwable
   */
  public void recordJobFailure(Throwable thrown) {
    setSuccess(false);
    String stacktrace = ExceptionUtils.getStackTrace(thrown);
    diagnostics.put("exception", thrown.toString());
    diagnostics.put("stacktrace", stacktrace);
  }
}
