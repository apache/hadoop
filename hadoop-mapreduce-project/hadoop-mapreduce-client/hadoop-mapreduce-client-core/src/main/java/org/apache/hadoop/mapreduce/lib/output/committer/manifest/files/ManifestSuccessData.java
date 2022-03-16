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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.util.JsonSerialization;

/**
 * Summary data saved into a {@code _SUCCESS} marker file.
 *
 * This is a copy of the S3A committer success data format, with
 * a goal of being/remaining compatible.
 * This makes it easier for tests in downstream modules to
 * be able to parse the success files from any of the committers.
 *
 * This should be considered public; it is based on the S3A
 * format, which has proven stable over time.
 *
 * The JSON format SHOULD be considered public and evolving
 * with compatibility across versions.
 *
 * All the Java serialization data is different and may change
 * across versions with no stability guarantees other than
 * "manifest summaries MAY be serialized between processes with
 * the exact same version of this binary on their classpaths."
 * That is sufficient for testing in Spark.
 *
 * To aid with Java serialization, the maps and lists are
 * exclusively those which serialize well.
 * IOStatisticsSnapshot has a lot of complexity in marshalling
 * there; this class doesn't worry about concurrent access
 * so is simpler.
 *
 */
@SuppressWarnings({"unused", "CollectionDeclaredAsConcreteClass"})
@InterfaceAudience.Public
@InterfaceStability.Unstable
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ManifestSuccessData
    extends AbstractManifestData<ManifestSuccessData> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ManifestSuccessData.class);

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@link #serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 1;

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 4755993198698104084L + VERSION;

  /**
   * Name to include in persisted data, so as to differentiate from
   * any other manifests: {@value}.
   */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.commit.files.SuccessData/" + VERSION;

  /**
   * Name of file; includes version marker.
   */
  private String name;

  /** Timestamp of creation. */
  private long timestamp;

  /**
   * Did this succeed?
   * It is implicitly true in a _SUCCESS file, but if the file
   * is also saved to a log dir, then it depends on the outcome
   */
  private boolean success = true;

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
   * Uses a treemap for serialization.
   */
  private TreeMap<String, Long> metrics = new TreeMap<>();

  /**
   * Diagnostics information.
   * Uses a treemap for serialization.
   */
  private TreeMap<String, String> diagnostics = new TreeMap<>();

  /**
   * Filenames in the commit.
   */
  private ArrayList<String> filenames = new ArrayList<>(0);

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
  public ManifestSuccessData validate() throws IOException {
    verify(name != null,
        "Incompatible file format: no 'name' field");
    verify(NAME.equals(name),
        "Incompatible file format: " + name);
    return this;
  }

  @Override
  public JsonSerialization<ManifestSuccessData> createSerializer() {
    return serializer();
  }

  @Override
  public byte[] toBytes() throws IOException {
    return serializer().toBytes(this);
  }

  /**
   * To JSON.
   * @return json string value.
   * @throws IOException failure
   */
  public String toJson() throws IOException {
    return serializer().toJson(this);
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    // always set the name field before being saved.
    name = NAME;
    serializer().save(fs, path, this, overwrite);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ManifestSuccessData{");
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
   */
  public static ManifestSuccessData load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading success data from {}", path);
    ManifestSuccessData instance = serializer().load(fs, path);
    instance.validate();
    return instance;
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<ManifestSuccessData> serializer() {
    return new JsonSerialization<>(ManifestSuccessData.class, false, true);
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

  public void setMetrics(TreeMap<String, Long> metrics) {
    this.metrics = metrics;
  }

  /**
   * @return a list of filenames in the commit.
   */
  public List<String> getFilenames() {
    return filenames;
  }

  /**
   * Get the list of filenames as paths.
   * @return the paths.
   */
  @JsonIgnore
  public List<Path> getFilenamePaths() {
    return getFilenames().stream()
        .map(AbstractManifestData::unmarshallPath)
        .collect(Collectors.toList());
  }

  /**
   * Set the list of filename paths.
   */
  @JsonIgnore
  public void setFilenamePaths(List<Path> paths) {
    setFilenames(new ArrayList<>(
        paths.stream()
            .map(AbstractManifestData::marshallPath)
            .collect(Collectors.toList())));
  }

  public void setFilenames(ArrayList<String> filenames) {
    this.filenames = filenames;
  }

  public Map<String, String> getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(TreeMap<String, String> diagnostics) {
    this.diagnostics = diagnostics;
  }

  /**
   * Add a diagnostics entry.
   * @param key name
   * @param value value
   */
  public void putDiagnostic(String key, String value) {
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
   * Set the IOStatistics to a snapshot of the source.
   * @param iostats. Statistics; may be null.
   */
  public void snapshotIOStatistics(IOStatistics iostats) {
    setIOStatistics(IOStatisticsSupport.snapshotIOStatistics(iostats));
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
   * Note a failure by setting success flag to false,
   * then add the exception to the diagnostics.
   * @param thrown throwable
   */
  public void recordJobFailure(Throwable thrown) {
    setSuccess(false);
    String stacktrace = ExceptionUtils.getStackTrace(thrown);
    diagnostics.put(DiagnosticKeys.EXCEPTION, thrown.toString());
    diagnostics.put(DiagnosticKeys.STACKTRACE, stacktrace);
  }
}
