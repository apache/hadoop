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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JSON-file-backed implementation of {@link DatanodeAffinityManager}.
 *
 * <p>Reads affinity groups from a locally stored JSON file.  The file path is
 * configured via {@code dfs.datanode.affinity.file.path} in
 * {@code hdfs-site.xml}.
 *
 * <p>Expected JSON format — an array of affinity group objects:
 * <pre>
 * [
 *   {
 *     "affinityGroupName": "tenant-a",
 *     "regexPattern":      "^/data/tenant-a/.*",
 *     "datanodeRegex":     "^dn-tenant-a[0-9]+\\.example\\.com(:\\d+)?$"
 *   }
 * ]
 * </pre>
 *
 * <p>{@code regexPattern} is matched against the HDFS source path.
 * {@code datanodeRegex} is matched against cluster datanode {@code "hostname:port"}
 * strings by the
 * base class {@link DatanodeAffinityManager#refresh()} to build
 * {@code fileRegexToDataNodeMap}.
 *
 * <p>Call {@code dfsadmin -refreshNodes} to hot-reload the file without a
 * NameNode restart.
 */
public class FileDatanodeAffinityManager extends DatanodeAffinityManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileDatanodeAffinityManager.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Configuration conf;

  // -------------------------------------------------------------------------
  // JSON model
  // -------------------------------------------------------------------------

  /** JSON-deserializable representation of one affinity group object. */
  static final class AffinityGroupEntry {
    @JsonProperty("affinityGroupName") public String affinityGroupName;
    @JsonProperty("regexPattern")      public String regexPattern;
    @JsonProperty("datanodeRegex")     public String datanodeRegex;
  }

  // -------------------------------------------------------------------------
  // Configurable
  // -------------------------------------------------------------------------

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  // -------------------------------------------------------------------------
  // DatanodeAffinityManager — refresh
  // -------------------------------------------------------------------------

  /**
   * Handle the "no file path configured" case (log + no-op), then delegate to
   * {@link DatanodeAffinityManager#refresh()} which calls
   * {@link #loadAffinityRecords()} and rebuilds the map.
   */
  @Override
  public void refresh() {
    String filePath = conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY);
    if (filePath == null || filePath.isEmpty()) {
      LOG.warn("FileDatanodeAffinityManager: {} is not set; "
          + "no affinity groups loaded",
          DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY);
    }
    // Always delegate to the base refresh. loadAffinityRecords() returns an
    // empty list for an unset path, so the base class clears any previously
    // loaded groups AND runs the full reconciliation (restoring formerly
    // isolated nodes to the default topology and rebuilding placement
    // policies). Handling the empty case here with the test-only injection
    // hook would bypass that reconciliation and leave stale runtime state.
    super.refresh();
  }

  // -------------------------------------------------------------------------
  // DatanodeAffinityManager — loadAffinityRecords
  // -------------------------------------------------------------------------

  /**
   * Read the JSON file and return the entries as raw {@link AffinityRecord}s.
   * The base class handles datanode resolution.
   *
   * @throws IOException if the file cannot be read or parsed
   */
  @Override
  protected List<AffinityRecord> loadAffinityRecords() throws IOException {
    String filePath = conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY);
    if (filePath == null || filePath.isEmpty()) {
      // No file configured: no affinity groups. Returning empty (rather than
      // throwing) lets the base refresh clear any previously loaded groups and
      // run the normal reconciliation path.
      return Collections.emptyList();
    }
    return readFromFile(filePath);
  }

  // -------------------------------------------------------------------------
  // File helper — package-private for unit testing
  // -------------------------------------------------------------------------

  @VisibleForTesting
  List<AffinityRecord> readFromFile(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      throw new IOException(
          "FileDatanodeAffinityManager: file not found: " + filePath);
    }
    try {
      AffinityGroupEntry[] entries =
          MAPPER.readValue(file, AffinityGroupEntry[].class);
      List<AffinityRecord> result = new ArrayList<>();
      if (entries == null) {
        // The whole document was JSON null (e.g. the file contains "null").
        LOG.warn("FileDatanodeAffinityManager: {} contained no affinity array;"
            + " treating as no groups", filePath);
        return Collections.emptyList();
      }
      for (AffinityGroupEntry entry : entries) {
        if (entry == null) {
          // A null array element (e.g. "[null, {...}]"): skip it rather than
          // NPE, which would abort the whole refresh and drop valid siblings.
          LOG.warn("FileDatanodeAffinityManager: skipping null affinity entry"
              + " in {}", filePath);
          continue;
        }
        result.add(new AffinityRecord(
            entry.affinityGroupName,
            entry.regexPattern,
            entry.datanodeRegex));
      }
      return Collections.unmodifiableList(result);
    } catch (IOException e) {
      LOG.error("FileDatanodeAffinityManager: failed to parse {}", filePath, e);
      throw new IOException(
          "FileDatanodeAffinityManager: failed to parse " + filePath, e);
    }
  }
}
