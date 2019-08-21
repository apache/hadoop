/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_INDEX;

/**
 * This class captures the snapshotIndex and term of the latest snapshot in
 * the OM.
 * Ratis server loads the snapshotInfo during startup and updates the
 * lastApplied index to this snapshotIndex. OM SnapshotInfo does not contain
 * any files. It is used only to store/ update the last applied index and term.
 */
public class OMRatisSnapshotInfo implements SnapshotInfo {

  static final Logger LOG = LoggerFactory.getLogger(OMRatisSnapshotInfo.class);

  private volatile long term = 0;
  private volatile long snapshotIndex = -1;

  private final File ratisSnapshotFile;

  public OMRatisSnapshotInfo(File ratisDir) throws IOException {
    ratisSnapshotFile = new File(ratisDir, OM_RATIS_SNAPSHOT_INDEX);
    loadRatisSnapshotIndex();
  }

  public void updateTerm(long newTerm) {
    term = newTerm;
  }

  private void updateSnapshotIndex(long newSnapshotIndex) {
    snapshotIndex = newSnapshotIndex;
  }

  private void updateTermIndex(long newTerm, long newIndex) {
    this.term = newTerm;
    this.snapshotIndex = newIndex;
  }

  /**
   * Load the snapshot index and term from the snapshot file on disk,
   * if it exists.
   * @throws IOException
   */
  private void loadRatisSnapshotIndex() throws IOException {
    if (ratisSnapshotFile.exists()) {
      RatisSnapshotYaml ratisSnapshotYaml = readRatisSnapshotYaml();
      updateTermIndex(ratisSnapshotYaml.term, ratisSnapshotYaml.snapshotIndex);
    }
  }

  /**
   * Read and parse the snapshot yaml file.
   */
  private RatisSnapshotYaml readRatisSnapshotYaml() throws IOException {
    try (FileInputStream inputFileStream = new FileInputStream(
        ratisSnapshotFile)) {
      Yaml yaml = new Yaml();
      try {
        return yaml.loadAs(inputFileStream, RatisSnapshotYaml.class);
      } catch (Exception e) {
        throw new IOException("Unable to parse RatisSnapshot yaml file.", e);
      }
    }
  }

  /**
   * Update and persist the snapshot index and term to disk.
   * @param index new snapshot index to be persisted to disk.
   * @throws IOException
   */
  public void saveRatisSnapshotToDisk(long index) throws IOException {
    updateSnapshotIndex(index);
    writeRatisSnapshotYaml();
    LOG.info("Saved Ratis Snapshot on the OM with snapshotIndex {}", index);
  }

  /**
   * Write snapshot details to disk in yaml format.
   */
  private void writeRatisSnapshotYaml() throws IOException {
    DumperOptions options = new DumperOptions();
    options.setPrettyFlow(true);
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
    Yaml yaml = new Yaml(options);

    RatisSnapshotYaml ratisSnapshotYaml = new RatisSnapshotYaml(term,
        snapshotIndex);

    try (Writer writer = new OutputStreamWriter(
        new FileOutputStream(ratisSnapshotFile), "UTF-8")) {
      yaml.dump(ratisSnapshotYaml, writer);
    }
  }

  @Override
  public TermIndex getTermIndex() {
    return TermIndex.newTermIndex(term, snapshotIndex);
  }

  @Override
  public long getTerm() {
    return term;
  }

  @Override
  public long getIndex() {
    return snapshotIndex;
  }

  @Override
  public List<FileInfo> getFiles() {
    return null;
  }

  /**
   * Ratis Snapshot details to be written to the yaml file.
   */
  public static class RatisSnapshotYaml {
    private long term;
    private long snapshotIndex;

    public RatisSnapshotYaml() {
      // Needed for snake-yaml introspection.
    }

    RatisSnapshotYaml(long term, long snapshotIndex) {
      this.term = term;
      this.snapshotIndex = snapshotIndex;
    }

    public void setTerm(long term) {
      this.term = term;
    }

    public long getTerm() {
      return this.term;
    }

    public void setSnapshotIndex(long index) {
      this.snapshotIndex = index;
    }

    public long getSnapshotIndex() {
      return this.snapshotIndex;
    }
  }
}
