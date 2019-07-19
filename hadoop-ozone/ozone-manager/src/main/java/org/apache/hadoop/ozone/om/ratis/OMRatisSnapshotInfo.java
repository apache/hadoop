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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_INDEX;

/**
 * This class captures the {@link OzoneManager#snapshotIndex} and term of the
 * snapshot.
 * Ratis server loads the snapshotInfo during startup and updates the
 * lastApplied index to this snapshotIndex. OM SnaphsotInfo does not contain
 * any files. It is used only to store/ update the last applied index and term.
 */
public class OMRatisSnapshotInfo implements SnapshotInfo {

  static final Logger LOG = LoggerFactory.getLogger(OMRatisSnapshotInfo.class);

  private volatile long term = 0;
  private volatile long snapshotIndex = -1;

  private final File ratisSnapshotFile;

  private final static String TERM_PREFIX = "term";
  private final static String INDEX_PREFIX = "index";
  private final static String SEPARATOR = ":";

  public OMRatisSnapshotInfo(File ratisDir) throws IOException {
    ratisSnapshotFile = new File(ratisDir, OM_RATIS_SNAPSHOT_INDEX);
    loadRatisSnapshotIndex();
  }

  public void updateTerm(long newTerm) {
    term = newTerm;
  }

  public void updateSnapshotIndex(long newSnapshotIndex) {
    snapshotIndex = newSnapshotIndex;
  }

  public void updateTermIndex(long newTerm, long newIndex) {
    this.term = newTerm;
    this.snapshotIndex = newIndex;
  }

  /**
   * Load the snapshot index and term from the snapshot file on disk,
   * if it exists.
   * @throws IOException
   */
  public void loadRatisSnapshotIndex() throws IOException {
    if (ratisSnapshotFile.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(
          ratisSnapshotFile))) {
        String termStr = reader.readLine();
        String indexStr = reader.readLine();

        updateTermIndex(Long.parseLong(termStr.split(SEPARATOR)[1]),
            Long.parseLong(indexStr.split(SEPARATOR)[1]));

      } catch (IOException e) {
        LOG.error("Failed to load Ratis Snapshot file {}", ratisSnapshotFile);
        throw e;
      }
    }
  }

  /**
   * Update and persist the snapshot index and term to disk.
   * @param snapshotIndex new snapshot index to be persisted to disk.
   * @throws IOException
   */
  public void saveRatisSnapshotToDisk(long snapshotIndex) throws IOException {
    updateSnapshotIndex(snapshotIndex);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(
          ratisSnapshotFile))) {
      writer.write(TERM_PREFIX + SEPARATOR + term);
      writer.write("\n");
      writer.write(INDEX_PREFIX + SEPARATOR + snapshotIndex);
      LOG.info("Saved Ratis Snapshot on the OM with snapshotIndex {}",
          snapshotIndex);
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
}
