/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold information and location of a RocksDB Checkpoint.
 */
public class RocksDBCheckpoint implements DBCheckpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpoint.class);

  private Path checkpointLocation;
  private long checkpointTimestamp = System.currentTimeMillis();
  private long latestSequenceNumber = -1;
  private long checkpointCreationTimeTaken = 0L;

  public RocksDBCheckpoint(Path checkpointLocation) {
    this.checkpointLocation = checkpointLocation;
  }

  public RocksDBCheckpoint(Path checkpointLocation,
                    long snapshotTimestamp,
                    long latestSequenceNumber,
                    long checkpointCreationTimeTaken) {
    this.checkpointLocation = checkpointLocation;
    this.checkpointTimestamp = snapshotTimestamp;
    this.latestSequenceNumber = latestSequenceNumber;
    this.checkpointCreationTimeTaken = checkpointCreationTimeTaken;
  }

  @Override
  public Path getCheckpointLocation() {
    return this.checkpointLocation;
  }

  @Override
  public long getCheckpointTimestamp() {
    return this.checkpointTimestamp;
  }

  @Override
  public long getLatestSequenceNumber() {
    return this.latestSequenceNumber;
  }

  @Override
  public long checkpointCreationTimeTaken() {
    return checkpointCreationTimeTaken;
  }

  @Override
  public void cleanupCheckpoint() throws IOException {
    LOG.debug("Cleaning up checkpoint at " + checkpointLocation.toString());
    FileUtils.deleteDirectory(checkpointLocation.toFile());
  }
}