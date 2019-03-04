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

/**
 * Generic DB Checkpoint interface.
 */
public interface DBCheckpoint {

  /**
   * Get Snapshot location.
   */
  Path getCheckpointLocation();

  /**
   * Get Snapshot creation timestamp.
   */
  long getCheckpointTimestamp();

  /**
   * Get last sequence number of Snapshot.
   */
  long getLatestSequenceNumber();

  /**
   * Time taken in milliseconds for the checkpoint to be created.
   */
  long checkpointCreationTimeTaken();

  /**
   * Destroy the contents of the specified checkpoint to ensure
   * proper cleanup of the footprint on disk.
   *
   * @throws IOException if I/O error happens
   */
  void cleanupCheckpoint() throws IOException;

}
