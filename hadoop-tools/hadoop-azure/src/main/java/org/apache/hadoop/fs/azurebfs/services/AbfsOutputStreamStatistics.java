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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Interface for {@link AbfsOutputStream} statistics.
 */
@InterfaceStability.Unstable
public interface AbfsOutputStreamStatistics extends IOStatisticsSource {

  /**
   * Number of bytes to be uploaded.
   *
   * @param bytes number of bytes to upload.
   */
  void bytesToUpload(long bytes);

  /**
   * Records a successful upload and the number of bytes uploaded.
   *
   * @param bytes number of bytes that were successfully uploaded.
   */
  void uploadSuccessful(long bytes);

  /**
   * Records that upload is failed and the number of bytes.
   *
   * @param bytes number of bytes that failed to upload.
   */
  void uploadFailed(long bytes);

  /**
   * Time spent in waiting for tasks to be completed in the blocking queue.
   * @return instance of the DurationTracker that tracks the time for waiting.
   */
  DurationTracker timeSpentTaskWait();

  /**
   * Number of times task queue is shrunk.
   */
  void queueShrunk();

  /**
   * Number of times buffer is written to the service after a write operation.
   */
  void writeCurrentBuffer();

  /**
   * Get the IOStatisticsStore instance from AbfsOutputStreamStatistics.
   * @return instance of IOStatisticsStore which extends IOStatistics.
   */
  IOStatistics getIOStatistics();

  /**
   * Method to form a string of all AbfsOutputStream statistics and their
   * values.
   *
   * @return AbfsOutputStream statistics.
   */
  @Override
  String toString();

}
