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

package org.apache.hadoop.fs.s3a.statistics;

/**
 * Statistics for S3A committers.
 */
public interface CommitterStatistics
    extends S3AStatisticInterface {

  /** A commit has been created. */
  void commitCreated();

  /**
   * Data has been uploaded to be committed in a subsequent operation.
   * @param size size in bytes
   */
  void commitUploaded(long size);

  /**
   * A commit has been completed.
   * @param size size in bytes
   */
  void commitCompleted(long size);

  /** A commit has been aborted. */
  void commitAborted();

  /**
   * A commit was reverted.
   */
  void commitReverted();

  /**
   * A commit failed.
   */
  void commitFailed();

  /**
   * Note that a task has completed.
   * @param success success flag
   */
  void taskCompleted(boolean success);

  /**
   * Note that a job has completed.
   * @param success success flag
   */
  void jobCompleted(boolean success);
}
