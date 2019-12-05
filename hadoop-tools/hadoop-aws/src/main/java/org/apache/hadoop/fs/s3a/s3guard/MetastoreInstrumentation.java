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

package org.apache.hadoop.fs.s3a.s3guard;

/**
 * Instrumentation exported to S3Guard.
 */
public interface MetastoreInstrumentation {

  /** Initialized event. */
  void initialized();

  /** Store has been closed. */
  void storeClosed();

  /**
   * Throttled request.
   */
  void throttled();

  /**
   * S3Guard is retrying after a (retryable) failure.
   */
  void retrying();

  /**
   * Records have been deleted.
   * @param count the number of records deleted.
   */
  void recordsDeleted(int count);

  /**
   * Records have been read.
   * @param count the number of records read
   */
  void recordsRead(int count);

  /**
   * records have been written (including tombstones).
   * @param count number of records written.
   */
  void recordsWritten(int count);

  /**
   * A directory has been tagged as authoritative.
   */
  void directoryMarkedAuthoritative();

  /**
   * An entry was added.
   * @param durationNanos time to add
   */
  void entryAdded(long durationNanos);
}
