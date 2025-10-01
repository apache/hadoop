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

package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static java.util.Objects.requireNonNull;

/**
 * API for bulk deletion of objects/files,
 * <i>but not directories</i>.
 * After use, call {@code close()} to release any resources and
 * to guarantee store IOStatistics are updated.
 * <p>
 * Callers MUST have no expectation that parent directories will exist after the
 * operation completes; if an object store needs to explicitly look for and create
 * directory markers, that step will be omitted.
 * <p>
 * Be aware that on some stores (AWS S3) each object listed in a bulk delete counts
 * against the write IOPS limit; large page sizes are counterproductive here, as
 * are attempts at parallel submissions across multiple threads.
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-16823">HADOOP-16823.
 *  Large DeleteObject requests are their own Thundering Herd</a>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface BulkDelete extends IOStatisticsSource, Closeable {

  /**
   * The maximum number of objects/files to delete in a single request.
   * @return a number greater than zero.
   */
  int pageSize();

  /**
   * Base path of a bulk delete operation.
   * All paths submitted in {@link #bulkDelete(Collection)} must be under this path.
   * @return base path of a bulk delete operation.
   */
  Path basePath();

  /**
   * Delete a list of files/objects.
   * <ul>
   *   <li>Files must be under the path provided in {@link #basePath()}.</li>
   *   <li>The size of the list must be equal to or less than the page size
   *       declared in {@link #pageSize()}.</li>
   *   <li>Directories are not supported; the outcome of attempting to delete
   *       directories is undefined (ignored; undetected, listed as failures...).</li>
   *   <li>The operation is not atomic.</li>
   *   <li>The operation is treated as idempotent: network failures may
   *        trigger resubmission of the request -any new objects created under a
   *        path in the list may then be deleted.</li>
   *    <li>There is no guarantee that any parent directories exist after this call.
   *    </li>
   * </ul>
   * @param paths list of paths which must be absolute and under the base path.
   * provided in {@link #basePath()}.
   * @return a list of paths which failed to delete, with the exception message.
   * @throws IOException IO problems including networking, authentication and more.
   * @throws IllegalArgumentException if a path argument is invalid.
   */
  List<Map.Entry<Path, String>> bulkDelete(Collection<Path> paths)
      throws IOException, IllegalArgumentException;

}
