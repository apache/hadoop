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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * This class is used to hold the response data for list operations.
 * It contains a list of VersionedFileStatus objects, a map of rename pending JSON paths,
 * continuation token and the executed REST operation.
 */
public class ListResponseData {

  private List<VersionedFileStatus> fileStatusList;
  private Map<Path, Integer> renamePendingJsonPaths;
  private AbfsRestOperation executedRestOperation;
  private String continuationToken;

  /**
   * Returns the list of VersionedFileStatus objects.
   * @return the list of VersionedFileStatus objects
   */
  public List<VersionedFileStatus> getFileStatusList() {
    return fileStatusList;
  }

  /**
   * Sets the list of VersionedFileStatus objects.
   * @param fileStatusList the list of VersionedFileStatus objects
   */
  public void setFileStatusList(final List<VersionedFileStatus> fileStatusList) {
    this.fileStatusList = fileStatusList;
  }

  /**
   * Returns the map of rename pending JSON paths.
   * @return the map of rename pending JSON paths
   */
  public Map<Path, Integer> getRenamePendingJsonPaths() {
    return renamePendingJsonPaths;
  }

  /**
   * Sets the map of rename pending JSON paths.
   * @param renamePendingJsonPaths the map of rename pending JSON paths
   */
  public void setRenamePendingJsonPaths(final Map<Path, Integer> renamePendingJsonPaths) {
    this.renamePendingJsonPaths = renamePendingJsonPaths;
  }

  /**
   * Returns the executed REST operation.
   * @return the executed REST operation
   */
  public AbfsRestOperation getOp() {
    return executedRestOperation;
  }

  /**
   * Sets the executed REST operation.
   * @param executedRestOperation the executed REST operation
   */
  public void setOp(final AbfsRestOperation executedRestOperation) {
    this.executedRestOperation = executedRestOperation;
  }

  /**
   * Returns the continuation token.
   * @return the continuation token
   */
  public String getContinuationToken() {
    return continuationToken;
  }

  /**
   * Sets the continuation token.
   * @param continuationToken the continuation token
   */
  public void setContinuationToken(final String continuationToken) {
    this.continuationToken = continuationToken;
  }
}
