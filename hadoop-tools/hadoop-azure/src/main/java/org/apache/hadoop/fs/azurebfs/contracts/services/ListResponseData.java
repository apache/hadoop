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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

public class ListResponseData {

  private List<FileStatus> fileStatusList;
  private Map<Path, Integer> renamePendingJsonPaths;
  private AbfsRestOperation executedRestOperation;
  private String continuationToken;

  public List<FileStatus> getFileStatusList() {
    return fileStatusList;
  }

  public void setFileStatusList(final List<FileStatus> fileStatusList) {
    this.fileStatusList = fileStatusList;
  }

  public Map<Path, Integer> getRenamePendingJsonPaths() {
    return renamePendingJsonPaths;
  }

  public void setRenamePendingJsonPaths(final Map<Path, Integer> renamePendingJsonPaths) {
    this.renamePendingJsonPaths = renamePendingJsonPaths;
  }

  public AbfsRestOperation getOp() {
    return executedRestOperation;
  }

  public void setOp(final AbfsRestOperation executedRestOperation) {
    this.executedRestOperation = executedRestOperation;
  }

  public String getContinuationToken() {
    return continuationToken;
  }

  public void setContinuationToken(final String continuationToken) {
    this.continuationToken = continuationToken;
  }
}
