
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

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;

import java.util.Set;

/**
 * A Container Report gets processsed by the Node2Container and returns the
 * Report Result class.
 */
public class StorageReportResult {
  private SCMNodeStorageStatMap.ReportStatus status;
  private Set<StorageLocationReport> fullVolumes;
  private Set<StorageLocationReport> failedVolumes;

  StorageReportResult(SCMNodeStorageStatMap.ReportStatus status,
      Set<StorageLocationReport> fullVolumes,
      Set<StorageLocationReport> failedVolumes) {
    this.status = status;
    this.fullVolumes = fullVolumes;
    this.failedVolumes = failedVolumes;
  }

  public SCMNodeStorageStatMap.ReportStatus getStatus() {
    return status;
  }

  public Set<StorageLocationReport> getFullVolumes() {
    return fullVolumes;
  }

  public Set<StorageLocationReport> getFailedVolumes() {
    return failedVolumes;
  }

  static class ReportResultBuilder {
    private SCMNodeStorageStatMap.ReportStatus status;
    private Set<StorageLocationReport> fullVolumes;
    private Set<StorageLocationReport> failedVolumes;

    static ReportResultBuilder newBuilder() {
      return new ReportResultBuilder();
    }

    public ReportResultBuilder setStatus(
        SCMNodeStorageStatMap.ReportStatus newstatus) {
      this.status = newstatus;
      return this;
    }

    public ReportResultBuilder setFullVolumeSet(
        Set<StorageLocationReport> fullVolumesSet) {
      this.fullVolumes = fullVolumesSet;
      return this;
    }

    public ReportResultBuilder setFailedVolumeSet(
        Set<StorageLocationReport> failedVolumesSet) {
      this.failedVolumes = failedVolumesSet;
      return this;
    }

    StorageReportResult build() {
      return new StorageReportResult(status, fullVolumes, failedVolumes);
    }
  }
}