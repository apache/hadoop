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

package org.apache.hadoop.hdds.scm.node.states;

import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * A Container/Pipeline Report gets processed by the
 * Node2Container/Node2Pipeline and returns Report Result class.
 */
public final class ReportResult<T> {
  private ReportStatus status;
  private Set<T> missingEntries;
  private Set<T> newEntries;

  private ReportResult(ReportStatus status,
      Set<T> missingEntries,
      Set<T> newEntries) {
    this.status = status;
    Preconditions.checkNotNull(missingEntries);
    Preconditions.checkNotNull(newEntries);
    this.missingEntries = missingEntries;
    this.newEntries = newEntries;
  }

  public ReportStatus getStatus() {
    return status;
  }

  public Set<T> getMissingEntries() {
    return missingEntries;
  }

  public Set<T> getNewEntries() {
    return newEntries;
  }

  /**
   * Result after processing report for node2Object map.
   * @param <T>
   */
  public static class ReportResultBuilder<T> {
    private ReportStatus status;
    private Set<T> missingEntries;
    private Set<T> newEntries;

    public ReportResultBuilder<T> setStatus(
        ReportStatus newStatus) {
      this.status = newStatus;
      return this;
    }

    public ReportResultBuilder<T> setMissingEntries(
        Set<T> missingEntriesList) {
      this.missingEntries = missingEntriesList;
      return this;
    }

    public ReportResultBuilder<T> setNewEntries(
        Set<T> newEntriesList) {
      this.newEntries = newEntriesList;
      return this;
    }

    public ReportResult<T> build() {

      Set<T> nullSafeMissingEntries = this.missingEntries;
      Set<T> nullSafeNewEntries = this.newEntries;
      if (nullSafeNewEntries == null) {
        nullSafeNewEntries = Collections.emptySet();
      }
      if (nullSafeMissingEntries == null) {
        nullSafeMissingEntries = Collections.emptySet();
      }
      return new ReportResult<T>(status, nullSafeMissingEntries,
              nullSafeNewEntries);
    }
  }

  /**
   * Results possible from processing a report.
   */
  public enum ReportStatus {
    ALL_IS_WELL,
    MISSING_ENTRIES,
    NEW_ENTRIES_FOUND,
    MISSING_AND_NEW_ENTRIES_FOUND,
    NEW_DATANODE_FOUND,
  }
}
