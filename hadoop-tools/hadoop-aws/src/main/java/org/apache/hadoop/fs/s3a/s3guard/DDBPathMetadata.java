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

import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * {@code DDBPathMetadata} wraps {@link PathMetadata} and adds the
 * isAuthoritativeDir flag to provide support for authoritative directory
 * listings in {@link DynamoDBMetadataStore}.
 */
public class DDBPathMetadata extends PathMetadata {

  private boolean isAuthoritativeDir;

  public DDBPathMetadata(PathMetadata pmd) {
    super(pmd.getFileStatus(), pmd.isEmptyDirectory(), pmd.isDeleted(),
        pmd.getLastUpdated());
    this.isAuthoritativeDir = false;
    this.setLastUpdated(pmd.getLastUpdated());
  }

  public DDBPathMetadata(S3AFileStatus fileStatus) {
    super(fileStatus);
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, long lastUpdated) {
    super(fileStatus, isEmptyDir, isDeleted, lastUpdated);
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(S3AFileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, boolean isAuthoritativeDir, long lastUpdated) {
    super(fileStatus, isEmptyDir, isDeleted, lastUpdated);
    this.isAuthoritativeDir = isAuthoritativeDir;
  }

  public boolean isAuthoritativeDir() {
    return isAuthoritativeDir;
  }

  public void setAuthoritativeDir(boolean authoritativeDir) {
    isAuthoritativeDir = authoritativeDir;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  @Override public String toString() {
    return "DDBPathMetadata{" +
        "isAuthoritativeDir=" + isAuthoritativeDir +
        ", PathMetadata=" + super.toString() +
        '}';
  }
}
