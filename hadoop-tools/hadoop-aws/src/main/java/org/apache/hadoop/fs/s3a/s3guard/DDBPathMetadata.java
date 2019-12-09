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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * {@code DDBPathMetadata} wraps {@link PathMetadata} and adds the
 * isAuthoritativeDir flag to provide support for authoritative directory
 * listings in {@link DynamoDBMetadataStore}.
 */
public class DDBPathMetadata extends PathMetadata {

  private boolean isAuthoritativeDir;

  public DDBPathMetadata(PathMetadata pmd, boolean isAuthoritativeDir) {
    super(pmd.getFileStatus(), pmd.isEmptyDirectory(), pmd.isDeleted());
    this.isAuthoritativeDir = isAuthoritativeDir;
  }

  public DDBPathMetadata(PathMetadata pmd) {
    super(pmd.getFileStatus(), pmd.isEmptyDirectory(), pmd.isDeleted());
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(FileStatus fileStatus) {
    super(fileStatus);
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted) {
    super(fileStatus, isEmptyDir, isDeleted);
    this.isAuthoritativeDir = false;
  }

  public DDBPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, boolean isAuthoritativeDir) {
    super(fileStatus, isEmptyDir, isDeleted);
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

}
