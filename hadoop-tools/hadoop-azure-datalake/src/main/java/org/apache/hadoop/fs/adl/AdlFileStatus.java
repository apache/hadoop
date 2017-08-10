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
 *
 */
package org.apache.hadoop.fs.adl;

import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_BLOCK_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys.ADL_REPLICATION_FACTOR;

/**
 * Shim class supporting linking against 2.x clients.
 */
class AdlFileStatus extends FileStatus {

  private static final long serialVersionUID = 0x01fcbe5e;

  private boolean hasAcl = false;

  AdlFileStatus(DirectoryEntry entry, Path path, boolean hasAcl) {
    this(entry, path, entry.user, entry.group, hasAcl);
  }

  AdlFileStatus(DirectoryEntry entry, Path path,
                String owner, String group, boolean hasAcl) {
    super(entry.length, DirectoryEntryType.DIRECTORY == entry.type,
        ADL_REPLICATION_FACTOR, ADL_BLOCK_SIZE,
        entry.lastModifiedTime.getTime(), entry.lastAccessTime.getTime(),
        new AdlPermission(hasAcl, Short.parseShort(entry.permission, 8)),
        owner, group, null, path);
    this.hasAcl = hasAcl;
  }

  @Override
  public boolean hasAcl() {
    return hasAcl;
  }

  @Override
  public boolean equals(Object o) {
    // satisfy findbugs
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    // satisfy findbugs
    return super.hashCode();
  }

}