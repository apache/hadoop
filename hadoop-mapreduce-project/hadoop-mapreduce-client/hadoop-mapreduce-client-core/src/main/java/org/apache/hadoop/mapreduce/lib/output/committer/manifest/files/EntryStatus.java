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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * Status of a file or dir entry, designed to be marshalled as
 * an integer -the ordinal value of the enum is the
 * wire value.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum EntryStatus {

  unknown,
  not_found,
  file,
  dir,
  created_dir;

  /**
   * Go from a marshalled type to a status value.
   * Any out of range value is converted to unknown.
   * @param type type
   * @return the status value.
   */
  public static EntryStatus toEntryStatus(int type) {
    switch (type) {
    case 1:
      return not_found;
    case 2:
      return file;
    case 3:
      return dir;
    case 4:
      return created_dir;
    case 0:
    default:
      return unknown;
    }
  }


  /**
   * Go from the result of a getFileStatus call or
   * listing entry to a status.
   * A null argument is mapped to {@link #not_found}
   * @param st file status
   * @return the status enum.
   */
  public static EntryStatus toEntryStatus(@Nullable FileStatus st) {

    if (st == null) {
      return not_found;
    }
    if (st.isDirectory()) {
      return dir;
    }
    if (st.isFile()) {
      return file;
    }
    return unknown;
  }


}
