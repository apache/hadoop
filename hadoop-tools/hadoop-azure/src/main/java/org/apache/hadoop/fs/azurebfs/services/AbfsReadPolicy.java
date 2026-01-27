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

import java.util.Locale;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ORC;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;

/**
 * Enum for ABFS Input Policies.
 * Each policy maps to a particular implementation of {@link AbfsInputStream}
 */
public enum AbfsReadPolicy {

  SEQUENTIAL(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL),
  RANDOM(FS_OPTION_OPENFILE_READ_POLICY_RANDOM),
  ADAPTIVE(FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE);

  private final String readPolicy;

  AbfsReadPolicy(String readPolicy) {
    this.readPolicy = readPolicy;
  }

  @Override
  public String toString() {
    return readPolicy;
  }

  /**
   * Get the enum constant from the string name.
   * @param name policy name as configured by user
   * @return the corresponding AbsInputPolicy to be used
   */
  public static AbfsReadPolicy getAbfsReadPolicy(String name) {
    String readPolicyStr = name.trim().toLowerCase(Locale.ENGLISH);
    switch (readPolicyStr) {
    // all these options currently map to random IO.
    case FS_OPTION_OPENFILE_READ_POLICY_RANDOM:
    case FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR:
    case FS_OPTION_OPENFILE_READ_POLICY_ORC:
    case FS_OPTION_OPENFILE_READ_POLICY_PARQUET:
      return RANDOM;

    // handle the sequential formats.
    case FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL:
    case FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE:
      return SEQUENTIAL;

    // Everything else including ABFS Default Policy maps to Adaptive
    case FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE:
    default:
      return ADAPTIVE;
    }
  }
}
