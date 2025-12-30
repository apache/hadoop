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
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_AVRO;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_CSV;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_HBASE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_JSON;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ORC;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_PARQUET;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_VECTOR;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;

public enum AbfsInputPolicy {

  SEQUENTIAL(FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL),
  RANDOM(FS_OPTION_OPENFILE_READ_POLICY_RANDOM),
  ADAPTIVE(FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE);

  private final String policy;

  AbfsInputPolicy(String policy) {
    this.policy = policy;
  }

  @Override
  public String toString() {
    return policy;
  }

  String getPolicy() {
    return policy;
  }

  public static AbfsInputPolicy getPolicy(String name) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    switch (trimmed) {
    // all these options currently map to random IO.
    case FS_OPTION_OPENFILE_READ_POLICY_HBASE:
    case FS_OPTION_OPENFILE_READ_POLICY_RANDOM:
    case FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR:
    case FS_OPTION_OPENFILE_READ_POLICY_ORC:
    case FS_OPTION_OPENFILE_READ_POLICY_PARQUET:
      return RANDOM;

    // handle the sequential formats.
    case FS_OPTION_OPENFILE_READ_POLICY_AVRO:
    case FS_OPTION_OPENFILE_READ_POLICY_CSV:
    case FS_OPTION_OPENFILE_READ_POLICY_JSON:
    case FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL:
    case FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE:
      return SEQUENTIAL;
    default:
      return ADAPTIVE;
    }
  }
}
