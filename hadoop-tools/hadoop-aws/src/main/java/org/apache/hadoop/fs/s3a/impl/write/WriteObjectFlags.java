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

package org.apache.hadoop.fs.s3a.impl.write;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE;
import static org.apache.hadoop.fs.Options.CreateFileOptionKeys.FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_MULTIPART;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_CREATE_PERFORMANCE;

/**
 * Flags to use when creating/writing objects.
 * The configuration key is used in two places:
 * <ol>
 *   <li>Parsing builder options</li>
 *   <li>hasCapability() probes of the output stream.</li>
 * </ol>
 */
public enum WriteObjectFlags {
  ConditionalOverwrite(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE),
  ConditionalOverwriteEtag(FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG),
  CreateMultipart(FS_S3A_CREATE_MULTIPART),
  Performance(FS_S3A_CREATE_PERFORMANCE),
  Recursive("");

  /** Configuration key, or "" if not configurable. */
  private final String key;

  /**
   * Constructor.
   * @param key key configuration key, or "" if not configurable.
   */
  WriteObjectFlags(final String key) {
    this.key = key;
  }

  /**
   * does the configuration contain this option as a boolean?
   * @param options options to scan
   * @return true if this is defined as a boolean
   */
  public boolean isEnabled(Configuration options) {
    return options.getBoolean(key, false);
  }

  /**
   * Does the key of this option match the parameter?
   * @param k key
   * @return true if there is a match.
   */
  public boolean hasKey(String k) {
    return !key.isEmpty() && key.equals(k);
  }
}
