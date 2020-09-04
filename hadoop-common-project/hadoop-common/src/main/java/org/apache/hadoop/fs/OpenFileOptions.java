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

package org.apache.hadoop.fs;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The standard openfile options.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OpenFileOptions {

  /**
   * Prefix for all standard filesystem options: {@value}.
   */
  public static final String FILESYSTEM_OPTION = "fs.option.";

  /**
   * Prefix for all openFile options: {@value}.
   */
  public static final String FS_OPTION_OPENFILE =
      FILESYSTEM_OPTION + "openfile.";

  /**
   * OpenFile option for seek policies: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_LENGTH =
      FS_OPTION_OPENFILE + "length";

  /**
   * OpenFile option for seek policies: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_FADVISE =
      FS_OPTION_OPENFILE + "fadvise";

  /**
   * fadvise policy: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_FADVISE_NORMAL =
      "normal";

  /**
   * fadvise policy: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_FADVISE_SEQUENTIAL =
      "sequential";

  /**
   * fadvise policy: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_FADVISE_RANDOM =
      "random";

  /**
   * fadvise policy: {@value}.
   */
  public static final String FS_OPTION_OPENFILE_FADVISE_ADAPTIVE =
      "adaptive";

  /**
   * Set of standard options which openfile implementations
   * MUST recognize, even if they ignore the actual values.
   */
  public static final Set<String> FS_OPTION_OPENFILE_STANDARD_OPTIONS =
      Stream.of(
          FS_OPTION_OPENFILE_FADVISE,
          FS_OPTION_OPENFILE_LENGTH)
          .collect(Collectors.toSet());

}
