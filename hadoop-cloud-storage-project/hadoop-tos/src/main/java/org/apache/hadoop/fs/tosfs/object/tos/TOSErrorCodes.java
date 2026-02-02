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

package org.apache.hadoop.fs.tosfs.object.tos;

import java.util.StringJoiner;

public final class TOSErrorCodes {
  private TOSErrorCodes() {
  }

  // The 409 error codes of HNS
  public static final String DELETE_NON_EMPTY_DIR = "0026-00000013";
  public static final String LOCATED_UNDER_A_FILE = "0026-00000020";
  public static final String COPY_BETWEEN_DIR_AND_FILE = "0026-00000021";
  public static final String PATH_LOCK_CONFLICT = "0026-00000022";
  public static final String RENAME_TO_AN_EXISTED_DIR = "0026-00000025";
  public static final String RENAME_TO_SUB_DIR = "0026-00000026";
  public static final String RENAME_BETWEEN_DIR_AND_FILE = "0026-00000027";

  // The 409 error codes shared by HNS and FNS.
  public static final String APPEND_OFFSET_NOT_MATCHED = "0017-00000208";
  public static final String APPEND_NOT_APPENDABLE = "0017-00000209";


  // The bellow error cannot be solved by retry the request except the code PATH_LOCK_CONFLICT,
  // so need to fail fast.
  public static final String FAST_FAILURE_CONFLICT_ERROR_CODES = new StringJoiner(",")
      .add(DELETE_NON_EMPTY_DIR)
      .add(LOCATED_UNDER_A_FILE)
      .add(COPY_BETWEEN_DIR_AND_FILE)
      .add(RENAME_TO_AN_EXISTED_DIR)
      .add(RENAME_TO_SUB_DIR)
      .add(RENAME_BETWEEN_DIR_AND_FILE)
      .add(APPEND_OFFSET_NOT_MATCHED)
      .add(APPEND_NOT_APPENDABLE)
      .toString();
}
