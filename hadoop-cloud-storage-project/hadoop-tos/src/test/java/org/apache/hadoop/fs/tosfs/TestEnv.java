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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.fs.tosfs.util.ParseUtils;

public final class TestEnv {
  public static final String ENV_TOS_UNIT_TEST_ENABLED = "TOS_UNIT_TEST_ENABLED";
  private static final boolean TOS_TEST_ENABLED;

  static {
    TOS_TEST_ENABLED = ParseUtils.envAsBoolean(ENV_TOS_UNIT_TEST_ENABLED, false);
  }

  private TestEnv() {}

  public static boolean checkTestEnabled() {
    return TOS_TEST_ENABLED;
  }
}
