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
package org.apache.hadoop.util.noguava;

import javax.annotation.Nullable;
import org.slf4j.LoggerFactory;

public final class Strings {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(Strings.class);

  private Strings() {
  }

  public static String nullToEmpty(@Nullable final String str) {
    return (str == null) ? "" : str;
  }

  public static @Nullable String emptyToNull(@Nullable final String str) {
    return (str == null || str.isEmpty()) ? null : str;
  }

  public static boolean isNullOrEmpty(@Nullable final String str) {
    return str == null || str.isEmpty();
  }
}
