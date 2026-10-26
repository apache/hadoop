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

package org.apache.hadoop.fs.gs;

import static org.apache.hadoop.thirdparty.com.google.common.base.Strings.isNullOrEmpty;

import javax.annotation.Nullable;

/**
 * Holder class for string values that should not be logged and displayed when {@code toString}
 * method called. For example, it should be used for credentials.
 */
class RedactedString {

  private final String value;

  RedactedString(String value) {
    this.value = value;
  }

  @Nullable
  static RedactedString create(@Nullable String value) {
    return isNullOrEmpty(value) ? null : new RedactedString(value);
  }

  String getValue() {
    return value;
  }

  @Override
  public final String toString() {
    return "<redacted>";
  }
}
