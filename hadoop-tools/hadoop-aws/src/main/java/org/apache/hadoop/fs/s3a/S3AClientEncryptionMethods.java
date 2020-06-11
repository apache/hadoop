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

package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * Enum of supported client-side encryption methods.
 */
public enum S3AClientEncryptionMethods {
  KMS("KMS"),
  CUSTOM("CUSTOM"),
  NONE("");

  private String method;

  S3AClientEncryptionMethods(String method) {
    this.method = method;
  }

  public String getMethod() {
    return method;
  }

  public static S3AClientEncryptionMethods getMethod(String name)
          throws IOException {
    if (StringUtils.isBlank(name)) {
      return NONE;
    }
    switch (name) {
    case "KMS":
      return KMS;
    case "CUSTOM":
      return CUSTOM;
    default:
      throw new IOException(
              "Unknown Client Side encryption method " + name);
    }
  }
}
