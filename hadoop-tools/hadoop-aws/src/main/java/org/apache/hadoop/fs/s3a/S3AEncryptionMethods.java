/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

/**
 * This enum is to centralize the encryption methods and
 * the value required in the configuration.
 */
public enum S3AEncryptionMethods {

  SSE_S3("AES256"),
  SSE_KMS("SSE-KMS"),
  SSE_C("SSE-C"),
  NONE("");

  static final String UNKNOWN_ALGORITHM
      = "Unknown Server Side Encryption algorithm ";

  private String method;

  S3AEncryptionMethods(String method) {
    this.method = method;
  }

  public String getMethod() {
    return method;
  }


  /**
   * Get the encryption mechanism from the value provided.
   * @param name algorithm name
   * @return the method
   * @throws IOException if the algorithm is unknown
   */
  public static S3AEncryptionMethods getMethod(String name) throws IOException {
    if(StringUtils.isBlank(name)) {
      return NONE;
    }
    switch(name) {
    case "AES256":
      return SSE_S3;
    case "SSE-KMS":
      return SSE_KMS;
    case "SSE-C":
      return SSE_C;
    default:
      throw new IOException(UNKNOWN_ALGORITHM + name);
    }
  }

}
