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

import org.apache.commons.lang3.StringUtils;

/**
 * This enum is to centralize the encryption methods and
 * the value required in the configuration.
 *
 * There's two enum values for the two client encryption mechanisms the AWS
 * S3 SDK supports, even though these are not currently supported in S3A.
 * This is to aid supporting CSE in some form in future, fundamental
 * issues about file length of encrypted data notwithstanding.
 *
 */
public enum S3AEncryptionMethods {

  NONE("", false),
  SSE_S3("AES256", true),
  SSE_KMS("SSE-KMS", true),
  SSE_C("SSE-C", true),
  CSE_KMS("CSE-KMS", false),
  CSE_CUSTOM("CSE-CUSTOM", false);

  static final String UNKNOWN_ALGORITHM
      = "Unknown encryption algorithm ";

  private String method;
  private boolean serverSide;

  S3AEncryptionMethods(String method, final boolean serverSide) {
    this.method = method;
    this.serverSide = serverSide;
  }

  public String getMethod() {
    return method;
  }

  /**
   * Flag to indicate this is a server-side encryption option.
   * @return true if this is server side.
   */
  public boolean isServerSide() {
    return serverSide;
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
    for (S3AEncryptionMethods v : values()) {
      if (v.getMethod().equals(name)) {
        return v;
      }
    }
    throw new IOException(UNKNOWN_ALGORITHM + name);
  }

}
