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

package org.apache.hadoop.fs.obs;

import static org.apache.hadoop.fs.obs.OBSConstants.SSE_KEY;
import static org.apache.hadoop.fs.obs.OBSConstants.SSE_TYPE;

import com.obs.services.model.SseCHeader;
import com.obs.services.model.SseKmsHeader;

import org.apache.hadoop.conf.Configuration;

/**
 * Wrapper for Server-Side Encryption (SSE).
 */
class SseWrapper {
  /**
   * SSE-KMS: Server-Side Encryption with Key Management Service.
   */
  private static final String SSE_KMS = "sse-kms";

  /**
   * SSE-C: Server-Side Encryption with Customer-Provided Encryption Keys.
   */
  private static final String SSE_C = "sse-c";

  /**
   * SSE-C header.
   */
  private SseCHeader sseCHeader;

  /**
   * SSE-KMS header.
   */
  private SseKmsHeader sseKmsHeader;

  @SuppressWarnings("deprecation")
  SseWrapper(final Configuration conf) {
    String sseType = conf.getTrimmed(SSE_TYPE);
    if (null != sseType) {
      String sseKey = conf.getTrimmed(SSE_KEY);
      if (sseType.equalsIgnoreCase(SSE_C) && null != sseKey) {
        sseCHeader = new SseCHeader();
        sseCHeader.setSseCKeyBase64(sseKey);
        sseCHeader.setAlgorithm(
            com.obs.services.model.ServerAlgorithm.AES256);
      } else if (sseType.equalsIgnoreCase(SSE_KMS)) {
        sseKmsHeader = new SseKmsHeader();
        sseKmsHeader.setEncryption(
            com.obs.services.model.ServerEncryption.OBS_KMS);
        sseKmsHeader.setKmsKeyId(sseKey);
      }
    }
  }

  boolean isSseCEnable() {
    return sseCHeader != null;
  }

  boolean isSseKmsEnable() {
    return sseKmsHeader != null;
  }

  SseCHeader getSseCHeader() {
    return sseCHeader;
  }

  SseKmsHeader getSseKmsHeader() {
    return sseKmsHeader;
  }
}
