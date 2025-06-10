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

import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * GCS provided validation attributes for a single object.
 */
class VerificationAttributes {
  private final byte[] md5hash;
  private final byte[] crc32c;

  VerificationAttributes(@Nullable byte[] md5hash, @Nullable byte[] crc32c) {
    this.md5hash = md5hash;
    this.crc32c = crc32c;
  }

  /**
   * MD5 hash of an object, if available.
   */
  @Nullable
  byte[] getMd5hash() {
    return md5hash;
  }

  /**
   * CRC32c checksum of an object, if available.
   */
  @Nullable
  byte[] getCrc32c() {
    return crc32c;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VerificationAttributes)) {
      return false;
    }
    VerificationAttributes that = (VerificationAttributes) o;
    return Arrays.equals(md5hash, that.md5hash) && Arrays.equals(crc32c, that.crc32c);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(md5hash);
    result = 31 * result + Arrays.hashCode(crc32c);
    return result;
  }
}
