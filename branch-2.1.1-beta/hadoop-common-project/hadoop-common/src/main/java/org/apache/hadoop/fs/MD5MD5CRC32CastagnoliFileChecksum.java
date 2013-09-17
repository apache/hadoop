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

package org.apache.hadoop.fs;

import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataChecksum;

/** For CRC32 with the Castagnoli polynomial */
public class MD5MD5CRC32CastagnoliFileChecksum extends MD5MD5CRC32FileChecksum {
  /** Same as this(0, 0, null) */
  public MD5MD5CRC32CastagnoliFileChecksum() {
    this(0, 0, null);
  }

  /** Create a MD5FileChecksum */
  public MD5MD5CRC32CastagnoliFileChecksum(int bytesPerCRC, long crcPerBlock, MD5Hash md5) {
    super(bytesPerCRC, crcPerBlock, md5);
  }

  @Override
  public DataChecksum.Type getCrcType() {
    // default to the one that is understood by all releases.
    return DataChecksum.Type.CRC32C;
  }
}
