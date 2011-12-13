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
package org.apache.hadoop.hdfs;

import java.io.IOException;

/**
 * For sharing between the local and remote block reader implementations.
 */
class BlockReaderUtil {

  /* See {@link BlockReader#readAll(byte[], int, int)} */
  public static int readAll(BlockReader reader,
      byte[] buf, int offset, int len) throws IOException {
    int n = 0;
    for (;;) {
      int nread = reader.read(buf, offset + n, len - n);
      if (nread <= 0)
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }

  /* See {@link BlockReader#readFully(byte[], int, int)} */
  public static void readFully(BlockReader reader,
      byte[] buf, int off, int len) throws IOException {
    int toRead = len;
    while (toRead > 0) {
      int ret = reader.read(buf, off, toRead);
      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }
}