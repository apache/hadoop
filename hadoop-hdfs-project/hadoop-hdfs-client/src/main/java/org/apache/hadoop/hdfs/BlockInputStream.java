/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;

/**
 * Facade around BlockReader that indeed implements the InputStream interface.
 */
public class BlockInputStream extends InputStream {
  private final BlockReader blockReader;

  public BlockInputStream(BlockReader blockReader) {
    this.blockReader = blockReader;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    int c = blockReader.read(b, 0, b.length);
    if (c > 0) {
      return b[0];
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    return blockReader.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return blockReader.skip(n);
  }
}
