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
package org.apache.hadoop.tools.rumen;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;

class PossiblyDecompressedInputStream extends InputStream {
  private final Decompressor decompressor;
  private final InputStream coreInputStream;

  public PossiblyDecompressedInputStream(Path inputPath, Configuration conf)
      throws IOException {
    CompressionCodecFactory codecs = new CompressionCodecFactory(conf);
    CompressionCodec inputCodec = codecs.getCodec(inputPath);

    FileSystem ifs = inputPath.getFileSystem(conf);
    FSDataInputStream fileIn = ifs.open(inputPath);

    if (inputCodec == null) {
      decompressor = null;
      coreInputStream = fileIn;
    } else {
      decompressor = CodecPool.getDecompressor(inputCodec);
      coreInputStream = inputCodec.createInputStream(fileIn, decompressor);
    }
  }

  @Override
  public int read() throws IOException {
    return coreInputStream.read();
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    return coreInputStream.read(buffer, offset, length);
  }

  @Override
  public void close() throws IOException {
    if (decompressor != null) {
      CodecPool.returnDecompressor(decompressor);
    }

    coreInputStream.close();
  }

}
