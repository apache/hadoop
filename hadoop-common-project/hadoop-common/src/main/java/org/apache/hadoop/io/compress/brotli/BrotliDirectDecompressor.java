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

package org.apache.hadoop.io.compress.brotli;

import com.aayushatharva.brotli4j.decoder.Decoder;
import com.aayushatharva.brotli4j.decoder.DecoderJNI;
import com.aayushatharva.brotli4j.decoder.DirectDecompress;
import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BrotliDirectDecompressor implements DirectDecompressor {

  private static final Logger LOG =
          LoggerFactory.getLogger(BrotliDirectDecompressor.class);

  static {
    BrotliCodec.loadNatives();
  }

  @Override
  public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException {

    int toRead = src.remaining();
    byte[] compressed = new byte[toRead];
    src.get(compressed, 0, toRead);
    final DirectDecompress result = Decoder.decompress(compressed);
    final DecoderJNI.Status status = result.getResultStatus();
    if (status == DecoderJNI.Status.DONE) {
      dst.put(result.getDecompressedData());
      src.position(src.limit());
    } else {
      LOG.error("An error occurred while decompressing data: {}", status);
    }
  }
}
