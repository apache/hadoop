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

import com.aayushatharva.brotli4j.encoder.Encoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBrotliCompressorDecompressor {

  @Test
  public void decompress() throws IOException {
    BrotliCodec codec = new BrotliCodec();

    Compressor compressor = codec.createCompressor();
    Configuration config = new Configuration(false);
    config.set(BrotliCodec.MODE_PROP, Encoder.Mode.TEXT.name());
    compressor.reinit(config);

    InputStream textStream = getClass().getResourceAsStream("/brotli/test_file.txt");
    byte[] textBytes = new byte[textStream.available()];
    IOUtils.readFully(textStream, textBytes, 0, textBytes.length);

    compressor.setInput(textBytes, 0, textBytes.length);
    compressor.finish();

    byte[] compressed = new byte[textBytes.length];
    int compressedBytes = compressor.compress(compressed, 0, compressed.length);
    compressor.end();
    assertTrue("Compressed size must be smaller than the input size",
               compressedBytes < textBytes.length);

    Decompressor decompressor = codec.createDecompressor();
    decompressor.setInput(compressed, 0, compressedBytes);

    byte[] result = new byte[textBytes.length];
    int decompressedBytes = decompressor.decompress(result, 0, result.length);
    decompressor.end();

    assertEquals(textBytes.length, decompressedBytes);
    assertEquals(new String(textBytes), new String(result));
  }
}
