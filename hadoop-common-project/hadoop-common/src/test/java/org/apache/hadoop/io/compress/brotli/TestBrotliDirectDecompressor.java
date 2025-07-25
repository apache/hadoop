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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BrotliCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBrotliDirectDecompressor {

  @Test
  public void decompress() throws IOException {
    String input = "Hello Brotli";

    BrotliCodec codec = new BrotliCodec();

    Compressor compressor = codec.createCompressor();

    byte[] uncompressed = input.getBytes(StandardCharsets.UTF_8);
    compressor.setInput(uncompressed, 0, uncompressed.length);
    compressor.finish();

    byte[] compressed = new byte[/*(int) compressor.getBytesWritten()*/ 16];
    int compressedBytes = compressor.compress(compressed, 0, compressed.length);
    assertEquals(compressed.length, compressedBytes);


    ByteBuffer src = ByteBuffer.wrap(compressed);
    ByteBuffer dest = ByteBuffer.allocateDirect(16);
    BrotliDirectDecompressor decompressor = new BrotliDirectDecompressor();
    decompressor.decompress(src, dest);

    assertFalse(src.hasRemaining());

    dest.flip();
    byte[] result = new byte[input.length()];
    dest.get(result, 0, result.length);
    assertEquals(input, new String(result));
  }

  @Test
  public void decompressLongText() throws IOException {
    InputStream textStream = getClass()
        .getResourceAsStream("/brotli/test_file.txt");
    byte[] textBytes = new byte[textStream.available()];
    IOUtils.readFully(textStream, textBytes, 0, textBytes.length);
    String input = new String(textBytes);

    BrotliCodec codec = new BrotliCodec();

    Compressor compressor = codec.createCompressor();

    byte[] uncompressed = input.getBytes(StandardCharsets.UTF_8);
    compressor.setInput(uncompressed, 0, uncompressed.length);
    compressor.finish();

    byte[] compressed = new byte[textBytes.length];
    int compressedBytes = compressor.compress(compressed, 0, compressed.length);
    compressor.end();
    assertTrue("Compressed size must be smaller than the input size",
               compressedBytes < textBytes.length);

    ByteBuffer src = ByteBuffer.wrap(compressed, 0, compressedBytes);
    ByteBuffer dest = ByteBuffer.allocateDirect(textBytes.length);
    BrotliDirectDecompressor decompressor = new BrotliDirectDecompressor();
    decompressor.decompress(src, dest);

    assertFalse("There is more input to decompress", src.hasRemaining());

    dest.flip();
    byte[] result = new byte[textBytes.length];
    dest.get(result, 0, result.length);
    assertEquals(input, new String(result));
  }
}
