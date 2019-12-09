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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class InMemoryMapOutput<K, V> extends IFileWrappedMapOutput<K, V> {
  private static final Logger LOG =
      LoggerFactory.getLogger(InMemoryMapOutput.class);
  private final byte[] memory;
  private BoundedByteArrayOutputStream byteStream;
  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final Decompressor decompressor;

  public InMemoryMapOutput(Configuration conf, TaskAttemptID mapId,
                           MergeManagerImpl<K, V> merger,
                           int size, CompressionCodec codec,
                           boolean primaryMapOutput) {
    super(conf, merger, mapId, (long)size, primaryMapOutput);
    this.codec = codec;
    byteStream = new BoundedByteArrayOutputStream(size);
    memory = byteStream.getBuffer();
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
    } else {
      decompressor = null;
    }
  }

  public byte[] getMemory() {
    return memory;
  }

  public BoundedByteArrayOutputStream getArrayStream() {
    return byteStream;
  }

  @Override
  protected void doShuffle(MapHost host, IFileInputStream iFin,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    InputStream input = iFin;

    // Are map-outputs compressed?
    if (codec != null) {
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }
  
    try {
      IOUtils.readFully(input, memory, 0, memory.length);
      metrics.inputBytes(memory.length);
      reporter.progress();
      LOG.info("Read " + memory.length + " bytes from map-output for " +
                getMapId());

      /**
       * We've gotten the amount of data we were expecting. Verify the
       * decompressor has nothing more to offer. This action also forces the
       * decompressor to read any trailing bytes that weren't critical
       * for decompression, which is necessary to keep the stream
       * in sync.
       */
      if (input.read() >= 0 ) {
        throw new IOException("Unexpected extra bytes from input stream for " +
                               getMapId());
      }
    } finally {
      CodecPool.returnDecompressor(decompressor);
    }
  }

  @Override
  public void commit() throws IOException {
    getMerger().closeInMemoryFile(this);
  }
  
  @Override
  public void abort() {
    getMerger().unreserve(memory.length);
  }

  @Override
  public String getDescription() {
    return "MEMORY";
  }
}
