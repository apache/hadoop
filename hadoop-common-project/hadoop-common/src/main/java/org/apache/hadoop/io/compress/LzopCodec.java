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

package org.apache.hadoop.io.compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class LzopCodec extends io.airlift.compress.lzo.LzopCodec
    implements Configurable, CompressionCodec {
  @Override
  public Class<? extends Compressor> getCompressorType()
    {
        return HadoopLzopCompressor.class;
    }

  @Override
  public Compressor createCompressor()
    {
        return new HadoopLzopCompressor();
    }

  /**
   * No Hadoop code seems to actually use the compressor, so just return a dummy one so the createOutputStream method
   * with a compressor can function.  This interface can be implemented if needed.
   */
  @DoNotPool
  static class HadoopLzopCompressor
      implements Compressor {
    @Override
    public void setInput(byte[] b, int off, int len) {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public boolean needsInput() {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public long getBytesRead() {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public long getBytesWritten() {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public void finish() {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public boolean finished() {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public void reset() {
    }

    @Override
    public void end() {
      throw new UnsupportedOperationException("LZOP block compressor is not supported");
    }

    @Override
    public void reinit(Configuration conf) {
    }
  }
}
