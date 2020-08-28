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

package com.hadoop.compression.lzo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates lzo compressors/decompressors that are bridged
 * to `org.apache.hadoop.io.compress.LzoCodec` from `com.hadoop.compression.lzo.LzoCodec`
 */
public class LzoCodec extends org.apache.hadoop.io.compress.LzoCodec {
  private static final Logger LOG = LoggerFactory.getLogger(LzoCodec.class.getName());
  private static final String gplLzoCodec = LzoCodec.class.getName();
  private static final String hadoopLzoCodec = org.apache.hadoop.io.compress.LzoCodec.class.getName();
  private static AtomicBoolean warned = new AtomicBoolean(false);

  static {
    LOG.info("Bridging " + gplLzoCodec + " to " + hadoopLzoCodec + ".");
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    if (warned.compareAndSet(false, true)) {
      LOG.warn("{} is deprecated. You should use {} instead to generate LZO compressed data.",
        gplLzoCodec, hadoopLzoCodec);
    }
    return super.createOutputStream(out, compressor);
  }
}
