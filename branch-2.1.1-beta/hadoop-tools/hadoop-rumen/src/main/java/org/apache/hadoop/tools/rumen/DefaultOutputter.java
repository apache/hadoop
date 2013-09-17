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
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Compressor;

/**
 * The default {@link Outputter} that outputs to a plain file. Compression
 * will be applied if the path has the right suffix.
 */
public class DefaultOutputter<T> implements Outputter<T> {
  JsonObjectMapperWriter<T> writer;
  Compressor compressor;
  
  @Override
  public void init(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);
    OutputStream output;
    if (codec != null) {
      compressor = CodecPool.getCompressor(codec);
      output = codec.createOutputStream(fs.create(path), compressor);
    } else {
      output = fs.create(path);
    }
    writer = new JsonObjectMapperWriter<T>(output, 
        conf.getBoolean("rumen.output.pretty.print", true));
  }

  @Override
  public void output(T object) throws IOException {
    writer.write(object);
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close();
    } finally {
      if (compressor != null) {
        CodecPool.returnCompressor(compressor);
      }
    }
  }
}
