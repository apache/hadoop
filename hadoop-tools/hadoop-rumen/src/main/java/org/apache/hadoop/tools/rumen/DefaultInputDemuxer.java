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
import org.apache.hadoop.fs.Path;

/**
 * {@link DefaultInputDemuxer} acts as a pass-through demuxer. It just opens
 * each file and returns back the input stream. If the input is compressed, it
 * would return a decompression stream.
 */
public class DefaultInputDemuxer implements InputDemuxer {
  String name;
  InputStream input;

  @Override
  public void bindTo(Path path, Configuration conf) throws IOException {
    if (name != null) { // re-binding before the previous one was consumed.
      close();
    }
    name = path.getName();

    input = new PossiblyDecompressedInputStream(path, conf);

    return;
  }

  @Override
  public Pair<String, InputStream> getNext() throws IOException {
    if (name != null) {
      Pair<String, InputStream> ret =
          new Pair<String, InputStream>(name, input);
      name = null;
      input = null;
      return ret;
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    try {
      if (input != null) {
        input.close();
      }
    } finally {
      name = null;
      input = null;
    }
  }
}
