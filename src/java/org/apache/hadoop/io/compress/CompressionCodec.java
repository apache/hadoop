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
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class encapsulates a streaming compression/decompression pair.
 * @author Owen O'Malley
 */
public interface CompressionCodec {

  /**
   * Create a stream compressor that will write to the given output stream.
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to
   */
  CompressionOutputStream createOutputStream(OutputStream out) 
  throws IOException;
  
  /**
   * Create a stream decompressor that will read from the given input stream.
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   */
  CompressionInputStream createInputStream(InputStream in) throws IOException;
  
  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  String getDefaultExtension();
}
