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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A simple wrapper class to make any input stream "rewindable". It could be
 * made more memory efficient by grow the internal buffer adaptively.
 */
public class RewindableInputStream extends InputStream {
  private InputStream input;

  /**
   * Constructor.
   * 
   * @param input
   */
  public RewindableInputStream(InputStream input) {
    this(input, 1024 * 1024);
  }

  /**
   * Constructor
   * 
   * @param input
   *          input stream.
   * @param maxBytesToRemember
   *          Maximum number of bytes we need to remember at the beginning of
   *          the stream. If {@link #rewind()} is called after so many bytes are
   *          read from the stream, {@link #rewind()} would fail.
   */
  public RewindableInputStream(InputStream input, int maxBytesToRemember) {
    this.input = new BufferedInputStream(input, maxBytesToRemember);
    this.input.mark(maxBytesToRemember);
  }

  @Override
  public int read() throws IOException {
    return input.read();
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    return input.read(buffer, offset, length);
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  public InputStream rewind() throws IOException {
    try {
      input.reset();
      return this;
    } catch (IOException e) {
      throw new IOException("Unable to rewind the stream", e);
    }
  }
}
