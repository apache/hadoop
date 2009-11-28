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

package org.apache.hadoop.sqoop.io;

import java.io.OutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An output stream that counts how many bytes its written.
 */
public class CountingOutputStream extends OutputStream {

  public static final Log LOG = LogFactory.getLog(CountingOutputStream.class.getName());

  private final OutputStream stream;
  private long bytesWritten;

  public CountingOutputStream(final OutputStream outputStream) {
    this.stream = outputStream;
    this.bytesWritten = 0;
  }

  /** @return the number of bytes written thus far to the stream. */
  public long getBytesWritten() {
    return bytesWritten;
  }

  /** Reset the counter of bytes written to zero. */
  public void resetCount() {
    this.bytesWritten = 0;
  }

  public void close() throws IOException {
    this.stream.close();
  }

  public void flush() throws IOException {
    this.stream.flush();
  }

  public void write(byte [] b) throws IOException {
    this.stream.write(b);
    bytesWritten += b.length;
  }

  public void write(byte [] b, int off, int len) throws IOException {
    this.stream.write(b, off, len);
    bytesWritten += len;
  }

  public void write(int b) throws IOException {
    this.stream.write(b);
    bytesWritten++;
  }
}
