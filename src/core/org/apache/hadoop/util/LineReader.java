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

package org.apache.hadoop.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * A class that provides a line reader from an input stream.
 */
public class LineReader {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  // the number of bytes of real data in the buffer
  private int bufferLength = 0;
  // the current position in the buffer
  private int bufferPosn = 0;

  /**
   * Create a line reader that reads from the given stream using the
   * default buffer-size (64k).
   * @param in The input stream
   * @throws IOException
   */
  public LineReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a line reader that reads from the given stream using the 
   * given buffer-size.
   * @param in The input stream
   * @param bufferSize Size of the read buffer
   * @throws IOException
   */
  public LineReader(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  /**
   * Create a line reader that reads from the given stream using the
   * <code>io.file.buffer.size</code> specified in the given
   * <code>Configuration</code>.
   * @param in input stream
   * @param conf configuration
   * @throws IOException
   */
  public LineReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  /**
   * Fill the buffer with more data.
   * @return was there more data?
   * @throws IOException
   */
  boolean backfill() throws IOException {
    bufferPosn = 0;
    bufferLength = in.read(buffer);
    return bufferLength > 0;
  }
  
  /**
   * Close the underlying stream.
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }
  
  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @param maxLineLength the maximum number of bytes to store into str.
   * @param maxBytesToConsume the maximum number of bytes to consume in this call.
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength,
                      int maxBytesToConsume) throws IOException {
    str.clear();
    boolean hadFinalNewline = false;
    boolean hadFinalReturn = false;
    boolean hitEndOfFile = false;
    int startPosn = bufferPosn;
    long bytesConsumed = 0;
    outerLoop: while (true) {
      if (bufferPosn >= bufferLength) {
        if (!backfill()) {
          hitEndOfFile = true;
          break;
        }
      }
      startPosn = bufferPosn;
      for(; bufferPosn < bufferLength; ++bufferPosn) {
        switch (buffer[bufferPosn]) {
        case '\n':
          hadFinalNewline = true;
          bufferPosn += 1;
          break outerLoop;
        case '\r':
          if (hadFinalReturn) {
            // leave this \r in the stream, so we'll get it next time
            break outerLoop;
          }
          hadFinalReturn = true;
          break;
        default:
          if (hadFinalReturn) {
            break outerLoop;
          }
        }        
      }
      bytesConsumed += bufferPosn - startPosn;
      int length = bufferPosn - startPosn - (hadFinalReturn ? 1 : 0);
      length = (int)Math.min(length, maxLineLength - str.getLength());
      if (length >= 0) {
        str.append(buffer, startPosn, length);
      }
      if (bytesConsumed >= maxBytesToConsume) {
        return (int)Math.min(bytesConsumed, (long)Integer.MAX_VALUE);
      }
    }
    int newlineLength = (hadFinalNewline ? 1 : 0) + (hadFinalReturn ? 1 : 0);
    if (!hitEndOfFile) {
      bytesConsumed += bufferPosn - startPosn;
      int length = bufferPosn - startPosn - newlineLength;
      length = (int)Math.min(length, maxLineLength - str.getLength());
      if (length > 0) {
        str.append(buffer, startPosn, length);
      }
    }
    return (int)Math.min(bytesConsumed, (long)Integer.MAX_VALUE);
  }

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @param maxLineLength the maximum number of bytes to store into str.
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str, int maxLineLength) throws IOException {
    return readLine(str, maxLineLength, Integer.MAX_VALUE);
}

  /**
   * Read from the InputStream into the given Text.
   * @param str the object to store the given line
   * @return the number of bytes read including the newline
   * @throws IOException if the underlying stream throws
   */
  public int readLine(Text str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

}
