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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * A BlockReader is responsible for reading a single block
 * from a single datanode.
 */
public interface BlockReader extends Seekable, PositionedReadable {

  /* same interface as inputStream java.io.InputStream#read()
   * used by DFSInputStream#read()
   * This violates one rule when there is a checksum error:
   * "Read should not modify user buffer before successful read"
   * because it first reads the data to user buffer and then checks
   * the checksum.
   */
  int read(byte[] buf, int off, int len) throws IOException;

  /**
   * Skip the given number of bytes
   */
  long skip(long n) throws IOException;

  /**
   * Read a single byte, returning -1 at enf of stream.
   */
  int read() throws IOException;

  void close() throws IOException;

  /**
   * kind of like readFully(). Only reads as much as possible.
   * And allows use of protected readFully().
   */
  int readAll(byte[] buf, int offset, int len) throws IOException;

  /**
   * Take the socket used to talk to the DN.
   */
  Socket takeSocket();

  /**
   * Whether the BlockReader has reached the end of its input stream
   * and successfully sent a status code back to the datanode.
   */
  boolean hasSentStatusCode();

}
