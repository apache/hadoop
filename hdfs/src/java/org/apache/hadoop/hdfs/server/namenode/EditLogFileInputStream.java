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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.io.IOUtils;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a local file.
 */
class EditLogFileInputStream extends EditLogInputStream {
  private File file;
  private FileInputStream fStream;

  EditLogFileInputStream(File name) throws IOException {
    file = name;
    fStream = new FileInputStream(name);
  }

  @Override // JournalStream
  public String getName() {
    return file.getPath();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.FILE;
  }

  @Override
  public int available() throws IOException {
    return fStream.available();
  }

  @Override
  public int read() throws IOException {
    return fStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return fStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    fStream.close();
  }

  @Override
  long length() throws IOException {
    // file size + size of both buffers
    return file.length();
  }
  
  /**
   * Return the length of non-zero bytes in the given file.
   * @param chunkSizeToRead chunk size for disk reads
   * @throws IOException if the file cannot be read
   */
  static long getValidLength(File f) throws IOException {
    return getValidLength(f, 1024*1024); // 1M chunks
  }

  /**
   * Return the length of bytes in the given file after subtracting
   * the trailer of 0xFF (OP_INVALID)s.
   * This seeks to the end of the file and reads chunks backwards until
   * it finds a non-0xFF byte.
   * @param chunkSizeToRead chunk size for disk reads
   * @throws IOException if the file cannot be read
   */
  static long getValidLength(File f, int chunkSizeToRead) throws IOException {    
    FileInputStream fis = new FileInputStream(f);
    try {
      
      byte buf[] = new byte[chunkSizeToRead];

      FileChannel fc = fis.getChannel();
      long size = fc.size();
      long pos = size - (size % chunkSizeToRead);
      
      while (pos >= 0) {
        fc.position(pos);

        int readLen = (int) Math.min(size - pos, chunkSizeToRead);
        IOUtils.readFully(fis, buf, 0, readLen);
        for (int i = readLen - 1; i >= 0; i--) {
          if (buf[i] != FSEditLogOpCodes.OP_INVALID.getOpCode()) {
            return pos + i + 1; // + 1 since we count this byte!
          }
        }
        
        pos -= chunkSizeToRead;
      }
      return 0;
    } finally {
      fis.close();
    }
  }
}
