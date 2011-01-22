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

package org.apache.hadoop.io;

import java.io.*;
import java.net.Socket;

import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;

/**
 * An utility class for I/O related functionality. 
 */
public class IOUtils {

  /**
   * Copies from one stream to another.
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.  
   */
  public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close) 
    throws IOException {

    try {
      copyBytes(in, out, buffSize);
    } finally {
      if(close) {
        out.close();
        in.close();
      }
    }
  }
  
  /**
   * Copies from one stream to another.
   * 
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   */
  public static void copyBytes(InputStream in, OutputStream out, int buffSize) 
    throws IOException {

    PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
    byte buf[] = new byte[buffSize];
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      if ((ps != null) && ps.checkError()) {
        throw new IOException("Unable to write to output stream.");
      }
      bytesRead = in.read(buf);
    }
  }

  /**
   * Copies from one stream to another. <strong>closes the input and output streams 
   * at the end</strong>.
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object 
   */
  public static void copyBytes(InputStream in, OutputStream out, Configuration conf)
    throws IOException {
    copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096), true);
  }
  
  /**
   * Copies from one stream to another.
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.
   */
  public static void copyBytes(InputStream in, OutputStream out, Configuration conf, boolean close)
    throws IOException {
    copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096),  close);
  }
  
  /** Reads len bytes in a loop.
   * @param in The InputStream to read from
   * @param buf The buffer to fill
   * @param off offset from the buffer
   * @param len the length of bytes to read
   * @throws IOException if it could not read requested number of bytes 
   * for any reason (including EOF)
   */
  public static void readFully( InputStream in, byte buf[],
      int off, int len ) throws IOException {
    int toRead = len;
    while ( toRead > 0 ) {
      int ret = in.read( buf, off, toRead );
      if ( ret < 0 ) {
        throw new IOException( "Premeture EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }
  
  /** Similar to readFully(). Skips bytes in a loop.
   * @param in The InputStream to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes 
   * for any reason (including EOF)
   */
  public static void skipFully( InputStream in, long len ) throws IOException {
    while ( len > 0 ) {
      long ret = in.skip( len );
      if ( ret < 0 ) {
        throw new IOException( "Premeture EOF from inputStream");
      }
      len -= ret;
    }
  }
  
  /**
   * Close the Closeable objects and <b>ignore</b> any {@link IOException} or 
   * null pointers. Must only be used for cleanup in exception handlers.
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanup(Log log, java.io.Closeable... closeables) {
    for(java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(IOException e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }

  /**
   * Closes the stream ignoring {@link IOException}.
   * Must only be called in cleaning up from exception handlers.
   * @param stream the Stream to close
   */
  public static void closeStream( java.io.Closeable stream ) {
    cleanup(null, stream);
  }
  
  /**
   * Closes the socket ignoring {@link IOException} 
   * @param sock the Socket to close
   */
  public static void closeSocket( Socket sock ) {
    // avoids try { close() } dance
    if ( sock != null ) {
      try {
       sock.close();
      } catch ( IOException ignored ) {
      }
    }
  }
  
  /** /dev/null of OutputStreams.
   */
  public static class NullOutputStream extends OutputStream {
    public void write(byte[] b, int off, int len) throws IOException {
    }

    public void write(int b) throws IOException {
    }
  }  
}
