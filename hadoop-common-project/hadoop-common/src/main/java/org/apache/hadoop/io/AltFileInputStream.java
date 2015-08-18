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

import org.apache.hadoop.util.Shell;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

/**
 * This class is substitute FileInputStream. When on windows, We are still use
 * FileInputStream. For non-windows, we use channel and FileDescriptor to
 * construct a stream.
 */
public class AltFileInputStream extends InputStream {
  // For non-Windows
  private final InputStream inputStream;
  private final FileDescriptor fd;
  private final FileChannel fileChannel;

  // For Windows
  private FileInputStream fileInputStream;

  public AltFileInputStream(File file) throws IOException {
    if (!Shell.WINDOWS) {
      RandomAccessFile rf = new RandomAccessFile(file, "r");
      this.fd = rf.getFD();
      this.fileChannel = rf.getChannel();
      this.inputStream = Channels.newInputStream(fileChannel);
    } else {
      FileInputStream fis = new FileInputStream(file);
      this.fileInputStream = fis;
      this.inputStream = fileInputStream;
      this.fd = fis.getFD();
      this.fileChannel = fis.getChannel();
    }
  }

  /**
   * Create a stream with fd and channel
   * @param fd FileDescriptor
   * @param fileChannel FileChannel
   */
  public AltFileInputStream(FileDescriptor fd, FileChannel fileChannel) {
    this.fd = fd;
    this.fileChannel = fileChannel;
    this.inputStream = Channels.newInputStream(fileChannel);
  }

  /**
   * Create a stream with FileInputSteam
   * @param fis FileInputStream
   * @throws IOException
   */
  public AltFileInputStream(FileInputStream fis) throws IOException {
    this.fileInputStream = fis;
    this.inputStream = fileInputStream;
    this.fd = fis.getFD();
    this.fileChannel = fis.getChannel();
  }

  /**
   * Returns the <code>FileDescriptor</code>
   * object  that represents the connection to
   * the actual file in the file system being
   * used by this <code>FileInputStream</code>.
   *
   * @return     the file descriptor object associated with this stream.
   * @exception  IOException  if an I/O error occurs.
   * @see        java.io.FileDescriptor
   */
  public final FileDescriptor getFD() throws IOException {
    if (fd != null) {
      return fd;
    }
    throw new IOException();
  }

  // return a channel
  public FileChannel getChannel() {
    return fileChannel;
  }

  /**
   * For Windows, use fileInputStream to read data.
   * For Non-Windows, use inputStream to read data.
   * @return
   * @throws IOException
   */
  public int read() throws IOException {
    if (fileInputStream != null) {
      return fileInputStream.read();
    } else {
      return inputStream.read();
    }
  }

  /**
   * Reads up to <code>len</code> bytes of data from this input stream
   * into an array of bytes. If <code>len</code> is not zero, the method
   * blocks until some input is available; otherwise, no
   * bytes are read and <code>0</code> is returned.
   *
   * @param      b     the buffer into which the data is read.
   * @param      off   the start offset in the destination array <code>b</code>
   * @param      len   the maximum number of bytes read.
   * @return     the total number of bytes read into the buffer, or
   *             <code>-1</code> if there is no more data because the end of
   *             the file has been reached.
   * @exception  NullPointerException If <code>b</code> is <code>null</code>.
   * @exception  IndexOutOfBoundsException If <code>off</code> is negative,
   * <code>len</code> is negative, or <code>len</code> is greater than
   * <code>b.length - off</code>
   * @exception  IOException  if an I/O error occurs.
   */
  public int read(byte[] b, int off, int len) throws IOException {
    if (fileInputStream != null) {
      return fileInputStream.read(b, off, len);
    } else {
      return inputStream.read(b, off, len);
    }
  }

  /**
   * Reads up to <code>b.length</code> bytes of data from this input
   * stream into an array of bytes. This method blocks until some input
   * is available.
   *
   * @param      b   the buffer into which the data is read.
   * @return     the total number of bytes read into the buffer, or
   *             <code>-1</code> if there is no more data because the end of
   *             the file has been reached.
   * @exception  IOException  if an I/O error occurs.
   */
  public int read(byte[] b) throws IOException {
    if (fileInputStream != null) {
      return fileInputStream.read(b);
    } else {
      return inputStream.read(b);
    }
  }

  /**
   * Closes this file input stream and releases any system resources
   * associated with the stream.
   *
   * <p> If this stream has an associated channel then the channel is closed
   * as well.
   *
   * @exception  IOException  if an I/O error occurs.
   */
  public void close() throws IOException {
    if (fileInputStream != null) {
      fileInputStream.close();
    }
    fileChannel.close();
    inputStream.close();
  }
}