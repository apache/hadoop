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

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

/**
 * This class is substitute for FileInputStream.Cause FileInputStream cause GC
 * pause for a long time.
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

  public AltFileInputStream(FileDescriptor fd, FileChannel fileChannel) {
    this.fd = fd;
    this.fileChannel = fileChannel;
    this.inputStream = Channels.newInputStream(fileChannel);
  }

  public AltFileInputStream(FileInputStream fis) throws IOException {
    this.fileInputStream = fis;
    this.inputStream = fileInputStream;
    this.fd = fis.getFD();
    this.fileChannel = fis.getChannel();
  }

  public final FileDescriptor getFD() throws IOException {
    if (fd != null) {
      return fd;
    }
    throw new IOException();
  }

  public FileChannel getChannel() {
    return fileChannel;
  }

  public int read() throws IOException {
    if (Shell.WINDOWS) {
      return fileInputStream.read();
    }else {
      return inputStream.read();
    }
  }

  public int read(byte[] b, int off, int len) throws IOException {
    if (Shell.WINDOWS) {
      return fileInputStream.read(b, off, len);
    } else {
      return inputStream.read(b, off, len);
    }
  }

  public int read(byte[] b) throws IOException {
    if (Shell.WINDOWS) {
      return fileInputStream.read(b);
    } else {
      return inputStream.read(b);
    }
  }

  public void close() throws IOException {
    if (Shell.WINDOWS) {
      fileInputStream.close();
    }
    fileChannel.close();
    inputStream.close();
  }
}