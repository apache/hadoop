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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;


public class AltFileInputStream extends InputStream{
  public static final Log LOG = LogFactory.getLog(AltFileInputStream.class);
  private final FileDescriptor fd;

  private Closeable parent;
  private List<Closeable> otherParents;

  private final String path;

  private BufferedInputStream bufferedInputStream;
  private InputStream inputStream;
  private FileChannel channel = null;


  public AltFileInputStream(File file) throws IOException{
    LOG.info("++++++++++++++++++++++++++++++++++++++++++AltFileInputStream(File file)++++++++++++++++++++++++++++++++++++++++++++++++++");
    String name = (file != null ? file.getPath() : null);

    inputStream = Files.newInputStream(file.toPath());
    fd = new FileDescriptor();
    path = name;
    open(name);
  }

  private void open(String name) throws FileNotFoundException {
    open0(name);
  }

  private native void open0(String name) throws FileNotFoundException;

  public FileChannel getChannel() {
    LOG.info("++++++++++++++++++++++++++++++++++++++++++FileChannel getChannel()++++++++++++++++++++++++++++++++++++++++++++++++++");
    synchronized (this) {
      if (channel == null) {
        channel = FileChannelImpl.open(fd, path, true, false, this);
      }
      return channel;
    }
  }

  public AltFileInputStream(String name) throws FileNotFoundException,IOException {
    this(name != null ? new File(name) : null);
    LOG.info("++++++++++++++++++++++++++++++++++++++++++AltFileInputStream(String name)++++++++++++++++++++++++++++++++++++++++++++++++++");
  }

  public final FileDescriptor getFD() throws IOException {
    LOG.info("++++++++++++++++++++++++++++++++++++++++++FileDescriptor getFD()++++++++++++++++++++++++++++++++++++++++++++++++++");
    if (fd != null) {
      return fd;
    }
    throw new IOException();
  }

  @Override
  public int read() throws IOException{
    LOG.info("++++++++++++++++++++++++++read method +++++++++++++++++++++++++++++++++");
    bufferedInputStream = new BufferedInputStream(inputStream);
    return bufferedInputStream.read();
  }
}
