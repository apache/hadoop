/*
 * Copyright (c) 1994, 2013, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
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
