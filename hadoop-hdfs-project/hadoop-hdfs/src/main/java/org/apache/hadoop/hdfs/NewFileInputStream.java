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
import java.util.ArrayList;
import java.util.List;


public class NewFileInputStream extends InputStream{
  public static final Log LOG = LogFactory.getLog(NewFileInputStream.class);
  private final FileDescriptor fd;

  private Closeable parent;
  private List<Closeable> otherParents;

  private final String path;

  private BufferedInputStream bufferedInputStream;
  private InputStream inputStream;
  private FileChannel channel = null;


  public NewFileInputStream(File file) throws IOException{
    LOG.info("++++++++++++++++++++++++++++++++++++++++++NewFileInputStream(File file)++++++++++++++++++++++++++++++++++++++++++++++++++");
    String name = (file != null ? file.getPath() : null);

    inputStream = Files.newInputStream(file.toPath());
    fd = new FileDescriptor();
    path = name;
  }

  public FileChannel getChannel() {
    LOG.info("++++++++++++++++++++++++++++++++++++++++++FileChannel getChannel()++++++++++++++++++++++++++++++++++++++++++++++++++");
    synchronized (this) {
      if (channel == null) {
        channel = FileChannelImpl.open(fd, path, true, false, this);
      }
      return channel;
    }
  }

  public NewFileInputStream(String name) throws FileNotFoundException,IOException {
    this(name != null ? new File(name) : null);
    LOG.info("++++++++++++++++++++++++++++++++++++++++++NewFileInputStream(String name)++++++++++++++++++++++++++++++++++++++++++++++++++");
  }

  public final FileDescriptor getFD() throws IOException {
    LOG.info("++++++++++++++++++++++++++++++++++++++++++FileDescriptor getFD()++++++++++++++++++++++++++++++++++++++++++++++++++");
    if (fd != null) {
      return fd;
    }
    throw new IOException();
  }

  public NewFileInputStream(FileDescriptor fdObj) {
    LOG.info("++++++++++++++++++++++++++++++++++++++++++NewFileInputStream(FileDescriptor fdObj)++++++++++++++++++++++++++++++++++++++++++++++++++");
    SecurityManager security = System.getSecurityManager();
    if (fdObj == null) {
      throw new NullPointerException();
    }
    if (security != null) {
      security.checkRead(fdObj);
    }
    fd = fdObj;
    path = null;

    attach(this);
  }

  synchronized void attach(Closeable c) {
    LOG.info("++++++++++++++++++++++++++++++++++++++++++attach++++++++++++++++++++++++++++++++++++++++++++++++++");
    if (parent == null) {
      parent = c;
    } else if (otherParents == null) {
      otherParents = new ArrayList<>();
      otherParents.add(parent);
      otherParents.add(c);
    } else {
      otherParents.add(c);
    }
  }

  @Override
  public int read() throws IOException{
    LOG.info("++++++++++++++++++++++++++read method +++++++++++++++++++++++++++++++++");
    bufferedInputStream = new BufferedInputStream(inputStream);
    return bufferedInputStream.read();
  }
}
