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

import sun.nio.ch.FileChannelImpl;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;


public
class NewFileInputStream extends InputStream{


  /* File Descriptor - handle to the open file */
  private final FileDescriptor fd;

  /**
   * The path of the referenced file
   * (null if the stream is created with a file descriptor)
   */
  private final String path;

  private InputStream inputStream;
  private FileChannel channel = null;




  /**
   * Creates a <code>FileInputStream</code> by
   * opening a connection to an actual file,
   * the file named by the <code>File</code>
   * object <code>file</code> in the file system.
   * A new <code>FileDescriptor</code> object
   * is created to represent this file connection.
   * <p>
   * First, if there is a security manager,
   * its <code>checkRead</code> method  is called
   * with the path represented by the <code>file</code>
   * argument as its argument.
   * <p>
   * If the named file does not exist, is a directory rather than a regular
   * file, or for some other reason cannot be opened for reading then a
   * <code>FileNotFoundException</code> is thrown.
   *
   * @param      file   the file to be opened for reading.
   * @exception FileNotFoundException  if the file does not exist,
   *                   is a directory rather than a regular file,
   *                   or for some other reason cannot be opened for
   *                   reading.
   * @exception  SecurityException      if a security manager exists and its
   *               <code>checkRead</code> method denies read access to the file.
   * @see        java.io.File#getPath()
   * @see        java.lang.SecurityManager#checkRead(java.lang.String)
   */
  public NewFileInputStream(File file) throws IOException{
    String name = (file != null ? file.getPath() : null);

    inputStream = Files.newInputStream(file.toPath());
    fd = new FileDescriptor();
    path = name;
  }

  /**
   * Returns the unique {@link java.nio.channels.FileChannel FileChannel}
   * object associated with this file input stream.
   *
   * <p> The initial {@link java.nio.channels.FileChannel#position()
   * position} of the returned channel will be equal to the
   * number of bytes read from the file so far.  Reading bytes from this
   * stream will increment the channel's position.  Changing the channel's
   * position, either explicitly or by reading, will change this stream's
   * file position.
   *
   * @return  the file channel associated with this file input stream
   *
   * @since 1.4
   * @spec JSR-51
   */
  public FileChannel getChannel() {
    synchronized (this) {
      if (channel == null) {
        channel = FileChannelImpl.open(fd, path, true, false, this);
      }
      return channel;
    }
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }
}
