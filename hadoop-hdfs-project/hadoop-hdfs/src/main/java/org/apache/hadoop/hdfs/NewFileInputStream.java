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

import java.io.*;
import java.nio.file.Files;


public
class NewFileInputStream extends InputStream{

  private InputStream inputStream;


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
    inputStream = Files.newInputStream(file.toPath());

  }
  @Override
  public int read() throws IOException {
    return inputStream.read();
  }
}
