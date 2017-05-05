/*
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

package org.apache.hadoop.fs.http;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;

abstract class AbstractHttpFileSystem extends FileSystem {
  private static final long DEFAULT_BLOCK_SIZE = 4096;
  private static final Path WORKING_DIR = new Path("/");

  private URI uri;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.uri = name;
  }

  public abstract String getScheme();

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    URLConnection conn = path.toUri().toURL().openConnection();
    InputStream in = conn.getInputStream();
    return new FSDataInputStream(new HttpDataInputStream(in));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                   boolean b, int i, short i1, long l,
                                   Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWorkingDirectory(Path path) {
  }

  @Override
  public Path getWorkingDirectory() {
    return WORKING_DIR;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission)
      throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return new FileStatus(-1, false, 1, DEFAULT_BLOCK_SIZE, 0, path);
  }

  private static class HttpDataInputStream extends FilterInputStream
      implements Seekable, PositionedReadable {

    HttpDataInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getPos() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
