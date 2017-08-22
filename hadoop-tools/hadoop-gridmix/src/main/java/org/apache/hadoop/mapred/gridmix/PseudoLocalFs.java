/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.gridmix;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.Progressable;

/**
 * Pseudo local file system that generates random data for any file on the fly
 * instead of storing files on disk. So opening same file multiple times will
 * not give same file content. There are no directories in this file system
 * other than the root and all the files are under root i.e. "/". All file URIs
 * on pseudo local file system should be of the format <code>
 * pseudo:///&lt;name&gt;.&lt;fileSize&gt;</code> where name is a unique name
 * and &lt;fileSize&gt; is a number representing the size of the file in bytes.
 */
class PseudoLocalFs extends FileSystem {
  Path home;
  /**
   * The creation time and modification time of all files in
   * {@link PseudoLocalFs} is same.
   */
  private static final long TIME = System.currentTimeMillis();
  private static final String HOME_DIR = "/";
  private static final long BLOCK_SIZE  = 4 * 1024 * 1024L; // 4 MB
  private static final int DEFAULT_BUFFER_SIZE = 1024  * 1024; // 1MB

  static final URI NAME = URI.create("pseudo:///");

  PseudoLocalFs() {
    this(new Path(HOME_DIR));
  }

  PseudoLocalFs(Path home) {
    super();
    this.home = home;
  }

  @Override
  public URI getUri() {
    return NAME;
  }

  @Override
  public Path getHomeDirectory() {
    return home;
  }

  @Override
  public Path getWorkingDirectory() {
    return getHomeDirectory();
  }

  /**
   * Generates a valid pseudo local file path from the given <code>fileId</code>
   * and <code>fileSize</code>.
   * @param fileId unique file id string
   * @param fileSize file size
   * @return the generated relative path
   */
  static Path generateFilePath(String fileId, long fileSize) {
    return new Path(fileId + "." + fileSize);
  }

  /**
   * Creating a pseudo local file is nothing but validating the file path.
   * Actual data of the file is generated on the fly when client tries to open
   * the file for reading.
   * @param path file path to be created
   */
  @Override
  public FSDataOutputStream create(Path path) throws IOException {
    try {
      validateFileNameFormat(path);
    } catch (FileNotFoundException e) {
      throw new IOException("File creation failed for " + path);
    }
    return null;
  }

  /**
   * Validate if the path provided is of expected format of Pseudo Local File
   * System based files.
   * @param path file path
   * @return the file size
   * @throws FileNotFoundException
   */
  long validateFileNameFormat(Path path) throws FileNotFoundException {
    path = this.makeQualified(path);
    boolean valid = true;
    long fileSize = 0;
    if (!path.toUri().getScheme().equals(getUri().getScheme())) {
      valid = false;
    } else {
      String[] parts = path.toUri().getPath().split("\\.");
      try {
        fileSize = Long.parseLong(parts[parts.length - 1]);
        valid = (fileSize >= 0);
      } catch (NumberFormatException e) {
        valid = false;
      }
    }
    if (!valid) {
      throw new FileNotFoundException("File " + path
          + " does not exist in pseudo local file system");
    }
    return fileSize;
  }

  /**
   * @See create(Path) for details
   */
  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    long fileSize = validateFileNameFormat(path);
    InputStream in = new RandomInputStream(fileSize, bufferSize);
    return new FSDataInputStream(in);
  }

  /**
   * @See create(Path) for details
   */
  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return open(path, DEFAULT_BUFFER_SIZE);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    long fileSize = validateFileNameFormat(path);
    return new FileStatus(fileSize, false, 1, BLOCK_SIZE, TIME, path);
  }

  @Override
  public boolean exists(Path path) {
    try{
      validateFileNameFormat(path);
    } catch (FileNotFoundException e) {
      return false;
    }
    return true;
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return create(path);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException,
      IOException {
    return new FileStatus[] {getFileStatus(path)};
  }

  /**
   * Input Stream that generates specified number of random bytes.
   */
  static class RandomInputStream extends InputStream
      implements Seekable, PositionedReadable {

    private final Random r = new Random();
    private BytesWritable val = null;
    private int positionInVal = 0;// current position in the buffer 'val'

    private long totalSize = 0;// total number of random bytes to be generated
    private long curPos = 0;// current position in this stream

    /**
     * @param size total number of random bytes to be generated in this stream
     * @param bufferSize the buffer size. An internal buffer array of length
     * <code>bufferSize</code> is created. If <code>bufferSize</code> is not a
     * positive number, then a default value of 1MB is used.
     */
    RandomInputStream(long size, int bufferSize) {
      totalSize = size;
      if (bufferSize <= 0) {
        bufferSize = DEFAULT_BUFFER_SIZE;
      }
      val = new BytesWritable(new byte[bufferSize]);
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      if (curPos < totalSize) {
        if (positionInVal < val.getLength()) {// use buffered byte
          b[0] = val.getBytes()[positionInVal++];
          ++curPos;
        } else {// generate data
          int num = read(b);
          if (num < 0) {
            return num;
          }
        }
      } else {
        return -1;
      }
      return b[0];
    }

    @Override
    public int read(byte[] bytes) throws IOException {
      return read(bytes, 0, bytes.length);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
      if (curPos == totalSize) {
        return -1;// EOF
      }
      int numBytes = len;
      if (numBytes > (totalSize - curPos)) {// position in file is close to EOF
        numBytes = (int)(totalSize - curPos);
      }
      if (numBytes > (val.getLength() - positionInVal)) {
        // need to generate data into val
        r.nextBytes(val.getBytes());
        positionInVal = 0;
      }

      System.arraycopy(val.getBytes(), positionInVal, bytes, off, numBytes);
      curPos += numBytes;
      positionInVal += numBytes;
      return numBytes;
    }

    @Override
    public int available() {
      return (int)(val.getLength() - positionInVal);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Get the current position in this stream/pseudo-file
     * @return the position in this stream/pseudo-file
     * @throws IOException
     */
    @Override
    public long getPos() throws IOException {
      return curPos;
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Append is not supported"
        + " in pseudo local file system.");
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException("Mkdirs is not supported"
        + " in pseudo local file system.");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException("Rename is not supported"
        + " in pseudo local file system.");
  }

  @Override
  public boolean delete(Path path, boolean recursive) {
    throw new UnsupportedOperationException("File deletion is not supported "
        + "in pseudo local file system.");
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    throw new UnsupportedOperationException("SetWorkingDirectory "
        + "is not supported in pseudo local file system.");
  }

  @Override
  public Path makeQualified(Path path) {
    // skip FileSystem#checkPath() to validate some other Filesystems
    return path.makeQualified(this.getUri(), this.getWorkingDirectory());
  }
}
