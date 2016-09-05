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

package org.apache.hadoop.fs.s3a;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.fs.s3a.S3AOutput.StreamDestination.DestState.*;

/**
 * Set of classes to support output streaming; kept all together for easier
 * management.
 */
class S3AOutput {

  static abstract class StreamDestinationFactory {

    protected S3AFileSystem owner;

    /**
     * Bind to the factory owner.
     * @param owner owner factory
     */
    void init(S3AFileSystem owner) {
      this.owner = owner;
    }

    /**
     * Create a destination.
     * @param limit limit of the destination.
     * @return a new destination.
     */
    abstract StreamDestination create(int limit) throws IOException;

  }

  static abstract class StreamDestination implements Closeable {

    enum DestState {Writing, Upload, Closed}

    private volatile DestState state = Writing;

    protected synchronized void enterState(DestState current, DestState next) {
      verifyState(current);
      state = next;
    }

    protected void verifyState(DestState current) {
      Preconditions.checkState(state == current,
          "Expected stream state " + current + " -but actual state is " + state
              + " in " + this);
    }

    DestState getState() {
      return state;
    }

    /**
     * Return the current destination size.
     * @return
     */
    abstract int currentSize();

    abstract boolean hasCapacity(long bytes);

    abstract int remainingCapacity();

    /**
     * Write a series of bytes from the buffer, from the offset.
     * Returns the number of bytes written:
     * @param b
     * @param off
     * @param len
     * @return
     * @throws IOException
     */
    int write(byte b[], int off, int len) throws IOException {
      verifyState(Writing);
      return 0;
    }

    void flush() throws IOException {

    }

    /**
     * Switch to the upload state and return a stream for uploading.
     * @return the stream
     * @throws IOException
     */
    InputStream openForUpload() throws IOException {
      enterState(Writing, Upload);
      return null;
    }

    /**
     * Enter the closed state.
     * @return true if the class was in any other state, implying that
     * the subclass should do its close operations
     * @throws IOException
     */
    protected synchronized boolean enterClosedState() {
      if (!state.equals(Closed)) {
        state = Closed;
        return true;
      } else {
        return false;
      }
    }
  }

  class UploadStream extends InputStream {

    private final StreamDestination owner;
    private final InputStream inner;

    public UploadStream(StreamDestination owner, InputStream inner) {
      this.owner = owner;
      this.inner = inner;
    }

    @Override
    public int read() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeStream(inner);
      IOUtils.closeStream(owner);
      super.close();
    }
  }

  /**
   * Stream to memory via a {@code ByteArrayOutputStream}.
   *
   * This was taken from {@link S3AFastOutputStream} and has the
   * same problem which surfaced there: it consumes heap space
   * proportional to the mismatch between writes to the stream and
   * the JVM-wide upload bandwidth to the S3 endpoint.
   */

  static class StreamToMemory extends StreamDestination {
    private ByteArrayOutputStream buffer;
    private int limit;

    public StreamToMemory(int limit) {
      this.limit = limit;
      buffer = new ByteArrayOutputStream();
    }

    @Override
    int currentSize() {
      return buffer.size();
    }

    @Override
    InputStream openForUpload() throws IOException {
      super.openForUpload();
      ByteArrayInputStream bufferData = new ByteArrayInputStream(
          buffer.toByteArray());
      buffer.reset();
      buffer = null;
      return bufferData;
    }

    @Override
    public boolean hasCapacity(long bytes) {
      return currentSize() + bytes <= limit;
    }

    @Override
    public int remainingCapacity() {
      return limit - currentSize();
    }

    @Override
    int write(byte[] b, int off, int len) throws IOException {
      super.write(b, off, len);
      int written = Math.min(remainingCapacity(), len);
      buffer.write(b, off, written);
      return written;
    }

    @Override
    public void close() throws IOException {
      buffer = null;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "StreamToMemory{");
      sb.append("state=").append(getState());
      sb.append(", limit=").append(limit);
      sb.append('}');
      return sb.toString();
    }
  }

  static class StreamToMemoryFactory extends StreamDestinationFactory {

    @Override
    void init(S3AFileSystem owner) {
      super.init(owner);
    }

    @Override
    StreamDestination create(int limit) throws IOException {
      return new StreamToMemory(limit);
    }
  }

  static class StreamToDiskFactory extends StreamDestinationFactory {

    @Override
    void init(S3AFileSystem owner) {
      super.init(owner);
    }

    @Override
    StreamDestination create(int limit) throws IOException {
      File destFile = owner.getDirectoryAllocator()
          .createTmpFileForWrite("s3a", limit, owner.getConf());
      return new StreamToFile(destFile, limit);
    }
  }

  /**
   * Stream to a file.
   * This will stop at the limit; the caller is expected to create a new file
   */
  static class StreamToFile extends StreamDestination {
    private File destFile;

    private int limit;
    protected int bytesWritten;

    private BufferedOutputStream out;
    private FileInputStream uploadStream;

    public StreamToFile(File destFile, int limit)
        throws FileNotFoundException {
      this.limit = limit;
      this.destFile = destFile;
      out = new BufferedOutputStream(
          new FileOutputStream(destFile));
    }

    @Override
    int currentSize() {
      return bytesWritten;
    }

    @Override
    boolean hasCapacity(long bytes) {
      return currentSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - bytesWritten;
    }

    @Override
    int write(byte[] b, int off, int len) throws IOException {
      super.write(b, off, len);
      int written = Math.min(remainingCapacity(), len);
      out.write(b, off, written);
      bytesWritten += written;
      return written;
    }

    @Override
    InputStream openForUpload() throws IOException {
      super.openForUpload();
      try {
        out.flush();
      } finally {
        out.close();
        out = null;
      }
      uploadStream = new FileInputStream(destFile);
      return uploadStream;
    }

    @Override
    public synchronized void close() throws IOException {
      enterClosedState();
      IOUtils.closeStream(out);
      IOUtils.closeStream(uploadStream);
      out = null;
      uploadStream = null;
      destFile.delete();
    }

    @Override
    void flush() throws IOException {
      verifyState(Writing);
      out.flush();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "StreamToFile{");
      sb.append("destFile=").append(destFile);
      sb.append(", state=").append(getState());
      sb.append(", limit=").append(limit);
      sb.append(", bytesWritten=").append(bytesWritten);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Validate args to write command.
   * @param b byte array containing data
   * @param off offset in array where to start
   * @param len number of bytes to be written
   * @throws NullPointerException for a null buffer
   * @throws IndexOutOfBoundsException if indices are out of range
   */
  public static void validateWriteArgs(byte[] b, int off, int len)
      throws IOException {
    Preconditions.checkNotNull(b);
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
  }
}
