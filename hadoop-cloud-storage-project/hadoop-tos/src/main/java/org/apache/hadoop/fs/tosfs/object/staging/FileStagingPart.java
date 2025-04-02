/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object.staging;

import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStagingPart implements StagingPart {
  private static final Logger LOG = LoggerFactory.getLogger(FileStagingPart.class);

  private final Path path;
  private final int stagingBufferSize;
  private final StagingFileOutputStream out;
  private State state = State.WRITABLE;

  public FileStagingPart(String filePath, int stagingBufferSize) {
    this.path = Paths.get(filePath);
    this.stagingBufferSize = stagingBufferSize;
    this.out = new StagingFileOutputStream(path, stagingBufferSize);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(state == State.WRITABLE,
        "Cannot write the part since it's not writable now, state: %s", state);
    out.write(b, off, len);
  }

  @Override
  public synchronized void complete() throws IOException {
    Preconditions.checkState(state == State.WRITABLE,
        "Cannot complete the part since it's not writable now, state: %s", state);
    out.close();
    state = State.READABLE;
  }

  @Override
  public synchronized InputStream newIn() {
    Preconditions.checkState(state == State.READABLE,
        "Cannot read the part since it's not readable now, state: %s.", state);
    return out.newIn();
  }

  @Override
  public synchronized long size() {
    return out.size();
  }

  @Override
  public synchronized State state() {
    return state;
  }

  @Override
  public synchronized void cleanup() {
    if (state != State.CLEANED) {
      try {
        // Close the stream quietly.
        CommonUtils.runQuietly(out::close, false);

        // Delete the staging file if exists.
        Files.deleteIfExists(path);
      } catch (Exception e) {
        LOG.error("Failed to delete staging file, stagingFile: {}", path, e);
      } finally {
        state = State.CLEANED;
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("stagingBufferSize", stagingBufferSize)
        .add("wroteByteSize", size())
        .toString();
  }

  private final static class StagingFileOutputStream extends OutputStream {
    private final Path path;
    private byte[] buffer;
    private boolean memBuffered;
    private int writePos;
    private OutputStream out;

    private StagingFileOutputStream(Path path, int stagingBufferSize) {
      this.path = path;
      this.buffer = new byte[stagingBufferSize];
      this.memBuffered = true;
      this.writePos = 0;
    }

    private int size() {
      return writePos;
    }

    public InputStream newIn() {
      // Just wrap it as a byte array input stream if the staging bytes are still in the in-memory
      // buffer.
      if (memBuffered) {
        return new ByteArrayInputStream(buffer, 0, writePos);
      }

      // Create a buffered file input stream.
      try {
        return new BufferedInputStream(Files.newInputStream(path));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void write(int b) throws IOException {
      write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (memBuffered && writePos + len > buffer.length) {
        flushMemToFile();
      }

      if (memBuffered) {
        System.arraycopy(b, off, buffer, writePos, len);
      } else {
        out.write(b, off, len);
      }

      writePos += len;
    }

    @Override
    public void close() throws IOException {
      if (out != null) {
        out.close();
        out = null;
      }
    }

    private void flushMemToFile() throws IOException {
      // Flush the buffered data to the new file OutputStream.
      out = new BufferedOutputStream(Files.newOutputStream(path));
      out.write(buffer, 0, writePos);
      memBuffered = false;
      buffer = null;
    }
  }
}
