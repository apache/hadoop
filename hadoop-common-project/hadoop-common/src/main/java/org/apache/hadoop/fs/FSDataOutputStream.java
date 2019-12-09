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
package org.apache.hadoop.fs;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility that wraps a {@link OutputStream} in a {@link DataOutputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataOutputStream extends DataOutputStream
    implements Syncable, CanSetDropBehind, StreamCapabilities {
  private final OutputStream wrappedStream;

  private static class PositionCache extends FilterOutputStream {
    private final FileSystem.Statistics statistics;
    private long position;

    PositionCache(OutputStream out, FileSystem.Statistics stats, long pos) {
      super(out);
      statistics = stats;
      position = pos;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      position++;
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }
    
    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
      
    long getPos() {
      return position;                            // return cached position
    }

    @Override
    public void close() throws IOException {
      // ensure close works even if a null reference was passed in
      if (out != null) {
        out.close();
      }
    }
  }

  public FSDataOutputStream(OutputStream out, FileSystem.Statistics stats) {
    this(out, stats, 0);
  }

  public FSDataOutputStream(OutputStream out, FileSystem.Statistics stats,
                            long startPosition) {
    super(new PositionCache(out, stats, startPosition));
    wrappedStream = out;
  }
  
  /**
   * Get the current position in the output stream.
   *
   * @return the current position in the output stream
   */
  public long getPos() {
    return ((PositionCache)out).getPos();
  }

  /**
   * Close the underlying output stream.
   */
  @Override
  public void close() throws IOException {
    out.close(); // This invokes PositionCache.close()
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "FSDataOutputStream{");
    sb.append("wrappedStream=").append(wrappedStream);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Get a reference to the wrapped output stream.
   *
   * @return the underlying output stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public OutputStream getWrappedStream() {
    return wrappedStream;
  }

  @Override
  public boolean hasCapability(String capability) {
    if (wrappedStream instanceof StreamCapabilities) {
      return ((StreamCapabilities) wrappedStream).hasCapability(capability);
    }
    return false;
  }

  @Override  // Syncable
  public void hflush() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).hflush();
    } else {
      wrappedStream.flush();
    }
  }
  
  @Override  // Syncable
  public void hsync() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).hsync();
    } else {
      wrappedStream.flush();
    }
  }

  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException {
    try {
      ((CanSetDropBehind)wrappedStream).setDropBehind(dropBehind);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("the wrapped stream does " +
          "not support setting the drop-behind caching setting.");
    }
  }
}
