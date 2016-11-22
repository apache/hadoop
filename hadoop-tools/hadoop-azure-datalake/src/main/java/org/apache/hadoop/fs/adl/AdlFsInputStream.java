/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.io.IOException;

import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_EXPERIMENT_POSITIONAL_READ_DEFAULT;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .ADL_EXPERIMENT_POSITIONAL_READ_KEY;
import static org.apache.hadoop.fs.adl.AdlConfKeys
    .DEFAULT_READ_AHEAD_BUFFER_SIZE;
import static org.apache.hadoop.fs.adl.AdlConfKeys.READ_AHEAD_BUFFER_SIZE_KEY;

/**
 * Wraps {@link ADLFileInputStream} implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class AdlFsInputStream extends FSInputStream {

  private final ADLFileInputStream in;
  private final Statistics stat;
  private final boolean enablePositionalReadExperiment;

  public AdlFsInputStream(ADLFileInputStream inputStream, Statistics statistics,
      Configuration conf) throws IOException {
    this.in = inputStream;
    this.in.setBufferSize(conf.getInt(READ_AHEAD_BUFFER_SIZE_KEY,
        DEFAULT_READ_AHEAD_BUFFER_SIZE));
    enablePositionalReadExperiment = conf
        .getBoolean(ADL_EXPERIMENT_POSITIONAL_READ_KEY,
            ADL_EXPERIMENT_POSITIONAL_READ_DEFAULT);
    stat = statistics;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    in.seek(pos);
  }

  /**
   * Return the current offset from the start of the file.
   */
  @Override
  public synchronized long getPos() throws IOException {
    return in.getPos();
  }

  @Override
  public boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    int ch = in.read();
    if (stat != null && ch != -1) {
      stat.incrementBytesRead(1);
    }
    return ch;
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    int numberOfByteRead = 0;
    if (enablePositionalReadExperiment) {
      numberOfByteRead = in.read(position, buffer, offset, length);
    } else {
      numberOfByteRead = super.read(position, buffer, offset, length);
    }

    if (stat != null && numberOfByteRead > 0) {
      stat.incrementBytesRead(numberOfByteRead);
    }
    return numberOfByteRead;
  }

  @Override
  public synchronized int read(byte[] buffer, int offset, int length)
      throws IOException {
    int numberOfByteRead = in.read(buffer, offset, length);
    if (stat != null && numberOfByteRead > 0) {
      stat.incrementBytesRead(numberOfByteRead);
    }
    return numberOfByteRead;
  }

  /**
   * This method returns the remaining bytes in the stream, rather than the
   * expected Java
   * interpretation of {@link java.io.InputStream#available()}, which expects
   * the
   * number of remaining
   * bytes in the local buffer. Moreover, it caps the value returned to a
   * maximum of Integer.MAX_VALUE.
   * These changed behaviors are to ensure compatibility with the
   * expectations of HBase WAL reader,
   * which depends on available() returning the number of bytes in stream.
   *
   * Given all other FileSystems in the hadoop ecosystem (especially HDFS) do
   * this, it is possible other
   * apps other than HBase would also pick up expectation of this behavior
   * based on HDFS implementation.
   * Therefore keeping this quirky behavior here, to ensure compatibility.
   *
   * @return remaining bytes in the stream, with maximum of Integer.MAX_VALUE.
   * @throws IOException If fails to get the position or file length from SDK.
   */
  @Override
  public synchronized int available() throws IOException {
    return (int) Math.min(in.length() - in.getPos(), Integer.MAX_VALUE);
  }

  @Override
  public synchronized void close() throws IOException {
    in.close();
  }

  @Override
  public synchronized long skip(long pos) throws IOException {
    return in.skip(pos);
  }

}
