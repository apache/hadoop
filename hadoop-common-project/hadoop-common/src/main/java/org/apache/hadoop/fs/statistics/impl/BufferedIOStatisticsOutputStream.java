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

package org.apache.hadoop.fs.statistics.impl;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * An extension of {@code BufferedOutputStream} which implements
 * {@link IOStatisticsSource} and forwards requests for the
 * {@link IOStatistics} to the wrapped stream.
 * This should be used when any output stream needs buffering while
 * allowing the inner stream to be a source of statistics.
 * It also implements {@link StreamCapabilities} and forwards the probe
 * to the inner stream, if possible.
 */
public class BufferedIOStatisticsOutputStream
    extends BufferedOutputStream
    implements IOStatisticsSource, Syncable, StreamCapabilities {


  public BufferedIOStatisticsOutputStream(final OutputStream out) {
    super(out);
  }

  public BufferedIOStatisticsOutputStream(final OutputStream out,
      final int size) {
    super(out, size);
  }

  @Override
  public IOStatistics getIOStatistics() {
    return retrieveIOStatistics(out);
  }

  /**
   * If the inner stream supports {@link StreamCapabilities},
   * forward the probe to it.
   * Otherwise: return false.
   *
   * @param capability string to query the stream support for.
   * @return true if a capability is known to be supported.
   */
  @Override
  public boolean hasCapability(final String capability) {
    if (out instanceof StreamCapabilities) {
      return ((StreamCapabilities) out).hasCapability(capability);
    } else {
      return false;
    }
  }

  /**
   * If the inner stream is Syncable, flush the buffer and then
   * invoke the inner stream's hflush() operation.
   * @throws IOException IO Problem
   * @throws UnsupportedOperationException if the inner class is not syncable
   */
  @Override
  public void hflush() throws IOException {
    if (out instanceof Syncable) {
      flush();
      ((Syncable) out).hflush();
    } else {
      throw new UnsupportedOperationException("hflush not supported by "
          + out);
    }
  }

  /**
   * If the inner stream is Syncable, flush the buffer and then
   * invoke the inner stream's hsync() operation.
   * @throws IOException IO Problem
   * @throws UnsupportedOperationException if the inner class is not syncable
   */
  @Override
  public void hsync() throws IOException {
    if (out instanceof Syncable) {
      flush();
      ((Syncable) out).hsync();
    } else {
      throw new UnsupportedOperationException("hsync not supported by "
          + out);
    }
  }
}
